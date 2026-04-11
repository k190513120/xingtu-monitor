import { TaskConfig, TaskStatus, VideoRecord, BloggerInfo } from './types';
import { TikHubClient } from './tikhub';
import {
  fetchAllRecords,
  batchCreateRecords,
  batchUpdateRecords,
  extractFieldValue,
  ensureFields,
  updateRecord,
} from './bitable';

// 每批处理博主数（3 并发，TikHub 峰值 ~9 qps，飞书表写入会串行化）
const CONCURRENCY = 3;
const BATCH_DELAY = 200;
// 单个博主处理超时：60 秒
const PER_BLOGGER_TIMEOUT_MS = 60_000;

// 安全执行时间：13 分钟（Workers 付费版 Cron 上限 15 分钟，留 2 分钟缓冲）
const MAX_EXECUTION_MS = 13 * 60 * 1000;

// 锁过期时间：15 分钟
const LOCK_TTL_SECONDS = 15 * 60;

// 需要在博主表中自动创建的字段
const SOURCE_TABLE_EXTRA_FIELDS = [
  { field_name: '星图ID',     type: 1 },
  { field_name: '博主名称',   type: 1 },
  { field_name: '博主头像',   type: 1 },
  { field_name: '博主微信',   type: 1 },
  { field_name: 'MCN机构',    type: 1 },
  { field_name: 'MCN机构Logo', type: 1 },
];

// 飞书机器人 webhook（用于推送执行结果通知）
const FEISHU_WEBHOOK = 'https://open.feishu.cn/open-apis/bot/v2/hook/5a155056-2f75-4bfe-8144-89360a1018d3';

// 发送飞书 webhook 通知（失败不影响主流程）
async function sendFeishuNotification(title: string, content: string): Promise<void> {
  try {
    await fetch(FEISHU_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        msg_type: 'post',
        content: {
          post: {
            zh_cn: {
              title,
              content: [[{ tag: 'text', text: content }]],
            },
          },
        },
      }),
    });
  } catch (e: any) {
    console.error('[executor] 飞书通知发送失败:', e?.message);
  }
}

// ── 防重入锁 ──────────────────────────────────────────────────────────────
async function acquireLock(kv: KVNamespace): Promise<boolean> {
  const existing = await kv.get('task_lock');
  if (existing) {
    console.log(`[executor] 任务锁已存在(${existing})，跳过本次执行`);
    return false;
  }
  await kv.put('task_lock', String(Date.now()), { expirationTtl: LOCK_TTL_SECONDS });
  return true;
}

async function releaseLock(kv: KVNamespace): Promise<void> {
  await kv.delete('task_lock');
}

// ── 游标管理（支持分批续跑）────────────────────────────────────────────────
interface TaskCursor {
  startIndex: number;      // 下次从第几个博主开始
  date: string;            // 本轮开始日期（仅用于展示）
  completedDate?: string;  // 最近一次完成全量的日期（用于判断是否需要开启新一轮）
}

async function getCursor(kv: KVNamespace): Promise<TaskCursor> {
  const cursor = await kv.get('task_cursor', 'json') as TaskCursor | null;
  const today = new Date().toISOString().slice(0, 10);
  // 没游标：第一次跑，从 0 开始
  if (!cursor) {
    return { startIndex: 0, date: today };
  }
  // 如果最近一次"完成全量"是今天，说明今日已处理完，后续 cron 无需再跑
  if (cursor.completedDate === today) {
    return cursor;
  }
  // 已经完成上一轮（completedDate 不是今天），开启今天的新一轮
  if (cursor.completedDate && cursor.completedDate !== today && cursor.startIndex === 0) {
    return { startIndex: 0, date: today };
  }
  // 没完成过 / 上一轮未跑完：继续从上次的断点续跑（即使跨天）
  return cursor;
}

async function saveCursor(kv: KVNamespace, cursor: TaskCursor): Promise<void> {
  await kv.put('task_cursor', JSON.stringify(cursor));
}

// ── 状态更新 ──────────────────────────────────────────────────────────────
async function updateStatus(kv: KVNamespace, status: TaskStatus): Promise<void> {
  await kv.put('task_status', JSON.stringify(status));
}

// ── 处理单个博主 ──────────────────────────────────────────────────────────
async function processSingleBlogger(
  tikhub: TikHubClient,
  profileUrl: string,
  existingKolId: string,
): Promise<{ kolId: string; bloggerInfo: BloggerInfo; videos: VideoRecord[] }> {
  const secUserId = tikhub.extractSecUserId(profileUrl);

  // 如果博主表已有星图ID，直接复用，省一次 API 调用
  const kolId = existingKolId || await tikhub.getKolId(secUserId);

  // 并发获取：名片 + 视频列表 + 报价（均容错，不影响其他步骤）
  const [bloggerInfo, videoItems, priceList] = await Promise.all([
    tikhub.getAuthorBusinessCard(kolId).catch(() => ({
      nickName: '', avatarUri: '', wechat: '', mcnName: '', mcnLogo: '',
    })),
    tikhub.getVideoList(kolId).catch(() => [] as { video: any; videoType: string }[]),
    tikhub.getPriceList(kolId).catch(() => [] as any[]),
  ]);

  const videos: VideoRecord[] = videoItems.map(({ video: v, videoType }) => {
    const videoId = String(v.item_id ?? v.aweme_id ?? v.video_id ?? '');
    const durationSec = Number(v.duration ?? v.duration_min ?? 0);
    return {
      profileUrl,
      secUserId,
      kolId,
      bloggerInfo,
      videoId,
      title: String(v.item_title ?? v.desc ?? v.title ?? v.video_title ?? ''),
      videoUrl: videoId ? `https://www.douyin.com/video/${videoId}` : '',
      publishTime: v.create_time
        ? new Date(Number(v.create_time) * 1000).toISOString().replace('T', ' ').slice(0, 19)
        : '',
      duration: durationSec,
      videoType,
      likeCount: Number(v.like ?? v.statistics?.digg_count ?? v.like_count ?? 0),
      playCount: Number(v.play ?? v.statistics?.play_count ?? v.play_count ?? 0),
      commentCount: Number(v.comment ?? v.statistics?.comment_count ?? v.comment_count ?? 0),
      customPrice: tikhub.matchPrice(priceList, durationSec),
    };
  });

  return { kolId, bloggerInfo, videos };
}

// ── 主执行逻辑 ────────────────────────────────────────────────────────────
export async function executeTask(
  config: TaskConfig,
  kv: KVNamespace,
  source: 'cron' | 'manual' = 'manual',
): Promise<void> {
  // 防重入
  const locked = await acquireLock(kv);
  if (!locked) {
    await updateStatus(kv, {
      state: 'error',
      message: '⚠️ 已有任务正在执行，请等待完成后再试',
      lastRunAt: Date.now(),
    });
    const triggerLabel = source === 'cron' ? '定时任务' : '手动执行';
    await sendFeishuNotification(
      `⏭️ 星图监控 - 跳过执行`,
      [
        `触发方式：${triggerLabel}`,
        `时间：${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}`,
        `原因：已有任务正在执行，跳过本次`,
      ].join('\n'),
    );
    return;
  }

  const startTime = Date.now();

  try {
    // ── 第一步：读取博主表 ──────────────────────────────────────────────
    await updateStatus(kv, {
      state: 'running',
      message: '正在读取博主列表...',
      lastRunAt: startTime,
    });

    const records = await fetchAllRecords(
      config.sourceAppToken,
      config.sourcePersonalBaseToken,
      config.sourceTableId,
    );

    // 提取每条记录的 { recordId, profileUrl, existingKolId }
    const bloggerEntries = records
      .map(r => ({
        recordId: r.record_id as string,
        profileUrl: extractFieldValue(r.fields?.[config.profileUrlFieldName]),
        existingKolId: extractFieldValue(r.fields?.['星图ID']),
      }))
      .filter(e => e.profileUrl.includes('douyin.com/user/'));

    if (bloggerEntries.length === 0) {
      await updateStatus(kv, {
        state: 'error',
        message: `博主表中未找到有效的抖音主页链接，请检查字段名 "${config.profileUrlFieldName}" 是否正确`,
        lastRunAt: startTime,
      });
      const triggerLabel = source === 'cron' ? '定时任务' : '手动执行';
      await sendFeishuNotification(
        `⚠️ 星图监控 - 无有效博主`,
        [
          `触发方式：${triggerLabel}`,
          `时间：${new Date(startTime).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}`,
          `原因：博主表中未找到有效的抖音主页链接`,
          `字段名：${config.profileUrlFieldName}`,
          `总记录数：${records.length}`,
          `博主表：${config.sourceTableId}`,
        ].join('\n'),
      );
      return;
    }

    // ── 读取游标，决定从哪里开始 ──────────────────────────────────────
    // 手动执行时始终从头开始，不受游标限制
    const cursor = await getCursor(kv);
    const today = new Date().toISOString().slice(0, 10);
    const startIndex = source === 'manual' ? 0 : cursor.startIndex;

    // 仅 cron 模式下：如果今日已完成全量，静默跳过（不发 webhook 避免刷屏）
    if (source !== 'manual' && cursor.completedDate === today) {
      await updateStatus(kv, {
        state: 'done',
        message: `✅ 今日所有 ${bloggerEntries.length} 个博主已处理完毕（无需重复执行）`,
        totalCount: bloggerEntries.length,
        processedCount: bloggerEntries.length,
        lastRunAt: startTime,
      });
      console.log(`[executor] 今日已完成，跳过本次 cron`);
      return;
    }

    const pendingEntries = bloggerEntries.slice(startIndex);

    // ── 确保博主表中有星图相关字段 ──────────────────────────────────────
    await updateStatus(kv, {
      state: 'running',
      message: `共 ${bloggerEntries.length} 个博主，本次从第 ${startIndex + 1} 个开始，检查字段中...`,
      totalCount: bloggerEntries.length,
      processedCount: startIndex,
      lastRunAt: startTime,
    });

    await ensureFields(
      config.sourceAppToken,
      config.sourcePersonalBaseToken,
      config.sourceTableId,
      SOURCE_TABLE_EXTRA_FIELDS,
    );

    // ── 读取目标表已有数据，用于 upsert 去重 ──────────────────────────
    await updateStatus(kv, {
      state: 'running',
      message: '正在读取目标表已有数据（用于去重）...',
      totalCount: bloggerEntries.length,
      processedCount: startIndex,
      lastRunAt: startTime,
    });

    const existingTargetRecords = await fetchAllRecords(
      config.targetAppToken,
      config.targetPersonalBaseToken,
      config.targetTableId,
      async (loaded: number) => {
        await updateStatus(kv, {
          state: 'running',
          message: `正在读取目标表已有数据（已加载 ${loaded} 条）...`,
          totalCount: bloggerEntries.length,
          processedCount: startIndex,
          lastRunAt: startTime,
        });
      },
    );

    // 构建去重索引：「星图ID_视频ID」→ record_id
    const existingVideoMap = new Map<string, string>();
    for (const r of existingTargetRecords) {
      const kolId = extractFieldValue(r.fields?.['星图ID']);
      const videoId = extractFieldValue(r.fields?.['视频ID']);
      if (kolId && videoId) {
        existingVideoMap.set(`${kolId}_${videoId}`, r.record_id);
      }
    }
    console.log(`[executor] 目标表已有 ${existingVideoMap.size} 条视频记录`);

    // ── 第二步：并发处理博主（每批处理完立即写入目标表）───────────────────
    let totalCreated = 0;
    let totalUpdated = 0;
    const allErrors: string[] = [];
    let processed = startIndex;
    let errorCount = 0;
    let timedOut = false;
    const tikhub = new TikHubClient(config.tikHubApiKey);

    for (let i = 0; i < pendingEntries.length; i += CONCURRENCY) {
      // 超时检查：接近 13 分钟时停止
      if (Date.now() - startTime > MAX_EXECUTION_MS) {
        timedOut = true;
        console.log(`[executor] 接近超时上限，在第 ${processed} 个博主处暂停`);
        break;
      }

      const batch = pendingEntries.slice(i, i + CONCURRENCY);

      const results = await Promise.allSettled(
        batch.map(entry => {
          // 单博主超时保护：超过 60 秒自动跳过，不阻塞其他博主
          const bloggerPromise = processSingleBlogger(tikhub, entry.profileUrl, entry.existingKolId);
          const timeoutPromise = new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`处理超时(60s)，已跳过: ${entry.profileUrl.slice(-30)}`)), PER_BLOGGER_TIMEOUT_MS),
          );
          return Promise.race([bloggerPromise, timeoutPromise]);
        }),
      );

      // 本批次需要写入的数据
      const batchToCreate: Record<string, any>[] = [];
      const batchToUpdate: { record_id: string; fields: Record<string, any> }[] = [];
      const errorDetails: string[] = [];

      for (let j = 0; j < results.length; j++) {
        const result = results[j];
        const entry = batch[j];

        if (result.status === 'fulfilled') {
          const { kolId, bloggerInfo, videos } = result.value;

          // 写回博主表：星图ID + 名片信息
          const fieldsToUpdate: Record<string, any> = {};
          if (kolId && kolId !== entry.existingKolId) fieldsToUpdate['星图ID'] = kolId;
          if (bloggerInfo.nickName) fieldsToUpdate['博主名称'] = bloggerInfo.nickName;
          if (bloggerInfo.avatarUri) fieldsToUpdate['博主头像'] = bloggerInfo.avatarUri;
          if (bloggerInfo.wechat) fieldsToUpdate['博主微信'] = bloggerInfo.wechat;
          if (bloggerInfo.mcnName) fieldsToUpdate['MCN机构'] = bloggerInfo.mcnName;
          if (bloggerInfo.mcnLogo) fieldsToUpdate['MCN机构Logo'] = bloggerInfo.mcnLogo;

          if (Object.keys(fieldsToUpdate).length > 0) {
            try {
              await updateRecord(
                config.sourceAppToken,
                config.sourcePersonalBaseToken,
                config.sourceTableId,
                entry.recordId,
                fieldsToUpdate,
              );
            } catch (e: any) {
              console.error(`[executor] 写回博主表失败 [${entry.profileUrl}]:`, e?.message);
            }
          }

          // 视频 upsert 分拣：新视频 → create，已存在 → update
          if (videos.length === 0) {
            if (allErrors.length < 5) allErrors.push(`[${entry.profileUrl.slice(-20)}] 视频列表为空`);
          }
          for (const video of videos) {
            const fields = videoRecordToFields(video);
            const dedupKey = `${video.kolId}_${video.videoId}`;
            const existingRecordId = existingVideoMap.get(dedupKey);
            if (existingRecordId) {
              batchToUpdate.push({ record_id: existingRecordId, fields });
            } else {
              batchToCreate.push(fields);
              existingVideoMap.set(dedupKey, '__pending__'); // 防止同批次重复
            }
          }
        } else {
          errorCount++;
          const errMsg = result.reason?.message || String(result.reason);
          console.error(`[executor] 博主处理失败 [${entry.profileUrl}]:`, errMsg);
          if (errorDetails.length < 3) errorDetails.push(errMsg.slice(0, 150));
          if (allErrors.length < 5) allErrors.push(`[${entry.profileUrl.slice(-20)}] ${errMsg.slice(0, 100)}`);
        }
      }

      // ── 每批处理完立即写入目标表 ──────────────────────────────────────
      try {
        if (batchToCreate.length > 0) {
          await batchCreateRecords(
            config.targetAppToken,
            config.targetPersonalBaseToken,
            config.targetTableId,
            batchToCreate,
          );
        }
        if (batchToUpdate.length > 0) {
          await batchUpdateRecords(
            config.targetAppToken,
            config.targetPersonalBaseToken,
            config.targetTableId,
            batchToUpdate,
          );
        }
        totalCreated += batchToCreate.length;
        totalUpdated += batchToUpdate.length;
      } catch (e: any) {
        console.error(`[executor] 批量写入目标表失败:`, e?.message);
        if (allErrors.length < 5) allErrors.push(`写入失败: ${e?.message?.slice(0, 100)}`);
      }

      processed += batch.length;

      // ── 每批保存游标，即使后续超时也不会丢失进度 ──────────────────────
      await saveCursor(kv, {
        startIndex: processed,
        date: cursor.date,
        completedDate: cursor.completedDate,
      });

      const errorSuffix = errorDetails.length > 0 ? `\n最近错误: ${errorDetails.join(' | ')}` : '';
      await updateStatus(kv, {
        state: 'running',
        message: `进度 ${processed}/${bloggerEntries.length}（${errorCount} 个失败），已写入：新增 ${totalCreated} / 更新 ${totalUpdated} 条视频${errorSuffix}`,
        totalCount: bloggerEntries.length,
        processedCount: processed,
        videoCount: totalCreated + totalUpdated,
        lastRunAt: startTime,
      });

      if (i + CONCURRENCY < pendingEntries.length) {
        await new Promise(r => setTimeout(r, BATCH_DELAY));
      }
    }

    // ── 本次是否完成全量？如果是，标记 completedDate 并重置游标为 0 ────────
    const isFullyCompleted = processed >= bloggerEntries.length;
    if (isFullyCompleted) {
      await saveCursor(kv, {
        startIndex: 0,
        date: today,
        completedDate: today,
      });
    }

    const finalErrorInfo = allErrors.length > 0 ? `\n错误详情: ${allErrors.join('\n')}` : '';
    const timeoutInfo = timedOut
      ? `\n⏱️ 本次接近超时上限，已处理到第 ${processed} 个博主，下次 cron 将自动续跑`
      : '';
    const doneState = 'done';

    await updateStatus(kv, {
      state: doneState,
      message: `✅ 完成！处理博主 ${processed}/${bloggerEntries.length} 个（${errorCount} 个失败），新增 ${totalCreated} / 更新 ${totalUpdated} 条视频${timeoutInfo}${finalErrorInfo}`,
      totalCount: bloggerEntries.length,
      processedCount: processed,
      videoCount: totalCreated + totalUpdated,
      lastRunAt: startTime,
    });

    // 发送飞书通知（成功）
    const durationSec = Math.round((Date.now() - startTime) / 1000);
    const triggerLabel = source === 'cron' ? '定时任务' : '手动执行';
    const statusEmoji = errorCount > 0 ? '⚠️' : '✅';
    const sourceTableUrl = `https://bytedance.larkoffice.com/base/${config.sourceAppToken}?table=${config.sourceTableId}`;
    const targetTableUrl = `https://bytedance.larkoffice.com/base/${config.targetAppToken}?table=${config.targetTableId}`;
    const summaryLines = [
      `触发方式：${triggerLabel}`,
      `开始时间：${new Date(startTime).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}`,
      `执行耗时：${durationSec} 秒`,
      `处理博主：${processed}/${bloggerEntries.length} 个`,
      `失败博主：${errorCount} 个`,
      `视频新增：${totalCreated} 条`,
      `视频更新：${totalUpdated} 条`,
      `目标表已有视频：${existingVideoMap.size} 条`,
      ``,
      `📋 博主表：${sourceTableUrl}`,
      `📊 目标表：${targetTableUrl}`,
    ];
    if (timedOut) summaryLines.push(`⏱️ 接近超时上限，下次 cron 将自动续跑`);
    if (isFullyCompleted) summaryLines.push(`🎉 本轮全部 ${bloggerEntries.length} 个博主已完成`);
    if (allErrors.length > 0) summaryLines.push(`\n错误样例：\n${allErrors.join('\n')}`);
    await sendFeishuNotification(
      `${statusEmoji} 星图监控 - 执行完成`,
      summaryLines.join('\n'),
    );
  } catch (e: any) {
    await updateStatus(kv, {
      state: 'error',
      message: `❌ 执行失败: ${e?.message ?? String(e)}`,
      lastRunAt: startTime,
    });
    // 发送飞书通知（失败）
    const triggerLabel2 = source === 'cron' ? '定时任务' : '手动执行';
    const sourceTableUrl2 = `https://bytedance.larkoffice.com/base/${config.sourceAppToken}?table=${config.sourceTableId}`;
    const targetTableUrl2 = `https://bytedance.larkoffice.com/base/${config.targetAppToken}?table=${config.targetTableId}`;
    await sendFeishuNotification(
      `❌ 星图监控 - 执行失败`,
      [
        `触发方式：${triggerLabel2}`,
        `开始时间：${new Date(startTime).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}`,
        `错误信息：${e?.message ?? String(e)}`,
        ``,
        `📋 博主表：${sourceTableUrl2}`,
        `📊 目标表：${targetTableUrl2}`,
      ].join('\n'),
    );
    throw e;
  } finally {
    await releaseLock(kv);
  }
}

// 将 VideoRecord 转换为多维表格字段对象
function videoRecordToFields(v: VideoRecord): Record<string, any> {
  return {
    '博主主页': v.profileUrl,
    '星图ID': v.kolId,
    '博主名称': v.bloggerInfo.nickName,
    '视频ID': v.videoId,
    '视频标题': v.title,
    '视频链接': v.videoUrl ? { link: v.videoUrl, text: v.videoUrl } : '',
    '发布时间': v.publishTime,
    '视频时长（秒）': v.duration,
    '视频类型': v.videoType,
    '点赞数': v.likeCount,
    '播放量': v.playCount,
    '评论数': v.commentCount,
    '定制报价（元）': v.customPrice,
    '博主微信': v.bloggerInfo.wechat,
    'MCN机构': v.bloggerInfo.mcnName,
    '抓取时间': new Date().toISOString().replace('T', ' ').slice(0, 19),
  };
}
