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

// 并发数
const CONCURRENCY = 20;
const BATCH_DELAY = 300;

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
  date: string;            // 当天日期，日期变化时重置游标
}

async function getCursor(kv: KVNamespace): Promise<TaskCursor> {
  const cursor = await kv.get('task_cursor', 'json') as TaskCursor | null;
  const today = new Date().toISOString().slice(0, 10);
  // 日期变了或没有游标，从头开始
  if (!cursor || cursor.date !== today) {
    return { startIndex: 0, date: today };
  }
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
export async function executeTask(config: TaskConfig, kv: KVNamespace): Promise<void> {
  // 防重入
  const locked = await acquireLock(kv);
  if (!locked) {
    await updateStatus(kv, {
      state: 'error',
      message: '⚠️ 已有任务正在执行，请等待完成后再试',
      lastRunAt: Date.now(),
    });
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
      return;
    }

    // ── 读取游标，决定从哪里开始 ──────────────────────────────────────
    const cursor = await getCursor(kv);
    const startIndex = cursor.startIndex;

    // 如果游标已超过博主数量，说明今天已经全部处理完
    if (startIndex >= bloggerEntries.length) {
      await updateStatus(kv, {
        state: 'done',
        message: `✅ 今日所有 ${bloggerEntries.length} 个博主已处理完毕（无需重复执行）`,
        totalCount: bloggerEntries.length,
        processedCount: bloggerEntries.length,
        lastRunAt: startTime,
      });
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

    // ── 第二步：并发处理博主 ────────────────────────────────────────────
    const allToCreate: Record<string, any>[] = [];
    const allToUpdate: { record_id: string; fields: Record<string, any> }[] = [];
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
        batch.map(entry =>
          processSingleBlogger(tikhub, entry.profileUrl, entry.existingKolId),
        ),
      );

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
              allToUpdate.push({ record_id: existingRecordId, fields });
            } else {
              allToCreate.push(fields);
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

      processed += batch.length;

      const errorSuffix = errorDetails.length > 0 ? `\n最近错误: ${errorDetails.join(' | ')}` : '';
      await updateStatus(kv, {
        state: 'running',
        message: `进度 ${processed}/${bloggerEntries.length}（${errorCount} 个失败），新增 ${allToCreate.length} / 更新 ${allToUpdate.length} 条视频${errorSuffix}`,
        totalCount: bloggerEntries.length,
        processedCount: processed,
        videoCount: allToCreate.length + allToUpdate.length,
        lastRunAt: startTime,
      });

      if (i + CONCURRENCY < pendingEntries.length) {
        await new Promise(r => setTimeout(r, BATCH_DELAY));
      }
    }

    // ── 保存游标（支持下次续跑）──────────────────────────────────────────
    await saveCursor(kv, { startIndex: processed, date: cursor.date });

    // ── 第三步：写入目标表（upsert）──────────────────────────────────────
    if (allToCreate.length > 0 || allToUpdate.length > 0) {
      await updateStatus(kv, {
        state: 'running',
        message: `正在写入目标表：新增 ${allToCreate.length} 条，更新 ${allToUpdate.length} 条...`,
        lastRunAt: startTime,
      });

      if (allToCreate.length > 0) {
        await batchCreateRecords(
          config.targetAppToken,
          config.targetPersonalBaseToken,
          config.targetTableId,
          allToCreate,
        );
      }

      if (allToUpdate.length > 0) {
        await batchUpdateRecords(
          config.targetAppToken,
          config.targetPersonalBaseToken,
          config.targetTableId,
          allToUpdate,
        );
      }
    }

    const finalErrorInfo = allErrors.length > 0 ? `\n错误详情: ${allErrors.join('\n')}` : '';
    const timeoutInfo = timedOut
      ? `\n⏱️ 本次接近超时上限，已处理到第 ${processed} 个博主，下次执行将自动续跑`
      : '';
    const doneState = processed >= bloggerEntries.length ? 'done' : 'done';

    await updateStatus(kv, {
      state: doneState,
      message: `✅ 完成！处理博主 ${processed}/${bloggerEntries.length} 个（${errorCount} 个失败），新增 ${allToCreate.length} / 更新 ${allToUpdate.length} 条视频${timeoutInfo}${finalErrorInfo}`,
      totalCount: bloggerEntries.length,
      processedCount: processed,
      videoCount: allToCreate.length + allToUpdate.length,
      lastRunAt: startTime,
    });
  } catch (e: any) {
    await updateStatus(kv, {
      state: 'error',
      message: `❌ 执行失败: ${e?.message ?? String(e)}`,
      lastRunAt: startTime,
    });
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
