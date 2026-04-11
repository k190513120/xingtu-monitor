import { Env, TaskConfig } from './types';
import { executeTask } from './executor';
import { getTableList, getTableFields, initTargetTable } from './bitable';

export default {
  // ── HTTP 请求处理（API + 静态资源由 ASSETS 处理）──────────────────────
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const method = request.method;

    // 跨域头（Feishu iframe 需要）
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,x-personal-token',
    };

    if (method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    function json(data: unknown, status = 200): Response {
      return new Response(JSON.stringify(data), {
        status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }

    // ── API 路由 ──────────────────────────────────────────────────────────

    // 保存配置 POST /api/config
    if (url.pathname === '/api/config' && method === 'POST') {
      try {
        const config = await request.json() as TaskConfig;
        await env.TASK_STORE.put('task_config', JSON.stringify({
          ...config,
          updatedAt: Date.now(),
        }));
        // 配置变更时重置游标，避免换表后被旧游标跳过
        await env.TASK_STORE.delete('task_cursor');
        return json({ success: true });
      } catch (e: any) {
        return json({ success: false, error: e?.message }, 400);
      }
    }

    // 读取配置 GET /api/config
    if (url.pathname === '/api/config' && method === 'GET') {
      const config = await env.TASK_STORE.get('task_config', 'json') as TaskConfig | null;
      if (!config) return json({});
      return json(config);
    }

    // 手动触发执行 POST /api/run?forceV1=true 可选
    if (url.pathname === '/api/run' && method === 'POST') {
      const config = await env.TASK_STORE.get('task_config', 'json') as TaskConfig | null;
      if (!config) {
        return json({ success: false, error: '请先保存配置' }, 400);
      }
      const forceV1 = url.searchParams.get('forceV1') === 'true';
      ctx.waitUntil(executeTask(config, env.TASK_STORE, 'manual', forceV1));
      return json({
        success: true,
        message: forceV1
          ? '任务已启动（v1 应急模式，成本较高），请在「执行」页查看进度'
          : '任务已启动（v2 模式），请在「执行」页查看进度',
      });
    }

    // 查询执行状态 GET /api/status
    if (url.pathname === '/api/status' && method === 'GET') {
      const status = await env.TASK_STORE.get('task_status', 'json');
      return json(status ?? { state: 'idle', message: '暂无执行记录' });
    }

    // 一键初始化目标表 POST /api/init
    if (url.pathname === '/api/init' && method === 'POST') {
      const { appToken, token, tableName } = await request.json() as any;
      if (!appToken || !token) return json({ error: '缺少 appToken 或 token' }, 400);
      try {
        const tableId = await initTargetTable(appToken, token, tableName);
        return json({ success: true, tableId });
      } catch (e: any) {
        return json({ success: false, error: e?.message }, 500);
      }
    }

    // 加载表列表 GET /api/tables?appToken=xxx
    if (url.pathname === '/api/tables' && method === 'GET') {
      const appToken = url.searchParams.get('appToken') ?? '';
      const token = request.headers.get('x-personal-token') ?? '';
      if (!appToken || !token) return json({ error: '缺少 appToken 或 token' }, 400);
      try {
        const tables = await getTableList(appToken, token);
        return json({ tables });
      } catch (e: any) {
        return json({ error: e?.message }, 500);
      }
    }

    // 加载字段列表 GET /api/fields?appToken=xxx&tableId=xxx
    if (url.pathname === '/api/fields' && method === 'GET') {
      const appToken = url.searchParams.get('appToken') ?? '';
      const tableId = url.searchParams.get('tableId') ?? '';
      const token = request.headers.get('x-personal-token') ?? '';
      if (!appToken || !tableId || !token) return json({ error: '缺少参数' }, 400);
      try {
        const fields = await getTableFields(appToken, token, tableId);
        return json({ fields });
      } catch (e: any) {
        return json({ error: e?.message }, 500);
      }
    }

    // ── 调试接口：测试 TikHub API 全流程 GET /api/debug?url=xxx ─────────
    if (url.pathname === '/api/debug' && method === 'GET') {
      const config = await env.TASK_STORE.get('task_config', 'json') as TaskConfig | null;
      if (!config) return json({ error: '请先保存配置' }, 400);
      const testUrl = url.searchParams.get('url') || '';
      if (!testUrl) return json({ error: '缺少 url 参数' }, 400);

      const { TikHubClient } = await import('./tikhub');
      const tikhub = new TikHubClient(config.tikHubApiKey);
      const steps: any = {};
      try {
        steps.secUserId = tikhub.extractSecUserId(testUrl);
        steps.kolId = await tikhub.getKolId(steps.secUserId);
        const [videoItems, priceList, businessCard] = await Promise.all([
          tikhub.getVideoList(steps.kolId),
          tikhub.getPriceList(steps.kolId).catch((e: any) => ({ error: e?.message })),
          tikhub.getAuthorBusinessCard(steps.kolId).catch((e: any) => ({ error: e?.message })),
        ]);
        steps.businessCard = businessCard;
        steps.videoCount = Array.isArray(videoItems) ? videoItems.length : 0;
        steps.starCount = Array.isArray(videoItems) ? videoItems.filter((i: any) => i.videoType === '星图视频').length : 0;
        steps.regularCount = Array.isArray(videoItems) ? videoItems.filter((i: any) => i.videoType === '普通视频').length : 0;
        steps.priceList = Array.isArray(priceList) ? priceList.map((p: any) => ({ desc: p.desc, price: p.price, video_type: p.video_type })) : priceList;
        if (Array.isArray(videoItems) && videoItems.length > 0) {
          const sample = videoItems[0];
          steps.sampleVideo = {
            videoType: sample.videoType,
            item_id: sample.video?.item_id,
            title: sample.video?.item_title,
            duration: sample.video?.duration,
            like: sample.video?.like,
            play: sample.video?.play,
          };
        }
        // 价格匹配测试（用第一条视频的时长）
        if (Array.isArray(videoItems) && videoItems.length > 0 && Array.isArray(priceList)) {
          const duration = Number(videoItems[0].video?.duration ?? 0);
          steps.samplePriceMatch = tikhub.matchPrice(priceList, duration);
        }
      } catch (e: any) {
        steps.error = e?.message;
      }
      return json(steps);
    }

    // ── 其他请求交给 Workers Assets（Vite 构建的静态文件）────────────────
    return env.ASSETS.fetch(request);
  },

  // ── Cron 定时任务触发 ─────────────────────────────────────────────
  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const config = await env.TASK_STORE.get('task_config', 'json') as TaskConfig | null;
    if (!config) {
      console.log('[cron] 未找到任务配置，跳过执行');
      return;
    }
    console.log('[cron] 定时任务开始执行');
    ctx.waitUntil(executeTask(config, env.TASK_STORE, 'cron'));
  },
};
