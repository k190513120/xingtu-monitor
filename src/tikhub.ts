import { VideoRecord } from './types';

const BASE_URL = 'https://api.tikhub.io';

export class TikHubClient {
  // forceV1: 强制使用 v1 接口（应急模式，v1 价格约为 v2 的 20 倍）
  constructor(private apiKey: string, private forceV1 = false) {}

  private async get(path: string, params: Record<string, string | number | boolean>, retries = 2): Promise<any> {
    const url = new URL(`${BASE_URL}${path}`);
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, String(v));
    }
    let lastError = '';
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) await new Promise(r => setTimeout(r, 1000 * attempt));
      // 单次请求 15 秒超时，防止连接挂起卡死整个任务
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 15_000);
      try {
        const res = await fetch(url.toString(), {
          headers: { Authorization: `Bearer ${this.apiKey}` },
          signal: controller.signal,
        });
        clearTimeout(timeout);
        const text = await res.text();
        if (res.ok) {
          // 用正则把超长数字转成字符串，避免 JS 精度丢失（如 kolId 19位）
          const safeJson = text.replace(/:\s*(\d{16,})/g, ': "$1"');
          return JSON.parse(safeJson);
        }
        lastError = `TikHub ${res.status} [${path}]: ${text.slice(0, 200)}`;
        // 400 可能是临时性的（TikHub 上游不稳定），重试
        if (res.status >= 500 || res.status === 400) continue;
        break; // 其他错误不重试
      } catch (e: any) {
        clearTimeout(timeout);
        if (e.name === 'AbortError') {
          lastError = `TikHub 请求超时(15s) [${path}]`;
          continue; // 超时后重试
        }
        lastError = `TikHub 网络错误 [${path}]: ${e?.message?.slice(0, 200)}`;
        continue; // 网络错误也重试
      }
    }
    throw new Error(lastError);
  }

  // 从抖音主页链接提取 sec_user_id
  // 链接格式：https://www.douyin.com/user/MS4wLjABAAAA...
  extractSecUserId(profileUrl: string): string {
    const match = profileUrl.trim().match(/\/user\/([^/?#\s]+)/);
    if (!match) throw new Error(`无法从主页链接提取 sec_user_id: ${profileUrl}`);
    return match[1];
  }

  // 步骤1：根据 sec_user_id 获取星图 KOL ID
  async getKolId(secUserId: string): Promise<string> {
    const res = await this.get('/api/v1/douyin/xingtu/get_xingtu_kolid_by_sec_user_id', {
      sec_user_id: secUserId,
    });
    // res.data 是一个对象，kol ID 在 data.id 字段
    const kolId = res.data?.id;
    if (!kolId) throw new Error(`未找到星图ID，sec_user_id: ${secUserId}`);
    return String(kolId);
  }

  // 步骤2：获取星图视频列表
  // ⚠️ v1/v2 都只取 latest_star_item_info（星图商单视频），
  // 不取 latest_item_info（个人/普通视频）——业务只关心星图数据
  // 默认用便宜的 v2 接口；forceV1=true 时走贵的 v1（应急）
  async getVideoList(kolId: string, limit = 20): Promise<{ video: any; videoType: string }[]> {
    if (this.forceV1) return this.getVideoListV1(kolId);
    return this.getVideoListV2(kolId, limit);
  }

  // v2 接口：get_author_show_items（便宜，偶尔不稳定）
  // 只返回星图视频
  private async getVideoListV2(kolId: string, limit = 20): Promise<{ video: any; videoType: string }[]> {
    const res = await this.get('/api/v1/douyin/xingtu_v2/get_author_show_items', {
      o_author_id: kolId,
      limit,
    });
    const data = res.data?.data ?? res.data ?? {};
    const result: { video: any; videoType: string }[] = [];
    const seen = new Set<string>();
    // 只读 latest_star_item_info，忽略 latest_item_info（个人视频）
    if (Array.isArray(data.latest_star_item_info)) {
      for (const v of data.latest_star_item_info) {
        const id = String(v.item_id ?? '');
        if (id && !seen.has(id)) { seen.add(id); result.push({ video: v, videoType: '星图视频' }); }
      }
    }
    return result;
  }

  // v1 接口：kol_video_performance_v1（稳定但贵，约 v2 的 20 倍价格）
  // 只返回星图视频（onlyAssign=true 让上游只返回商单）
  private async getVideoListV1(kolId: string): Promise<{ video: any; videoType: string }[]> {
    const res = await this.get('/api/v1/douyin/xingtu/kol_video_performance_v1', {
      kolId,
      onlyAssign: true,
    });
    const innerData = res.data?.data ?? res.data ?? {};
    // 只读 latest_star_item_info，忽略 latest_item_info（个人视频）
    const starItems = innerData.latest_star_item_info;
    const result: { video: any; videoType: string }[] = [];
    const seen = new Set<string>();
    if (Array.isArray(starItems)) {
      for (const v of starItems) {
        const id = String(v.item_id ?? '');
        if (id && !seen.has(id)) { seen.add(id); result.push({ video: v, videoType: '星图视频' }); }
      }
    }
    return result;
  }

  // 步骤3：获取报价列表（按视频时长分档）
  // video_type 1=1-20s, 2=21-60s, 71=60s以上
  async getPriceList(kolId: string): Promise<any[]> {
    const res = await this.get('/api/v1/douyin/xingtu/kol_service_price_v1', {
      kolId,
      platformChannel: '_1', // _1=抖音视频
    });
    // 兼容双层嵌套：res.data.data.price_info 或 res.data.price_info
    const innerData = res.data?.data ?? res.data ?? {};
    return innerData.price_info ?? res.data?.price_info ?? [];
  }

  // 步骤4：获取博主名片信息（名称、头像、微信、MCN等）
  // 默认用便宜的 v2 接口；forceV1=true 时走贵的 v1（应急）
  async getAuthorBusinessCard(kolId: string): Promise<{
    nickName: string;
    avatarUri: string;
    wechat: string;
    mcnName: string;
    mcnLogo: string;
  }> {
    if (this.forceV1) return this.getAuthorBaseInfoV1(kolId);
    return this.getAuthorBusinessCardV2(kolId);
  }

  // v2 名片接口（便宜）
  private async getAuthorBusinessCardV2(kolId: string): Promise<{
    nickName: string; avatarUri: string; wechat: string; mcnName: string; mcnLogo: string;
  }> {
    const res = await this.get('/api/v1/douyin/xingtu_v2/get_author_business_card_info', {
      o_author_id: kolId,
    });
    const innerData = res.data?.data ?? res.data ?? {};
    const card = innerData.card_info ?? innerData;
    return {
      nickName: String(card.nick_name ?? ''),
      avatarUri: String(card.avatar_uri ?? ''),
      wechat: String(card.wechat ?? ''),
      mcnName: String(card.mcn_info?.mcn_name ?? card.mcn_name ?? ''),
      mcnLogo: String(card.mcn_info?.mcn_logo ?? card.mcn_logo ?? ''),
    };
  }

  // v1 名片接口 kol_base_info_v1（贵，约 v2 的 20 倍）
  private async getAuthorBaseInfoV1(kolId: string): Promise<{
    nickName: string; avatarUri: string; wechat: string; mcnName: string; mcnLogo: string;
  }> {
    const res = await this.get('/api/v1/douyin/xingtu/kol_base_info_v1', {
      kolId,
      platformChannel: '_1',
    });
    const info = res.data?.data ?? res.data ?? {};
    // v1 没有独立 wechat 字段，尝试从 mcn_introduction 提取
    const mcnIntro = String(info.mcn_introduction ?? '');
    const wxMatch = mcnIntro.match(/(?:VX|vx|微信|wx|WeChat)\s*[:：]?\s*(\S+)/i);
    return {
      nickName: String(info.nick_name ?? ''),
      avatarUri: String(info.avatar_uri ?? ''),
      wechat: wxMatch ? wxMatch[1] : '',
      mcnName: String(info.mcn_name ?? ''),
      mcnLogo: String(info.mcn_logo ?? ''),
    };
  }

  // 根据视频时长（秒）匹配报价
  matchPrice(priceList: any[], durationSec: number): number {
    // video_type: 1=1-20s, 2=21-60s, 71=60s以上
    let targetType = 1; // 默认 1-20s
    if (durationSec > 60) targetType = 71;
    else if (durationSec > 20) targetType = 2;

    const matched = priceList.find((p: any) => p.video_type === targetType);
    if (matched) return Number(matched.price) || 0;

    // fallback: 取第一个固定价格的报价
    const fallback = priceList.find((p: any) => p.settlement_type === 2 && p.task_category === 1);
    return fallback ? Number(fallback.price) || 0 : 0;
  }
}
