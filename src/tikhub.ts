import { VideoRecord } from './types';

const BASE_URL = 'https://api.tikhub.io';

export class TikHubClient {
  constructor(private apiKey: string) {}

  private async get(path: string, params: Record<string, string | number | boolean>, retries = 2): Promise<any> {
    const url = new URL(`${BASE_URL}${path}`);
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, String(v));
    }
    let lastError = '';
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) await new Promise(r => setTimeout(r, 1000 * attempt));
      const res = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${this.apiKey}` },
      });
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

  // 步骤2：获取星图视频列表（直接用稳定的 v1 接口）
  async getVideoList(kolId: string): Promise<{ video: any; videoType: string }[]> {
    const res = await this.get('/api/v1/douyin/xingtu/kol_video_performance_v1', {
      kolId,
      onlyAssign: true,
    });
    // v1 接口嵌套结构：TikHub 外层 data -> 星图原始 data
    const innerData = res.data?.data ?? res.data ?? {};
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

  // 步骤4：获取博主名片信息（直接用稳定的 v1 接口）
  async getAuthorBusinessCard(kolId: string): Promise<{
    nickName: string;
    avatarUri: string;
    wechat: string;
    mcnName: string;
    mcnLogo: string;
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
