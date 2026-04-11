export interface Env {
  TASK_STORE: KVNamespace;
  ASSETS: Fetcher;
}

export interface TaskConfig {
  // 博主表（数据源）
  sourceAppToken: string;
  sourcePersonalBaseToken: string;
  sourceTableId: string;
  profileUrlFieldName: string; // 主页链接的字段名，如 "抖音主页"

  // 视频数据表（写入目标）
  targetAppToken: string;
  targetPersonalBaseToken: string;
  targetTableId: string;

  // TikHub
  tikHubApiKey: string;

  // 接口版本：v2 便宜（默认），v1 贵约 20 倍（应急）
  apiVersion?: 'v1' | 'v2';

  // 名片/报价缓存天数：缓存未过期时不调接口（0=禁用缓存，每次都拉最新）
  cacheDays?: number;

  // 定时任务允许运行的星期（0=周日, 1=周一, ..., 6=周六），按新加坡时区(UTC+8)判断
  // 未设置或空数组 = 每天都跑（向后兼容）
  runDays?: number[];

  updatedAt?: number;
}

export interface BloggerInfo {
  nickName: string;
  avatarUri: string;
  wechat: string;
  mcnName: string;
  mcnLogo: string;
}

export interface VideoRecord {
  profileUrl: string;
  secUserId: string;
  kolId: string;
  bloggerInfo: BloggerInfo;
  videoId: string;
  title: string;
  videoUrl: string;
  publishTime: string;
  duration: number;       // 视频时长（秒）
  videoType: string;      // 星图视频 / 普通视频
  likeCount: number;
  playCount: number;
  commentCount: number;
  customPrice: number;    // 定制视频报价，单位：元
}

export interface TaskStatus {
  state: 'idle' | 'running' | 'done' | 'error';
  message: string;
  lastRunAt?: number;
  processedCount?: number;
  totalCount?: number;
  videoCount?: number;
}
