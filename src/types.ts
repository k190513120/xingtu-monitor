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
