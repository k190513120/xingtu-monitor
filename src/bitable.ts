// 飞书多维表格 OpenAPI，直接用 fetch 调用，无需 Node SDK
// 域名：飞书用 base-api.feishu.cn，Lark 用 base-api.larksuite.com
const BASE_API = 'https://base-api.feishu.cn';

async function request(
  method: string,
  path: string,
  token: string,
  body?: unknown,
): Promise<any> {
  const res = await fetch(`${BASE_API}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let json: any;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`飞书API返回非JSON ${res.status} [${method} ${path}]: ${text.slice(0, 300)}`);
  }

  if (!res.ok || json.code !== 0) {
    // 飞书常见错误码说明
    const hints: Record<number, string> = {
      1254043: '请检查 PersonalBaseToken 是否正确',
      1254045: '无权限，请确认 Token 对该多维表格有编辑权限',
      1254607: 'AppToken 无效，请检查是否正确',
    };
    const hint = hints[json.code] ? ` (${hints[json.code]})` : '';
    throw new Error(
      `飞书API错误 HTTP${res.status} code=${json.code} msg=${json.msg}${hint}`,
    );
  }
  return json;
}

// 读取一张表的所有记录（自动翻页）
export async function fetchAllRecords(
  appToken: string,
  token: string,
  tableId: string,
): Promise<any[]> {
  const all: any[] = [];
  let pageToken: string | undefined;

  do {
    const params = new URLSearchParams({ page_size: '100' });
    if (pageToken) params.set('page_token', pageToken);

    const res = await request(
      'GET',
      `/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/records?${params}`,
      token,
    );

    all.push(...(res.data?.items ?? []));
    pageToken = res.data?.has_more ? res.data.page_token : undefined;
  } while (pageToken);

  return all;
}

// 批量新建记录，每批最多 500 条（API 限制）
export async function batchCreateRecords(
  appToken: string,
  token: string,
  tableId: string,
  fieldsList: Record<string, any>[],
): Promise<void> {
  const BATCH_SIZE = 500;
  for (let i = 0; i < fieldsList.length; i += BATCH_SIZE) {
    const chunk = fieldsList.slice(i, i + BATCH_SIZE);
    await request(
      'POST',
      `/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_create`,
      token,
      { records: chunk.map(fields => ({ fields })) },
    );
  }
}

// 批量更新记录，每批最多 500 条
export async function batchUpdateRecords(
  appToken: string,
  token: string,
  tableId: string,
  records: { record_id: string; fields: Record<string, any> }[],
): Promise<void> {
  const BATCH_SIZE = 500;
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const chunk = records.slice(i, i + BATCH_SIZE);
    await request(
      'POST',
      `/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_update`,
      token,
      { records: chunk },
    );
  }
}

// 获取数据表字段列表（前端选择字段名时用）
export async function getTableFields(
  appToken: string,
  token: string,
  tableId: string,
): Promise<{ field_id: string; field_name: string; ui_type: string }[]> {
  const res = await request(
    'GET',
    `/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/fields?page_size=100`,
    token,
  );
  return res.data?.items ?? [];
}

// 创建目标表并初始化所有字段，返回新建的 table_id
export async function initTargetTable(
  appToken: string,
  token: string,
  tableName = '星图视频数据',
): Promise<string> {
  // 字段类型：1=多行文本, 2=数字, 15=超链接
  // 字段类型：1=多行文本, 2=数字, 15=超链接
  const fields = [
    { field_name: '博主主页',       type: 1 },
    { field_name: '星图ID',         type: 1 },
    { field_name: '博主名称',       type: 1 },
    { field_name: '视频ID',         type: 1 },
    { field_name: '视频标题',       type: 1 },
    { field_name: '视频链接',       type: 15 },
    { field_name: '发布时间',       type: 1 },
    { field_name: '视频时长（秒）', type: 2 },
    { field_name: '视频类型',       type: 1 },
    { field_name: '点赞数',         type: 2 },
    { field_name: '播放量',         type: 2 },
    { field_name: '评论数',         type: 2 },
    { field_name: '定制报价（元）', type: 2 },
    { field_name: '博主微信',       type: 1 },
    { field_name: 'MCN机构',        type: 1 },
    { field_name: '抓取时间',       type: 1 },
  ];

  const res = await request(
    'POST',
    `/open-apis/bitable/v1/apps/${appToken}/tables`,
    token,
    { table: { name: tableName, fields } },
  );

  return res.data?.table_id as string;
}

// 获取所有数据表列表
export async function getTableList(
  appToken: string,
  token: string,
): Promise<{ table_id: string; name: string }[]> {
  const res = await request(
    'GET',
    `/open-apis/bitable/v1/apps/${appToken}/tables?page_size=100`,
    token,
  );
  return res.data?.items ?? [];
}

// 确保表中存在指定字段，不存在则创建（跳过已存在的）
export async function ensureFields(
  appToken: string,
  token: string,
  tableId: string,
  requiredFields: { field_name: string; type: number }[],
): Promise<void> {
  const existing = await getTableFields(appToken, token, tableId);
  const existingNames = new Set(existing.map(f => f.field_name));

  for (const field of requiredFields) {
    if (existingNames.has(field.field_name)) continue;
    await request(
      'POST',
      `/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/fields`,
      token,
      field,
    );
  }
}

// 更新单条记录的字段
export async function updateRecord(
  appToken: string,
  token: string,
  tableId: string,
  recordId: string,
  fields: Record<string, any>,
): Promise<void> {
  await request(
    'PUT',
    `/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/records/${recordId}`,
    token,
    { fields },
  );
}

// 从飞书记录中提取字段值（兼容 URL 字段、文本字段、富文本数组）
export function extractFieldValue(fieldValue: unknown): string {
  if (!fieldValue) return '';
  if (typeof fieldValue === 'string') return fieldValue;
  if (Array.isArray(fieldValue)) {
    // 富文本数组：[{type:"text",text:"aaa"},{type:"url",link:"bbb"}]
    // 或 URL 字段：[{link:"...",text:"..."}]
    // 拼接所有文本片段，取出完整的 URL
    const parts: string[] = [];
    for (const item of fieldValue) {
      if (typeof item === 'string') { parts.push(item); continue; }
      if (item?.link) { parts.push(String(item.link)); continue; }
      if (item?.text) { parts.push(String(item.text)); continue; }
      if (item?.value) { parts.push(String(item.value)); continue; }
    }
    const joined = parts.join('');
    // 如果拼接结果包含 URL，直接返回
    if (joined) return joined;
    // fallback: 取第一个元素
    const first = fieldValue[0];
    if (!first) return '';
    return String(first.link ?? first.text ?? first.value ?? '');
  }
  if (typeof fieldValue === 'object') {
    const obj = fieldValue as any;
    return String(obj.link ?? obj.text ?? obj.value ?? '');
  }
  return String(fieldValue);
}
