---
displayed_sidebar: docs
---

# to_datetime

将 Unix 时间戳转换为 **基于当前时区设置** 的 DATETIME 类型值。

有关设置时区的详细说明，请参阅 [配置时区](../../../administration/management/timezone.md)。

如果您想将 Unix 时间戳转换为与当前会话时区设置无关的 DATETIME 类型值，可以使用 [to_datetime_ntz](./to_datetime_ntz.md)。

## 语法

```sql
DATETIME to_datetime(BIGINT unix_ts, INT scale)
```

## 参数

| 名称      | 类型   | 是否必需 | 描述                                             |
| --------- | ------ | -------- | ------------------------------------------------ |
| `unix_ts` | BIGINT | 是       | 要转换的 Unix 时间戳。例如，`1598306400`（秒）和 `1598306400123`（毫秒）。 |
| `scale`   | INT    | 否       | 时间精度。有效值：<ul><li>`0` 表示秒（默认）。</li><li>`3` 表示毫秒。</li><li>`6` 表示微秒。</li></ul> |

## 返回值

- 成功时：返回基于当前会话时区的 `DATETIME` 值。
- 失败时：返回 `NULL`。常见原因包括：
  - 无效的 `scale`（不是 0、3 或 6）
  - 值超出 DATETIME 范围（0001-01-01 到 9999-12-31）

## 示例

```sql
SET time_zone = 'Asia/Shanghai';

SELECT to_datetime(1598306400);
-- 返回: 2020-08-25 06:00:00

SELECT to_datetime(1598306400123, 3);
-- 返回: 2020-08-25 06:00:00.123000
```