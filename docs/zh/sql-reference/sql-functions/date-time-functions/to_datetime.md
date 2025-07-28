---
displayed_sidebar: docs
---

# to_datetime

> 将 Unix 时间戳转换为 `DATETIME` 类型（时区感知）

## 功能说明

`to_datetime` 用于将自 1970-01-01 00:00:00 UTC 起的 Unix 时间戳，**根据当前 session 的 time_zone 设置**转换为`DATETIME` 类型。

---

## 语法

```sql
DATETIME to_datetime(BIGINT unix_ts)
DATETIME to_datetime(BIGINT unix_ts, INT scale)
```

---

## 参数说明

| 参数        | 类型      | 是否必需 | 描述 |
|-------------|-----------|----------|------|
| `unix_ts`   | BIGINT    | 是       | Unix 时间戳，例如 `1598306400`（秒）、`1598306400123`（毫秒） |
| `scale`     | INT       | 否       | 时间粒度：<br/>• 0 = 秒（默认）<br/>• 3 = 毫秒<br/>• 6 = 微秒 |

---

## 返回值

- 成功：返回当前会话时区下的 `DATETIME` 值
- 失败：返回 `NULL`，常见原因包括：
  - 非法的 `scale` 值（非 0/3/6）
  - 时间超出 `DATETIME` 可表示范围（0001-01-01 ～ 9999-12-31）

---

## 示例（假设当前时区为 Asia/Shanghai）

```sql

SET time_zone = 'Asia/Shanghai';

SELECT to_datetime(1598306400);
-- 返回：2020-08-25 06:00:00

SELECT to_datetime(1598306400123, 3);
-- 返回：2020-08-25 06:00:00.123000
```

---
