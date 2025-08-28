---
displayed_sidebar: docs
---

# to_datetime

Unix タイムスタンプを **現在のタイムゾーン設定に基づいて** DATETIME 型の値に変換します。

タイムゾーンの設定方法についての詳細は、 [Configure a time zone](../../../administration/management/timezone.md) を参照してください。

現在のセッションのタイムゾーン設定に依存せずに Unix タイムスタンプを DATETIME 型の値に変換したい場合は、 [to_datetime_ntz](./to_datetime_ntz.md) を使用できます。

## Syntax

```sql
DATETIME to_datetime(BIGINT unix_ts, INT scale)
```

## Parameters

| Name      | Type   | Required | Description                                             |
| --------- | ------ | -------- | ------------------------------------------------------- |
| `unix_ts` | BIGINT | Yes      | 変換する Unix タイムスタンプです。例: `1598306400` (秒) および `1598306400123` (ミリ秒)。 |
| `scale`   | INT    | No       | 時間の精度。 有効な値:<ul><li>`0` は秒を示します (デフォルト)。</li><li>`3` はミリ秒を示します。</li><li>`6` はマイクロ秒を示します。</li></ul> |

## Return Value

- 成功時: 現在のセッションのタイムゾーンに基づいた `DATETIME` 値を返します。
- 失敗時: `NULL` を返します。一般的な理由には以下が含まれます:
  - 無効な `scale` (0, 3, または 6 以外)
  - DATETIME 範囲外の値 (0001-01-01 から 9999-12-31)

## Example 

```sql
SET time_zone = 'Asia/Shanghai';

SELECT to_datetime(1598306400);
-- Returns: 2020-08-25 06:00:00

SELECT to_datetime(1598306400123, 3);
-- Returns: 2020-08-25 06:00:00.123000
```