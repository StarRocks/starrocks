---
displayed_sidebar: docs
description: "日付と時刻の型で、ミリ秒とマイクロ秒の精度をサポート。"
---

import DateTip from '../../../_assets/commonMarkdown/dateTimeTip.mdx'

# DATETIME

日付と時刻の型です。値の範囲は ['0000-01-01 00:00:00', '9999-12-31 23:59:59'] です。

<DateTip />

出力形式は `YYYY-MM-DD HH:MM:SS` です。

バージョン v3.3.5 から、DATETIME はミリ秒およびマイクロ秒の精度をサポートしています。出力形式は `YYYY-MM-DD HH:MM:SS.ffffff` です。

MySQL との互換性のため、StarRocks はオプションの小数秒精度引数 `DATETIME(p)`（`p` は 0 から 6）も受け付けます。この構文は列定義や `CAST` 式（例: `CAST(now() AS DATETIME(3))`）で使用でき、MySQL エコシステムのツールが生成した SQL をそのまま実行できます。精度引数は無視され、値は常にマイクロ秒精度で格納されます。詳細については、[cast](../../sql-functions/cast.md) 関数を参照してください。