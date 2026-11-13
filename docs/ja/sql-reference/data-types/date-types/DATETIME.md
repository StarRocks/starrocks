---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "日付と時刻の型で、ミリ秒とマイクロ秒の精度をサポート。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

import DateTip from '../../../_assets/commonMarkdown/dateTimeTip.mdx'

# DATETIME

日付と時刻の型です。値の範囲は ['0000-01-01 00:00:00', '9999-12-31 23:59:59'] です。

<DateTip />

出力形式は `YYYY-MM-DD HH:MM:SS` です。

バージョン v3.3.5 から、DATETIME はミリ秒およびマイクロ秒の精度をサポートしています。出力形式は `YYYY-MM-DD HH:MM:SS.ffffff` です。