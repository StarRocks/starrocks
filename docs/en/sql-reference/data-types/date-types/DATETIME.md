---
displayed_sidebar: docs
description: "Date and time type."
---

import DateTip from '../../../_assets/commonMarkdown/dateTimeTip.mdx'

# DATETIME

Date and time type. The value range is ['0000-01-01 00:00:00', '9999-12-31 23:59:59'].

<DateTip />

The form of printing is `YYYY-MM-DD HH:MM:SS`.

From v3.3.5, DATETIME supports millisecond and microsecond precision. The form of printing is `YYYY-MM-DD HH:MM:SS.ffffff`.

For MySQL compatibility, StarRocks also accepts an optional fractional-seconds precision argument, `DATETIME(p)`, where `p` ranges from 0 to 6. This syntax is accepted in column definitions and `CAST` expressions (for example, `CAST(now() AS DATETIME(3))`), which lets generated SQL from MySQL ecosystem tools run unchanged. The precision argument is ignored: values are always stored at microsecond precision.
