---
displayed_sidebar: docs
---

import DateTip from '../../../_assets/commonMarkdown/dateTimeTip.mdx'

# DATE

DATE 型。値の範囲は ['0000-01-01', '9999-12-31'] で、デフォルトの形式は `YYYY-MM-DD` です。

<DateTip />

## 例

例 1: テーブルを作成する際に、列を DATE 型として指定します。

```SQL
CREATE TABLE dateDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    make_time DATE NOT NULL COMMENT "YYYY-MM-DD"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk)
```

例 2: DATETIME 値を DATE 値に変換します。

```sql
mysql> SELECT DATE('2003-12-31 01:02:03');
-> '2003-12-31'
```

詳細については、[date](../../sql-functions/date-time-functions/date.md) 関数を参照してください。