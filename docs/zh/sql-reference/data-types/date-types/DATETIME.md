---
displayed_sidebar: docs
---

import DateTip from '../../../_assets/commonMarkdown/dateTimeTip.mdx'

# DATETIME

日期时间类型，取值范围是 ['0000-01-01 00:00:00', '9999-12-31 23:59:59']。

<DateTip />

打印的形式是 `YYYY-MM-DD HH:MM:SS`。

从 v3.3.5 起，DATETIME 支持毫秒和微秒精度。打印形式为 `YYYY-MM-DD HH:MM:SS.fffffff`

为兼容 MySQL，StarRocks 还接受可选的小数秒精度参数 `DATETIME(p)`，其中 `p` 的取值范围为 0 到 6。该语法可用于列定义和 `CAST` 表达式（例如 `CAST(now() AS DATETIME(3))`），使 MySQL 生态工具生成的 SQL 无需修改即可运行。该精度参数会被忽略：数据始终以微秒精度存储。更多信息，参见 [cast](../../sql-functions/cast.md) 函数。

## 示例

创建表时指定字段类型为 DATETIME。

```sql
CREATE TABLE dateTimeDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    relTime DATETIME COMMENT "YYYY-MM-DD HH:MM:SS"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
