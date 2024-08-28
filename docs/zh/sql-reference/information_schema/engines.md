---
displayed_sidebar: docs
---

# engines

:::note

该视图不适用于 StarRocks 当前支持的功能。

:::

`engines` 提供有关存储引擎的信息。

`engines` 提供以下字段：

| 字段         | 描述                                                         |
| ------------ | ------------------------------------------------------------ |
| ENGINE       | 存储引擎的名称。                                             |
| SUPPORT      | 服务器对存储引擎的支持水平。有效值：<br />`YES`：引擎受支持且处于活动状态。<br />`DEFAULT`：类似于 YES，此外还是默认引擎。<br />`NO`：引擎不受支持。<br />`DISABLED`：引擎受支持但已被禁用。 |
| COMMENT      | 存储引擎的简要描述。                                         |
| TRANSACTIONS | 存储引擎是否支持事务。                                       |
| XA           | 存储引擎是否支持 XA 事务。                                   |
| SAVEPOINTS   | 存储引擎是否支持保存点。                                     |

