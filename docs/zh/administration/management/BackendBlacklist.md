---
displayed_sidebar: "Chinese"
---

# 管理BE黑名单

本文介绍如何管理机器黑名单 (Backend Blacklist)。

您可以在 StarRocks 中维护 BE 黑名单，以在某些场景下禁止查询使用指定的 BE，避免某个 BE 由于网络故障导致查询频繁失败或者其他预期之外的行为。

默认情况下，StarRocks会自动管理故障 BE 节点，将无法连通的 BE 节点自动加入/移出黑名单，但不会将手动添加的 BE 节点自动移出黑名单

> **注意**
>
> * 只有拥有 System 级 BLACKLIST 权限的用户才可以使用黑名单功能。
> * 每个FE上的黑名单由在各自FE配置，FE之间不会相互传递黑名单。

## 添加黑名单

通过以下命令添加 SQL 黑名单。

```sql
ADD BACKEND BLACKLIST BeID, BeID, ...;
```

**BeID**：可以通过`SHOW BACKENDS`获得。

示例:

* 禁止使用 BeID 为 3609624 的节点。

    ```sql
    ADD BACKEND BLACKLIST 3609624;
    ```


## 展示黑名单列表

```sql
SHOW BACKEND BLACKLIST;
```

示例：

```plain text
MySQL > show backend blacklist
+-----------+------------------+----------------------+------------------------------+--------------------+
| BackendId | AddBlackListType | LostConnectionTime   | LostConnectionNumberInPeriod | CheckTimePeriod(s) |
+-----------+------------------+----------------------+------------------------------+--------------------+
| 3609624   | MANUAL           | 2024-01-15 19:14:57  | 0                            | 5                  |
+-----------+------------------+----------------------+------------------------------+--------------------+
1 row in set
Time: 0.005s
```

返回结果包括 `Index` 和 `Forbidden SQL`。其中，`Index` 字段为被禁止的 SQL 黑名单序号，`Forbidden SQL` 字段展示了被禁止的 SQL，对于所有 SQL 语义的字符做了转义处理。

`AddBlackListType`：添加 BE 的方式，`MANUAL`为用户手动添加，`AUTO`为StarRocks自动添加
`LostConnectionTime`：添加黑名单时间，`AUTO`下为最后一次连接 BE 时间
`LostConnectionNumberInPeriod`：黑名单检测周期内，机器失联次数
`CheckTimePeriod(s)`：黑名单检测周期，单位为秒，来源于 FE 配置中的`black_host_history_sec`


## 删除黑名单

您可以通过以下命令删除 SQL 黑名单。

```sql
DELETE BACKEND BLACKLIST BeID, BeID, ...;
```


示例：

```sql
DELETE BACKEND BLACKLIST 3609624;
```

## FE配置项
`black_host_history_sec`：机器失联事件保留时间，默认为120s
`black_host_connect_failures_within_time`：机器失联次数限制，默认为5。在`black_host_history_sec`周期内失联次数过多，将认为 BE 节点不稳定，不会自动移出黑名单。
