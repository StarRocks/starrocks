---
displayed_sidebar: docs
---

# ADMIN SKIP COMMITTED TRANSACTION

强制 abort 一个在存算分离（lake）表上卡在 `COMMITTED` 状态的事务：通过一次"no-op publish" 推进分区的可见版本——丢弃该事务的数据贡献，但仍写出一份不含该事务任何数据变更的新 tablet 元数据文件，让分区版本号正常前进。

这是面向运维的 **应急通道**，用于解除 publish 队列因 `txnlog` 丢失、segment / SST 数据文件丢失或对象存储故障而**永久卡死**的情况。它会**丢弃事务的数据**，仅在正常重试无法恢复时使用，并优先于更具破坏性的恢复路径（如 drop partition）。

:::warning

Abort 一个已经 `COMMITTED` 的事务是**不可逆**的，会导致该事务的数据丢失。事务记录仍会被标记为 `VISIBLE`（以保持分区版本序列完整），但其内部携带一个 no-op publish 的标记用于审计。

:::

:::tip

执行此操作需要 SYSTEM 级别的 OPERATE 权限。可参考 [GRANT](../../account-management/GRANT.md) 授权。

:::

## 语法

```sql
ADMIN SKIP COMMITTED TRANSACTION <txn_id> [REASON '<text>']
```

## 参数

| 参数 | 是否必填 | 说明 |
|---|---|---|
| `txn_id` | 是 | 事务的数字 ID。可通过 [SHOW TRANSACTION](../../loading_unloading/SHOW_TRANSACTION.md) 或 FE 日志获取。 |
| `REASON '<text>'` | 否 | abort 的原因，自由文本，写入 FE 审计日志。强烈建议填写，便于事后复盘。 |

## 限制（第一期）

1. **该功能需要 FE 配置打开**。FE config `enable_admin_skip_committed_txn` 为 `false` 时该语句会被拒绝并报错（默认 `false`，避免误操作）。打开方式：

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");
   ```

   恢复操作完成后请立即关闭。

2. **事务必须处于 `COMMITTED` 状态**。处于 `PREPARED` 的事务会自动按 `timeout` 超时，或者通过各导入类型自带的取消方式中止 —— 例如 broker / spark / routine load 用 `CANCEL LOAD`，stream load 用 `POST /api/transaction/rollback`。已经 `VISIBLE` 的事务无法撤销。

3. **仅支持开启了 `file_bundling=true` 的存算分离（cloud-native）表**。`file_bundling` 模式下分区元数据由 aggregator 一次原子写出，no-op publish 要么对整个分区生效要么完全不生效。未开启 `file_bundling` 的表存在 partial-publish 中间状态风险，会在入口处被拒绝。

4. **当前版本仅支持导入和 lake-compaction 类型的事务**。Alter / schema-change 类型的事务暂不支持，会报错。

## 行为

1. FE 校验事务状态、来源类型和涉及的表是否符合限制。
2. FE 将 no-op publish 标记写入事务并落盘到 edit log。
3. 下一轮 publish daemon tick 时，FE 把该标记通过 publish RPC 传给 BE。
4. BE 在事务目标版本写出一份 tablet 元数据文件，内容等于 base 版本（不含该事务的数据变更）。`file_bundling` 下由 aggregator 保证分区原子。
5. 分区可见版本推进到事务的目标版本，后续事务可以正常 publish。
6. 该事务最终状态记为 `VISIBLE`，但携带 no-op publish 标记便于审计。

竞态处理：如果原 publish RPC 恰好先成功，事务会按正常流程标 `VISIBLE`，标记变成 no-op（数据反而保住了）。FE 日志会记录这一结果。

## 审计

`SHOW PROC '/transactions/<db_name>/finished'` 在末尾追加了两列：

- `NoOpPublish`：`true`/`false`，标识该事务最终落盘时是否走了 no-op publish 路径
- `NoOpPublishReason`：`ADMIN SKIP COMMITTED TRANSACTION` 语句里 `REASON` 的内容

事后复盘时通过这两列就能确认 abort 是否真的生效，以及当时填写的原因。

## 示例

1. 找到卡死的事务：

   ```sql
   SHOW PROC '/transactions/<db_name>/running';
   ```

2. 打开开关：

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "true");
   ```

3. Abort 卡死的事务：

   ```sql
   ADMIN SKIP COMMITTED TRANSACTION 12345 REASON 'OSS 上 txnlog 文件丢失，手动恢复';
   ```

4. 再次关闭开关：

   ```sql
   ADMIN SET FRONTEND CONFIG ("enable_admin_skip_committed_txn" = "false");
   ```

## 相关命令

- [`SHOW TRANSACTION`](../../loading_unloading/SHOW_TRANSACTION.md) —— 查询事务状态。
- [`ADMIN REPAIR`](./ADMIN_REPAIR.md) —— 当元数据无法恢复时把 lake 分区回滚到一个历史可用版本；本命令不适用时的更激进备选。
