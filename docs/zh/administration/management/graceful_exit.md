---
displayed_sidebar: docs
---

# 优雅退出

从 v3.3 开始，StarRocks 支持优雅退出。

## 概述

优雅退出是一种机制，旨在支持 StarRocks FE、BE 和 CN 节点的**无中断升级和重启**。其主要目标是在节点重启、滚动升级或集群扩展等维护操作期间，尽量减少对正在运行的查询和数据摄取任务的影响。

优雅退出确保：

- 节点在退出开始后**停止接受新任务**；
- 现有查询和导入作业在控制的时间窗口内**允许完成**；
- 系统组件（FE/BE/CN）**协调状态变化**，以便集群正确地重新路由流量。

FE 和 BE/CN 节点的优雅退出机制有所不同，具体如下所述。

### FE 优雅退出机制

#### 触发信号

FE 优雅退出通过以下方式启动：

```bash
stop_fe.sh -g
```

这会发送一个 `SIGUSR1` 信号，而默认退出（不带 `-g` 选项）会发送 `SIGTERM` 信号。

#### 负载均衡器感知

收到信号后：

- FE 在 `/api/health` 端点立即返回 **HTTP 500**。
- 负载均衡器在约 15 秒内检测到降级状态，并停止将新连接路由到此 FE。

#### 连接排空和关闭逻辑

**Follower FE**

- 处理只读查询。
- 如果 FE 节点没有活动会话，连接会立即关闭。
- 如果 SQL 正在运行，FE 节点会等待执行完成后再关闭会话。

**Leader FE**

- 读取请求处理与 Follower 相同。
- 写入请求处理需要：

  - 关闭 BDBJE。
  - 允许新的 Leader 选举完成。
  - 将后续写入重定向到新选出的 Leader。

#### 超时控制

如果查询运行时间过长，FE 在 **60 秒** 后强制退出（可通过 `--timeout` 选项配置）。

### BE/CN 优雅退出机制

#### 触发信号

BE 优雅退出通过以下方式启动：

```bash
stop_be.sh -g
```

CN 优雅退出通过以下方式启动：

```bash
stop_cn.sh -g
```

这会发送一个 `SIGTERM` 信号，而默认退出（不带 `-g` 选项）会发送 `SIGKILL` 信号。

#### 状态转换

收到信号后：

- BE/CN 节点将自身标记为**Exiting**。
- 它通过返回 `INTERNAL_ERROR` 拒绝**新的查询 Fragment**。
- 它继续处理现有Fragment。

#### 等待进行中的查询循环

BE/CN 等待现有 Fragment 完成的行为由 BE/CN 配置 `loop_count_wait_fragments_finish` 控制（默认值：2）。实际等待时间等于 `loop_count_wait_fragments_finish × 10 秒`（即默认 20 秒）。如果 Fragment 在超时后仍然存在，BE/CN 将继续正常关闭（关闭线程、网络和其他进程）。

#### 改进的 FE 感知

从 v3.4 开始，FE 不再根据心跳失败将 BE/CN 标记为 `DEAD`。它正确识别 BE/CN 的“Exiting”状态，允许显著更长的优雅退出窗口以完成 Fragment。

## 配置

### FE 配置

#### `stop_fe.sh -g --timeout`

- 描述：FE 被强制终止前的最大等待时间。
- 默认值：60（秒）
- 如何应用：在脚本命令中指定，例如 `--timeout 120`。

#### *最小 LB 检测时间*

- 描述：LB 至少需要 15 秒来检测变更的健康状态。
- 默认值：15（秒）
- 如何应用：固定值

### BE/CN 配置

#### `loop_count_wait_fragments_finish`

- 描述：BE/CN 等待现有 Fragment 的持续时间。将该值乘以 10 秒。
- 默认值：2
- 如何应用：在 BE/CN 配置文件中修改或动态更新。

#### `graceful_exit_wait_for_frontend_heartbeat`

- 描述：BE/CN 是否等待 FE 通过心跳确认 **SHUTDOWN**。从 v3.4.5 开始支持。
- 默认值：false
- 如何应用：在 BE/CN 配置文件中修改或动态更新。

#### `stop_be.sh -g --timeout`, `stop_cn.sh -g --timeout`

- 描述：BE/CN 被强制终止前的最大等待时间。将其设置为大于 `loop_count_wait_fragments_finish` * 10 的值，以防止在 BE/CN 等待时间到达之前终止。
- 默认值：false
- 如何应用：在脚本命令中指定，例如 `--timeout 30`。

### 全局开关

从 v3.4 开始，优雅退出**默认启用**。要暂时禁用它，请将 BE/CN 配置 `loop_count_wait_fragments_finish` 设置为 `0`。

## 优雅退出期间的预期行为

### 查询工作负载

| 查询类型                          | 预期行为                                                                   |
| ----------------------------------- | ----------------------------------------------------------------------------------- |
| **短（少于 20 秒）**    | BE/CN 等待足够长时间，因此查询正常完成。                              |
| **中（20 到 60 秒）**       | 在 BE/CN 等待窗口内完成的查询返回成功；否则查询被取消并需要手动重试。 |
| **长（超过 60 秒）**     | 查询可能由于超时而被 FE/BE/CN 终止，需要手动重试。 |

### 数据摄取任务

- **通过 Flink 或 Kafka Connector 的导入任务**会自动重试，没有用户可见的中断。
- **Stream Load（非框架）、Broker Load 和 Routine Load 任务**如果连接中断可能会失败。需要手动重试。
- **后台任务**会自动重新调度并由 FE 重试机制执行。

### 升级和重启操作

优雅退出确保：

- 没有集群范围的停机；
- 通过逐个排空节点实现安全的滚动升级。

## 限制和版本差异

### 版本行为差异

| 版本   | 行为                                                                                                                  |
| --------- | ------------------------------------------------------------------------------------------------------------------------- |
| **v3.3**  | BE 优雅退出存在缺陷：FE 可能会过早将 BE/CN 标记为 `DEAD`，导致查询被取消。有效等待时间有限（默认 15 秒）。 |
| **v3.4+** | 完全支持延长等待时间；FE 正确识别 BE/CN 的“Exiting”状态。推荐用于生产环境。         |

### 操作限制

- 在极端情况下（例如，BE/CN 挂起），优雅退出可能会失败。终止进程需要 `kill -9`，这可能导致部分数据未能持久化（可通过快照恢复）。

## 使用

### 前提条件

**StarRocks 版本**：

- **v3.3+**：基本的优雅退出支持。
- **v3.4+**：增强的状态管理，延长的等待窗口（长达几分钟）。

**配置**：

- 确保 `loop_count_wait_fragments_finish` 设置为正整数。
- 将 `graceful_exit_wait_for_frontend_heartbeat` 设置为 `true` 以允许 FE 检测 BE 的“Exiting”状态。

### 执行 FE 优雅退出

```bash
./bin/stop_fe.sh -g --timeout 60
```

参数：

- `--timeout`：在 FE 节点被强制终止前的最大等待时间。

行为：

- 系统首先发送 `SIGUSR1` 信号。
- 超时后，回退到 `SIGKILL`。

#### 验证 FE 状态

您可以通过以下 API 检查 FE 的健康状态：

```
http://<fe_host>:8030/api/health
```

LB 在收到连续的非 200 响应后移除节点。

### 执行 BE/CN 优雅退出

- **对于 v3.3：**

  - BE：

  ```bash
  ./be/bin/stop_be.sh -g
  ```

  - CN：

  ```bash
  ./be/bin/stop_cn.sh -g
  ```

- **对于 v3.4+：**

  - BE：

  ```bash
  ./bin/stop_be.sh -g --timeout 600
  ```

  - CN：

  ```bash
  ./bin/stop_cn.sh -g --timeout 600
  ```

如果没有 Fragment 剩余，BE/CN 会立即退出。

#### 验证 BE/CN 状态

在 FE 上运行：

```sql
SHOW BACKENDS;
```

`StatusCode`：

- `SHUTDOWN`：BE/CN 优雅退出进行中。
- `DISCONNECTED`：BE/CN 节点已完全退出。

## 滚动升级工作流程

### 步骤

1. 在节点 `A` 上执行优雅退出。
2. 确认节点 `A` 在 FE 端显示为 `DISCONNECTED`。
3. 升级并重启节点 `A`。
4. 对剩余节点重复上述步骤。

### 监控优雅退出

检查 FE 日志 `fe.log`、BE 日志 `be.log` 或 CN 日志 `cn.log` 以确保在退出期间是否有任务。

## 故障排除

### BE/CN 因超时退出

如果任务未能在优雅退出期间完成，BE/CN 将触发强制终止（`SIGKILL`）。验证这是否是由于任务持续时间过长或配置不当（例如，`--timeout` 值过小）导致的。

### 节点状态不是 SHUTDOWN

如果节点状态不是 `SHUTDOWN`，请验证 `loop_count_wait_fragments_finish` 是否设置为正整数，或者 BE/CN 在退出前是否报告了心跳（如果没有，请将 `graceful_exit_wait_for_frontend_heartbeat` 设置为 `true`）。