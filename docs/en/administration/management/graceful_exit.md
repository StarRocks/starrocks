---
displayed_sidebar: docs
---

# Graceful Exit

From v3.3 onwards, StarRocks supports Graceful Exit.

## Overview

Graceful Exit is a mechanism designed to support **non-disruptive upgrades and restarts** of StarRocks FE, BE, and CN nodes. Its primary objective is to minimize the impact on running queries and data ingestion tasks during maintenance operations such as node restart, rolling upgrade, or cluster scaling.

Graceful Exit ensures that:

- The node **stops accepting new tasks** once exit begins;
- Existing queries and load jobs are **allowed to complete** within a controlled time window;
- System components (FE/BE/CN) **coordinate status changes** so that the cluster correctly reroutes traffic.

Graceful Exit mechanisms differ between FE and BE/CN nodes, as described below.

### FE Graceful Exit Mechanism

#### Trigger Signal

FE Graceful Exit is initiated via:

```bash
stop_fe.sh -g
```

This sends a `SIGUSR1` signal, while the default exit (without the `-g` option) sends `SIGTERM` signal.

#### Load Balancer Awareness

Upon receiving the signal:

- FE immediately returns **HTTP 500** on the `/api/health` endpoint.
- Load balancers detect the degraded state within ~15 seconds and stop routing new connections to this FE.

#### Connection Drain and Shutdown Logic

**Follower FE**

- Handles read-only queries.
- If the FE node has no active sessions, the connection is closed immediately.
- If SQL is running, the FE node waits for execution to finish before closing the session.

**Leader FE**

- Read request handling is identical to the of Followers.
- Write request handling requires:

  - Closing BDBJE.
  - Allowing a new Leader election to complete.
  - Redirecting subsequent writes to the newly elected Leader.

#### Timeout Control

If a query runs for too long, FE forcibly exits after **60 seconds** (configurable via the `--timeout` option).

### BE/CN Graceful Exit Mechanism

#### Trigger Signal

BE Graceful Exit is initiated via:

```bash
stop_be.sh -g
```

CN Graceful Exit is initiated via:

```bash
stop_cn.sh -g
```

This sends a `SIGTERM` signal, while the default exit (without the `-g` option) sends `SIGKILL` signal.

#### State Transition

After receiving the signal:

- The BE/CN node marks itself as **exiting**.
- It rejects **new query fragments** by returning `INTERNAL_ERROR`.
- It continues processing existing fragments.

#### Wait Loop for In-Flight Queries

The behavior that BE/CN waits for existing fragments to finish is controlled by the BE/CN configuration `loop_count_wait_fragments_finish` (Default: 2). The actual wait duration equals `loop_count_wait_fragments_finish × 10 seconds` (that is, 20 seconds by default). If fragments remain after timeout, BE/CN proceeds with normal shutdown (closing threads, network, and other processes).

#### Improved FE Awareness

From v3.4 onwards, FE no longer marks BE/CN as `DEAD` based on heartbeat failures. It correctly recognizes the BE/CN “exiting” state, allowing significantly longer graceful-exit windows for fragments to be completed.

## Configurations

### FE Configurations

#### `stop_fe.sh -g --timeout`

- Description: Maximum waiting time before FE is force-killed.
- Default: 60 (seconds)
- How to apply: Specify it in the script command, for example, `--timeout 120`.

#### *Minimum LB detection time*

- Description: LB requires at least 15 seconds to detect degraded health.
- Default: 15 (seconds)
- How to apply: Fixed value

### BE/CN Configurations

#### `loop_count_wait_fragments_finish`

- Description: BE/CN wait duration for existing fragments. Multiply the value with 10 seconds.
- Default: 2
- How to apply: Modify it in the BE/CN configuration file or update it dynamically.

#### `graceful_exit_wait_for_frontend_heartbeat`

- Description: Whether BE/CN waits for FE to confirm **SHUTDOWN** via heartbeat. From v3.4.5 onwards.
- Default: false
- How to apply: Modify it in the BE/CN configuration file or update it dynamically.

#### `stop_be.sh -g --timeout`, `stop_cn.sh -g --timeout`

- Description: Maximum waiting time before BE/CN is force-killed. Set it to a value larger than `loop_count_wait_fragments_finish` * 10 to prevent termination before the BE/CN wait duration is reached.
- Default: false
- How to apply: Specify it in the script command, for example, `--timeout 30`.

### Global Switches

Graceful Exit is **enabled by default** from v3.4 onwards. To disable it temporarily, set the BE/CN configuration `loop_count_wait_fragments_finish` to `0`.

## Expected Behavior During Graceful Exit

### Query Workloads

| Query Type                          | Expected Behavior                                                                   |
| ----------------------------------- | ----------------------------------------------------------------------------------- |
| **Short (less than 20 seconds)**    | BE/CN waits long enough, so queries complete normally.                              |
| **Medium (20 to 60 seconds)**       | Queries completed within BE/CN wait window are returned with success; else queries are cancelled and require manual retry. |
| **Long (more than 60 seconds)**     | Queries are likely terminated by FE/BE/CN due to timeout and requires manual retry. |

### Data Ingestion Tasks

- **Loading tasks via Flink or Kafka Connectors** are automatically retried with no user-visible interruption.
- **Stream Load (non-framework), Broker Load, and Routine Load tasks** may fail if connection breaks. Manual retry is required.
- **Background tasks** are automatically re-scheduled and executed by the FE retry mechanism.

### Upgrade and Restart Operations

Graceful Exit ensures:

- No cluster-wide downtime;
- Safe rolling upgrade by draining nodes one at a time.

## Limitations and Version Differences

### Version Behavior Differences

| Version   | Behavior                                                                                                                  |
| --------- | ------------------------------------------------------------------------------------------------------------------------- |
| **v3.3**  | BE Graceful Exit flawed: FE may prematurely mark BE/CN as `DEAD`, causing queries to get cancelled. The effective wait is limited (15 seconds by default). |
| **v3.4+** | Fully supports extended wait duration; FE correctly identifies BE/CN “exiting” state. Recommended for production.         |

### Operational Limitations

- In extreme cases (for example, BE/CN hangs), Graceful Exit may fail. Terminating the process requires `kill -9`, risking partial data persistence (recoverable via snapshot).

## Usage

### Prerequisites

**StarRocks version**:

- **v3.3+**: Basic Graceful Exit support.
- **v3.4+**: Enhanced status management, longer wait windows (up to several minutes).

**Configuration**:

- Make sure `loop_count_wait_fragments_finish` is set to a positive integer.
- Set `graceful_exit_wait_for_frontend_heartbeat` to `true` allow FE to detect BE's "EXITING" state.

### Perform FE Graceful Exit

```bash
./bin/stop_fe.sh -g --timeout 60
```

Parameters:

- `--timeout`: The maximum time to wait before the FE node is force-killed.

Behavior:

- The system sends the `SIGUSR1` signal first.
- After timeout, it falls back to `SIGKILL`.

#### Validate FE State

You can check the FE health via the following API:

```
http://<fe_host>:8030/api/health
```

LB removes the node after receiving consecutive non-200 responses.

### Perform BE/CN Graceful Exit

- **For v3.3:**

  - BE:

  ```bash
  ./be/bin/stop_be.sh -g
  ```

  - CN:

  ```bash
  ./be/bin/stop_cn.sh -g
  ```

- **For v3.4+:**

  - BE:

  ```bash
  ./bin/stop_be.sh -g --timeout 600
  ```

  - CN:

  ```bash
  ./bin/stop_cn.sh -g --timeout 600
  ```

BE/CN exits immediately if no fragments remain.

#### Validate BE/CN Status

Run on FE:

```sql
SHOW BACKENDS;
```

`StatusCode`:

- `SHUTDOWN`: BE/CN Graceful Exit in progress.
- `DISCONNECTED`: BE/CN Node has fully exited.

## Rolling Upgrade Workflow

### Procedure

1. Perform Graceful Exit on the node `A`.
2. Confirm the node `A` is shown as `DISCONNECTED` from the FE side.
3. Upgrade and restart the node `A`.
4. Repeat the above for remaining nodes.

### Monitor Graceful Exit

Check FE logs `fe.log`, BE logs `be.log`, or CN logs `cn.log` to make sure whether there were tasks during the exit.

## Troubleshooting

### BE/CN exits by timeout

If tasks fail to complete within the Graceful Exit period, BE/CN will trigger forced termination (`SIGKILL`). Verify whether this is caused by excessively long task duration or improper configurations (for example, an overly small `--timeout` value).

### Node status is not SHUTDOWN

If the node status is not `SHUTDOWN`, verify whether `loop_count_wait_fragments_finish` is set to a positive integer, or if BE/CN reported a heartbeat before exiting (if not, set `graceful_exit_wait_for_frontend_heartbeat` to `true`).
