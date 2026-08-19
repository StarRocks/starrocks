# What is failpoint?
Failpoint is a fault injection testing framework that can precisely inject faults at any location within a function, helping to test system fault tolerance and stability.  
Writing failpoints does not require modifying the source code of the system under test (SUT), making it suitable for both R&D engineers (RD) and quality assurance (QA).

# How to use failpoint?
## Defining a failpoint
Defining a failpoint is very simple. Here's an example:  

```text
RULE bdb_ha_get_leader_exception
CLASS com.starrocks.ha.BDBHA
METHOD getLeader()
HELPER com.starrocks.failpoint.FailPointHelper
IF shouldTrigger("bdb_ha_get_leader_exception")
DO throw new RuntimeException("failpoint triggered");
ENDRULE
```

This is a failpoint written in Byteman script that triggers an exception when calling `com.starrocks.ha.BDBHA.getLeader`.

- `RULE`: The name of the rule.
- `CLASS`: The class name where the fault is injected.
- `METHOD`: The method name where the fault is injected. You can also specify parameter types for overloaded methods, e.g., `getLeader(int)`.
    - Below `METHOD`, you can define the injection location, such as:
        - `AT ENTRY`: At the beginning of the method.
        - `AT EXIT`: At the end of the method.
        - If not specified, the default is the beginning of the method.
    - For more location specifiers, refer to:  
      https://downloads.jboss.org/byteman/latest/byteman-programmers-guide.html#location-specifiers
- `HELPER`: The helper class. All functions in this class can be used in the `IF` and `DO` blocks below.
- `IF`: The trigger condition. Here, `shouldTrigger` is fixed, with the parameter being the `RULE` name.
- `DO`: The fault action. In the example, it throws an exception. Other options include:
    - Returning a result directly: `DO return null`.
    - Executing a block of code: `DO sleep(1000)`.
    - Byteman supports powerful execution logic, allowing access to any variables in the context. For complex logic, refer to:  
      https://downloads.jboss.org/byteman/latest/byteman-programmers-guide.html#rule-bindings
- `ENDRULE`: Marks the end of the rule definition.

## Using failpoints
1. Place the written Byteman script in `conf/failpoint.btm` and add the startup option `--failpoint`.

2. Use admin commands to trigger failpoints:  

```text
// Enable permanently
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' ON FRONTEND;

// Disable after 10 executions
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' WITH 10 TIMES ON FRONTEND;

// Trigger with 10% probability
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' WITH 0.1 PROBABILITY ON FRONTEND;

// Pause every thread that reaches the failpoint, until it is disabled
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' WITH PAUSE ON FRONTEND;

// Disable (also releases a pause)
ADMIN DISABLE FAILPOINT 'bdb_ha_get_leader_exception' ON FRONTEND;
```

All of these require the `OPERATE` system privilege.

## Pausing at a failpoint

`WITH PAUSE` blocks every thread that reaches the failpoint instead of injecting a fault. It exists
for the fault pattern that a fail-only failpoint cannot express:

> stop at phase X -> act externally (kill a node, switch the leader) -> release -> assert

```sql
ADMIN ENABLE FAILPOINT 'some_failpoint' WITH PAUSE ON BACKEND '10.0.0.2:9060';
-- ... poll SHOW FAILPOINTS until PausedThreads > 0, then do the external action ...
ADMIN DISABLE FAILPOINT 'some_failpoint' ON BACKEND '10.0.0.2:9060';
```

Points worth knowing:

- **A released pause never injects.** Once released, the trigger evaluates to false and the flow
  continues normally, so arming an existing fail-style failpoint `WITH PAUSE` turns it into a pure
  stop point. To pause *and* fail, arm a second failpoint downstream.
- **Any mode change releases**, not just `ADMIN DISABLE FAILPOINT`.
- **A forgotten disable self-heals.** A parked thread resumes after
  `failpoint_pause_timeout_second` (FE config, default 300, mutable) with a `pause timed out`
  WARNING. The FE sends this value to BEs/CNs with the arming request, so both sides share one
  timeout.
- **Observability.** `SHOW FAILPOINTS` reports `TriggerCount` (cumulative fires) and `PausedThreads`
  (threads parked right now) for backends. FE failpoints are not listed by `SHOW FAILPOINTS`; an FE
  pause logs `failpoint <name> paused, waiting for ADMIN DISABLE FAILPOINT` in `fe.log`, and the
  effect on a reshard job is visible in `information_schema.tablet_reshard_jobs`.
- **Mixed versions are safe but not useful.** A pause is sent as `DISABLE` plus a request-level pause
  flag, so a node that predates this feature simply disables the failpoint rather than arming it.
  Nothing is injected, but nothing pauses either, and such a node reports `DISABLE` rather than
  `PAUSE`. Always confirm a pause with `PausedThreads > 0`, which is the only signal that proves a
  thread actually parked.
- **A pause blocks its logical thread.** The backend wait uses bthread primitives, so a pause inside
  a brpc handler yields the worker rather than occupying it and `ADMIN DISABLE FAILPOINT` stays
  serviceable. On the frontend, `TabletReshardJobMgr` runs every reshard job on one daemon thread, so
  a pause inside a job also freezes the other reshard jobs on that frontend. A node shutdown while a
  thread is parked waits out that thread's pause timeout.

## Build requirement for BE failpoints

FE failpoints need `--failpoint` at FE startup. **BE/CN failpoints exist only in a backend compiled
with `ENABLE_FAULT_INJECTION=ON`** (`ENABLE_FAULT_INJECTION=ON ./build.sh --be`); the default build
has them compiled out and `ADMIN ENABLE FAILPOINT ... ON BACKEND` returns
`FailPoint is not supported, need re-compile BE with ENABLE_FAULT_INJECTION`.
