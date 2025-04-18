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
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' ON FRONTEND WITH 10 TIMES;

// Trigger with 10% probability
ADMIN ENABLE FAILPOINT 'bdb_ha_get_leader_exception' ON FRONTEND WITH 0.1 PROBABILITY;

// Disable
ADMIN DISABLE FAILPOINT 'bdb_ha_get_leader_exception' ON FRONTEND;
```