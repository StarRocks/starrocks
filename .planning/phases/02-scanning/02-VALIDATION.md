---
phase: 2
slug: scanning
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-06
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework (FE)** | JUnit 5 (Jupiter) with Mockito |
| **Framework (BE)** | Google Test (gtest) |
| **FE quick run** | `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest` |
| **BE quick run** | `./run-be-ut.sh --gtest_filter "ADBCScannerTest*"` |
| **FE full suite** | `./run-fe-ut.sh --test com.starrocks.connector.adbc` |
| **Full suite** | `./run-fe-ut.sh && ./run-be-ut.sh` |
| **Estimated runtime** | ~120 seconds (FE UT ~60s, BE UT ~60s) |

---

## Sampling Rate

- **After every task commit:** `cd fe && mvn compile -pl fe-core -am -DskipTests -q` (FE) or quick BE compile check
- **After every plan wave:** FE UT for ADBC tests + BE compile
- **Before `/gsd:verify-work`:** Full FE + BE build + all ADBC unit tests green
- **Max feedback latency:** 120 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | OPT-01 | unit | `./run-fe-ut.sh --test com.starrocks.sql.optimizer.operator.ADBCScanOperatorTest` | Wave 0 | pending |
| 02-01-02 | 01 | 1 | OPT-02 | unit | `./run-fe-ut.sh --test com.starrocks.sql.optimizer.rule.ADBCScanImplementationRuleTest` | Wave 0 | pending |
| 02-02-01 | 02 | 1 | SCAN-04, SCAN-05, SCAN-06, OPT-03, OPT-04 | unit | `./run-fe-ut.sh --test com.starrocks.planner.ADBCScanNodeTest` | Wave 0 | pending |
| 02-03-01 | 03 | 2 | SCAN-02, SCAN-03, SCAN-07 | unit | `./run-be-ut.sh --gtest_filter "ADBCScannerTest*"` | Wave 0 | pending |

*Status: pending / green / red / flaky*

---

## Wave 0 Requirements

- [ ] `fe/fe-core/src/test/java/com/starrocks/planner/ADBCScanNodeTest.java` -- covers SCAN-04/05/06, OPT-03/04
- [ ] `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/operator/ADBCScanOperatorTest.java` -- covers OPT-01
- [ ] `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/ADBCScanImplementationRuleTest.java` -- covers OPT-02
- [ ] `be/test/connector/adbc_connector_test.cpp` -- covers SCAN-02/03/07
- [ ] Thrift IDL additions (TADBCScanNode, ADBC_SCAN_NODE, ADBC_TABLE, TADBCTable) -- prerequisites for all

*Wave 0 tests are created alongside implementation in each plan.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| SCAN-01: FE ADBC metadata via Java API | SCAN-01 | Already covered by Phase 1 ADBCMetadataTest | Existing tests pass |
| Multi-endpoint parallel reading | SCAN-02 | Requires live FlightSQL server with partitioned data | Connect to test FlightSQL server, verify parallel endpoint consumption |
| LIMIT correctness with multi-endpoint | SCAN-06 | Requires live server returning multiple endpoints | `SELECT * FROM table LIMIT 1`, verify exactly 1 row returned |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 120s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
