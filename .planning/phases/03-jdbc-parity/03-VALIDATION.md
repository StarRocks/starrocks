---
phase: 3
slug: jdbc-parity
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-06
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 5 (Jupiter) + JMockit |
| **Config file** | `fe/pom.xml` (surefire plugin) |
| **Quick run command** | `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest -am` |
| **Full suite command** | `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest -am` |
| **Estimated runtime** | ~60 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest -am`
- **After every plan wave:** Run `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest -am`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 01 | 1 | PART-01, PART-02 | unit | `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest#testListPartitionNames -am` | Partial | ⬜ pending |
| 03-01-02 | 01 | 1 | MV-01 | unit | `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest#testPartitionTraitsRegistered -am` | Partial (W0) | ⬜ pending |
| 03-02-01 | 02 | 1 | STAT-01, STAT-02 | unit | `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest#testGetTableStatistics -am` | Partial | ⬜ pending |
| 03-03-01 | 03 | 2 | MV-01, MV-02, MV-04 | unit (structural) | `cd fe && mvn test -pl fe-core -Dtest=ADBCMetadataTest -am` | Partial | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

**Note on task 03-01-02:** Plan 01 Task 2 creates partition traits tests inside ADBCMetadataTest.java (not a separate ADBCPartitionTraitsTest file). All traits tests live in ADBCMetadataTest.

**Note on task 03-03-01:** MV-02 (refresh behavioral) and MV-04 (query rewrite behavioral) are verified structurally here. Full behavioral tests (MvRefreshAndRewriteADBCTest) require MockedADBCMetadata from Phase 4: Integration Testing.

---

## Wave 0 Requirements

- [x] `ADBCPartitionTraits.java` — new file in `connector/partitiontraits/` (created by Plan 01 Task 1)
- [x] Extend `ADBCMetadataTest.java` — add stats, partition, and MV test methods (created by Plan 01 Task 2, Plan 02 Task 1, Plan 03 Task 1)

*Wave 0 items are created as part of plan tasks. All test files exist or are extended by plan tasks -- no orphaned test references.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| MV-03 (PCT refresh) | MV-03 | DEFERRED per CONTEXT.md | N/A |
| PART-03 (pruning) | PART-03 | DEFERRED per CONTEXT.md | N/A |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 60s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved
