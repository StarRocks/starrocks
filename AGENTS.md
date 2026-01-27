# AGENTS.md - StarRocks

> This file provides context for AI coding agents working with the StarRocks codebase.
> Compatible with: Claude Code, OpenCode, Cursor, Gemini CLI, Windsurf, Aider, Continue, Cline, and other MCP-compatible tools.

## Project Overview

StarRocks is a high-performance, real-time analytical database. It delivers sub-second query latency for complex analytics workloads through its MPP (Massively Parallel Processing) architecture.

**Key capabilities:**
- Real-time analytics with sub-second latency
- High-concurrency, low-latency queries
- Support for both shared-nothing and shared-data deployments
- Materialized views for query acceleration
- External data source federation (Hive, Iceberg, Hudi, Delta Lake, JDBC, etc.)

## Architecture

StarRocks uses a decoupled frontend-backend architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (FE)                          │
│  Java │ SQL Parsing │ Query Planning │ Metadata Management  │
└────────────────────────────┬────────────────────────────────┘
                             │ Thrift RPC
┌────────────────────────────▼────────────────────────────────┐
│                      Backend (BE)                           │
│  C++ │ Query Execution │ Storage Engine │ Data Processing   │
└─────────────────────────────────────────────────────────────┘
```

- **FE (Frontend)**: Java-based. Handles SQL parsing, query optimization, metadata management, and cluster coordination.
- **BE (Backend)**: C++-based. Handles query execution, storage management, and data processing.

## Directory Structure

```
starrocks/
├── be/                    # Backend (C++) - Query execution & storage
├── fe/                    # Frontend (Java) - SQL & metadata
│   ├── fe-core/          # Core FE logic
│   ├── fe-parser/        # SQL parser
│   ├── fe-grammar/       # ANTLR grammar files
│   ├── fe-type/          # Type system
│   ├── fe-spi/           # Service Provider Interfaces
│   ├── connector/        # External data source connectors
│   └── plugin/           # FE plugins
├── java-extensions/       # JNI connectors for external sources
├── gensrc/               # Generated code (Thrift, Protobuf)
├── test/                 # SQL integration tests
├── docs/                 # Documentation (Docusaurus)
├── thirdparty/           # Third-party dependencies
└── docker/               # Docker build files
```

## Quick Commands

### Build

```bash
# Build everything
./build.sh --fe --be --clean

# Build only Backend
./build.sh --be

# Build only Frontend
./build.sh --fe

# Build with specific type (Release/Debug/ASAN)
BUILD_TYPE=Debug ./build.sh --be
BUILD_TYPE=ASAN ./build.sh --be
```

### Run Tests

```bash
# Run all FE unit tests
./run-fe-ut.sh

# Run specific FE test
./run-fe-ut.sh --test com.starrocks.sql.plan.TPCHPlanTest

# Run all BE unit tests
./run-be-ut.sh

# Run specific BE test
./run-be-ut.sh --test CompactionUtilsTest

# Run with gtest filter
./run-be-ut.sh --gtest_filter "TabletUpdatesTest*"

# Run SQL integration tests (requires running cluster)
cd test && python3 run.py -v
```

### Code Formatting

```bash
# Format C++ code (BE)
clang-format -i <file.cpp>

# Check Java code style (FE)
cd fe && mvn checkstyle:check
```

## Code Style Summary

### C++ (Backend)
- **Style**: Google C++ Style (with modifications)
- **Config**: `.clang-format` at project root
- **Indent**: 4 spaces
- **Line limit**: 120 characters
- **Pointer alignment**: Left (`int* ptr`)

### Java (Frontend)
- **Style**: Google Java Style (with modifications)
- **Config**: `fe/checkstyle.xml`
- **Indent**: 4 spaces
- **Line limit**: 130 characters
- **Import order**: Third-party, then Java standard, then static

### Protobuf
- Message names: `PascalCasePB` (e.g., `MyMessagePB`)
- Field names: `snake_case`
- **Never** use `required` fields
- **Never** change field ordinals

### Thrift
- Struct names: `TPascalCase` (e.g., `TMyStruct`)
- Field names: `snake_case`
- **Never** use `required` fields
- **Never** change field ordinals

## Documentation Sync Requirements

### Configuration Changes

**When you add, modify, or remove configuration parameters, you MUST update the corresponding documentation:**

| Component | Documentation File |
|-----------|-------------------|
| FE Config | `docs/en/administration/management/FE_configuration.md` |
| BE Config | `docs/en/administration/management/BE_configuration.md` |

**Required documentation for each config parameter:**
- Parameter name
- Default value
- Value range (if applicable)
- Description of what it controls
- When to use/modify it
- Whether it requires restart

### Metrics Changes

**When you add, modify, or remove metrics, you MUST update the corresponding documentation:**

| Metrics Type | Documentation File |
|--------------|-------------------|
| General Metrics | `docs/en/administration/management/monitoring/metrics.md` |
| Shared-Data Metrics | `docs/en/administration/management/monitoring/metrics-shared-data.md` |
| MV Metrics | `docs/en/administration/management/monitoring/metrics-materialized_view.md` |
| BE Metrics (SQL) | `docs/en/sql-reference/information_schema/be_metrics.md` |
| FE Metrics (SQL) | `docs/en/sql-reference/information_schema/fe_metrics.md` |

**Required documentation for each metric:**
- Metric name
- Type (Counter, Gauge, Histogram)
- Labels (if any)
- Description of what it measures
- Unit (if applicable)

### PR Checklist for Config/Metrics Changes

When your PR includes configuration or metrics changes:

- [ ] Updated corresponding documentation in `docs/en/`
- [ ] Updated Chinese documentation in `docs/zh/` (if exists)
- [ ] Documented default values and valid ranges
- [ ] Added deprecation notice if replacing old config/metric

## Commit & PR Guidelines

### Commit Messages
- Write in English
- Start with a verb in imperative mood (e.g., "Fix", "Add", "Update")
- Be concise but descriptive
- Example: `Fix null pointer exception in tablet compaction`

### PR Title Format

```
[Type] Brief description
```

**Types:**
| Type | Usage |
|------|-------|
| `[BugFix]` | Bug fixes |
| `[Feature]` | New features |
| `[Enhancement]` | Improvements to existing features |
| `[Refactor]` | Code refactoring (no behavior change) |
| `[UT]` | Unit test additions/fixes |
| `[Doc]` | Documentation changes |
| `[Tool]` | Tooling/build changes |

**Examples:**
- `[BugFix] Fix memory leak in hash join operator`
- `[Feature] Add support for ARRAY_AGG function`
- `[Enhancement] Improve partition pruning performance`

### PR Body Template

Every PR must follow this template:

```markdown
## Why I'm doing:
<!-- Explain the motivation and context -->
Describe why this change is needed.

## What I'm doing:
<!-- Describe what changes you made -->
Explain what this PR does.

Fixes #issue_number

## What type of PR is this:
- [ ] BugFix
- [ ] Feature
- [ ] Enhancement
- [ ] Refactor
- [ ] UT
- [ ] Doc
- [ ] Tool

## Does this PR entail a change in behavior?
- [ ] Yes, this PR will result in a change in behavior.
- [ ] No, this PR will not result in a change in behavior.

## If yes, please specify the type of change:
- [ ] Interface/UI changes: syntax, type conversion, expression evaluation, display information
- [ ] Parameter changes: default values, similar parameters but with different default values
- [ ] Policy changes: use new policy to replace old one, functionality automatically enabled
- [ ] Feature removed
- [ ] Miscellaneous: upgrade & downgrade compatibility, etc.

## Checklist:
- [ ] I have added test cases for my bug fix or my new feature
- [ ] This PR needs user documentation (for new or modified features or behaviors)
  - [ ] I have added documentation for my new feature or new function
- [ ] This is a backport PR

## Bugfix cherry-pick branch check:
- [ ] I have checked the version labels which the PR will be auto-backported to the target branch
  - [ ] 4.1
  - [ ] 4.0
  - [ ] 3.5
  - [ ] 3.4
```

### PR Requirements Checklist

1. **One Commit per PR**: Squash multiple commits before merging
2. **Link Issue**: Reference related issue with `Fixes #issue_number`
3. **Tests Required**:
   - Bug fixes must include regression tests
   - New features must include unit tests
4. **Documentation**: Update docs for user-facing changes
5. **Fill Template**: Complete all sections of PR template
6. **CI Must Pass**: All automated checks must be green

### Behavior Change Classification

If your PR changes behavior, classify the change type:

| Change Type | Examples |
|-------------|----------|
| **Interface/UI** | New SQL syntax, changed output format, type conversion changes |
| **Parameter** | Changed default values, new config parameters |
| **Policy** | Auto-enabled features, changed default policies |
| **Feature removed** | Deprecated functionality removed |
| **Compatibility** | Upgrade/downgrade impacts |

### Backport Guidelines

For bug fixes that need to be backported to release branches:

1. Add version labels (e.g., `4.1`, `4.0`, `3.5`) to your PR
2. The CI will auto-create backport PRs after merge
3. Verify backport PRs are created and merged

### Review Process

1. **Minimum 2 Approvals**: At least 2 committers must approve
2. **CI Checks**: All automated checks must pass:
   - Code style (checkstyle, clang-format)
   - Unit tests (FE and BE)
   - Build verification
3. **Address Feedback**: Respond to all review comments
4. **CLA Required**: Sign CLA once: https://cla-assistant.io/StarRocks/starrocks

### CI Pipeline

Your PR will trigger these checks:

| Check | Description |
|-------|-------------|
| `PR CHECKER` | Basic PR validation |
| `FE UT` | Frontend unit tests |
| `BE UT` | Backend unit tests |
| `Build` | Full build verification |
| `Checkstyle` | Java code style |
| `Clang-format` | C++ code style |

### Common PR Issues

| Issue | Solution |
|-------|----------|
| CI timeout | Re-run failed jobs; check for flaky tests |
| Checkstyle failure | Run `mvn checkstyle:check` locally |
| Build failure | Run `./build.sh --fe --be` locally |
| Merge conflicts | Rebase on latest main branch |

## Testing Guidelines

### Unit Tests
- **BE**: Use Google Test framework. Tests in `be/test/` mirror source structure.
- **FE**: Use JUnit 5. Tests in `fe/fe-core/src/test/java/`.

### SQL Integration Tests
- Use the SQL-tester framework in `test/`
- See `test/README.md` for detailed documentation
- Basic execution: `python3 run.py -v`

### Test Requirements
- All new features must have corresponding tests
- Bug fixes should include regression tests
- Maintain or improve test coverage

## Performance Considerations

### Backend (C++)
- Use SIMD instructions where applicable (AVX2/AVX512)
- Prefer vectorized processing over row-by-row
- Use `Column` and `Chunk` abstractions for data processing
- Be mindful of memory allocations in hot paths
- Profile with `perf` or async-profiler before optimization

### Frontend (Java)
- Avoid creating unnecessary objects in hot paths
- Use appropriate data structures for the use case
- Be cautious with synchronized blocks

## Security Guidelines

- Validate all user inputs at system boundaries
- Never log sensitive information (passwords, tokens, PII)
- Follow existing authentication/authorization patterns
- Use parameterized queries to prevent SQL injection

## Nested AGENTS.md Files

For module-specific guidelines, refer to:
- `be/AGENTS.md` - Backend C++ development
- `fe/AGENTS.md` - Frontend Java development
- `java-extensions/AGENTS.md` - JNI connector development
- `gensrc/AGENTS.md` - Generated code handling
- `test/AGENTS.md` - SQL integration testing
- `docs/AGENTS.md` - Documentation contribution

## Useful Resources

- [Official Documentation](https://docs.starrocks.io/)
- [Contributing Guide](./CONTRIBUTING.md)
- [GitHub Issues](https://github.com/StarRocks/starrocks/issues)
- [Slack Community](https://try.starrocks.com/join-starrocks-on-slack)

## License

StarRocks is licensed under the Apache License 2.0.

All source files must include the appropriate license header:
```
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...
```
