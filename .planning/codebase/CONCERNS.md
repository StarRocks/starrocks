# Known Concerns & Technical Debt

## Architecture Concerns

### 1. Dual Build Systems (FE)
**Issue**: FE has both Maven (`pom.xml`) and Gradle (`build.gradle.kts`)
- Maven is primary and maintained
- Gradle exists but may be outdated
- Potential for configuration drift

**Impact**: Confusion for new contributors, inconsistent builds

**Mitigation**: Standardize on Maven, document build process clearly

### 2. Mixed RPC Protocols
**Issue**: Multiple RPC mechanisms in use
- Thrift (legacy FE-BE communication)
- gRPC (newer lake service)
- BRPC (some BE internal)

**Impact**: Operational complexity, protocol evolution challenges

### 3. JNI Connector Overhead
**Issue**: Java extensions use JNI for external format reading
- Context switching between C++ and Java
- Memory management complexity
- GC pauses affecting query performance

**Mitigation**: Incremental migration to native C++ connectors where possible

## Code Quality Concerns

### 1. Legacy Code in `analysis/` and `planner/`
**Location**: `fe/fe-core/src/main/java/com/starrocks/analysis/`

**Issue**: Legacy analyzer and planner code still exists alongside new `sql/` package
- Maintenance burden
- Risk of using wrong code path
- Technical debt accumulation

**Recommendation**: Complete migration to new SQL framework, deprecate legacy

### 2. Large Files
**Issue**: Some files exceed reasonable size limits
- `FunctionSet.java`: Large function registry
- Some test files with excessive line counts

**Impact**: Code review difficulty, merge conflicts

### 3. TODO/FIXME Comments
**Pattern**: Scattered TODO comments indicating incomplete work
- Search: `grep -r "TODO\|FIXME" --include="*.java" --include="*.cpp" | wc -l`
- Many are old and may be outdated

## Performance Concerns

### 1. Memory Management (BE)
**Issue**: Manual memory management in hot paths
- Risk of memory leaks
- Memory fragmentation under load
- Complex lifecycle management

**Areas**: Storage engine, query execution buffers

### 2. Metadata Cache Invalidation
**Issue**: FE metadata caching may have stale data edge cases
- Catalog metadata consistency
- Statistics freshness

### 3. Compaction Strategy
**Issue**: Tablet compaction can be resource-intensive
- Write amplification
- Background task scheduling
- Shared-data mode complexity

## Security Concerns

### 1. SQL Injection Prevention
**Status**: Generally well-protected via parameterized queries
**Concern**: Custom parsers for certain syntax need review

### 2. Privilege Escalation
**Location**: `fe/fe-core/src/main/java/com/starrocks/authorization/`

**Concern**: Complex privilege model may have edge cases
- Regular security audits recommended
- Test coverage for privilege checks

### 3. Credential Storage
**Issue**: Cloud storage credentials in configuration
**Mitigation**: Support for credential providers (AWS IAM, etc.)

## Testing Concerns

### 1. Flaky Tests
**Issue**: Some tests exhibit non-deterministic behavior
- Timing-dependent tests
- Resource cleanup issues
- Race conditions in concurrent tests

**Mitigation**: Ongoing effort to stabilize, use `FLAKY` marker

### 2. Test Coverage Gaps
**Areas with lower coverage**:
- Error handling paths
- Edge cases in connectors
- Concurrent access scenarios

### 3. Integration Test Speed
**Issue**: Full SQL test suite takes significant time
- Long feedback loop for developers
- CI pipeline duration

## Dependency Concerns

### 1. Third-Party Library Updates
**Issue**: Many pinned dependencies, some may have CVEs
- Regular security scanning (Trivy configured)
- Update cadence for dependencies

### 2. Java Version
**Current**: Java 17
**Concern**: Keeping up with LTS releases

### 3. Native Dependencies
**Issue**: BE relies on many native libraries
- Build complexity
- Cross-platform support (Linux primary, Mac dev)

## Build & Development Concerns

### 1. Build Time
**Issue**: Full build takes significant time
- BE compilation especially slow
- Thirdparty library compilation

**Mitigation**: Docker-based builds, ccache, incremental builds

### 2. IDE Configuration
**Issue**: Multiple IDEs supported, configuration drift
- IntelliJ (primary)
- VS Code (growing)
- CLion (for BE)

### 3. Developer Onboarding
**Concern**: Complex setup for new contributors
- Thirdparty dependencies
- Environment configuration
- Build toolchain

## Documentation Concerns

### 1. Code Documentation
**Issue**: Inconsistent JavaDoc/comment coverage
- Public APIs should be documented
- Complex algorithms need explanation

### 2. Architecture Documentation
**Concern**: High-level architecture docs may drift from implementation
- Keep diagrams updated
- Document design decisions

## Operational Concerns

### 1. Observability
**Status**: Good metrics coverage
**Gap**: Distributed tracing across FE-BE

### 2. Configuration Management
**Issue**: Many configuration parameters
- Discovery difficulty
- Validation complexity

### 3. Upgrade Process
**Concern**: Zero-downtime upgrades require careful coordination
- FE leader failover
- BE rolling restart
- Schema compatibility

## Recommendations

### Short Term
1. Stabilize flaky tests
2. Document build process clearly
3. Remove truly dead legacy code

### Medium Term
1. Consolidate RPC protocols
2. Improve test coverage in weak areas
3. Automate dependency updates

### Long Term
1. Modernize JNI connectors
2. Improve developer experience
3. Enhance observability

---
*Mapped: 2026-03-18*
*Note: This document reflects current known concerns. Regular review recommended.*
