# Harness Engineering Roadmap

- Status: active
- Owner: Engineering Productivity
- Last Updated: 2026-03-27

## Summary

Track the staged conversion of StarRocks toward a harness-engineering workflow where repo-local knowledge, mechanical checks, and agent-legible plans are first-class assets.

## Acceptance Criteria

- Root entrypoints route agents to `handbook/` for repo topology and active plans.
- Phase-1 BE boundary harness remains the structural guardrail baseline.
- Handbook structure is validated mechanically in CI.

## Decision Log

- 2026-03-27: Keep internal engineering knowledge outside `docs/` to avoid mixing with public documentation and translation automation.
- 2026-03-27: Use `handbook/` as the internal knowledge root for architecture maps and execution plans.
