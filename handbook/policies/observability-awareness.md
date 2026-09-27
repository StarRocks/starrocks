# Observability Awareness

## Intent

Prevent feature development, bug fixes, and performance optimizations from bypassing or weakening the observability already present in an affected production path.

## Applies To

- Feature development, bug fixes, and performance optimizations in BE and FE production paths.
- Agent implementation, review, verification, and PR handoff for those changes.

## Enforcement

- Before implementation, read neighboring code and trace callers and callees to identify existing metrics, logs, traces, profiles, error codes and messages, audit records, system tables, and administrative APIs. Understand where each signal is created, updated, aggregated, and exposed.
- When adding or changing a state, branch, asynchronous phase, retry, timeout, cancellation, fallback, or error path, check whether every applicable existing signal still runs once and retains accurate semantics. Extend the established mechanism when needed instead of creating a parallel one.
- For a new feature, ask whether operators can tell when it is used, whether it is progressing or healthy, why it failed, and where time or resources are spent. Reuse and extend an applicable existing mechanism; add a new signal only when the existing mechanisms cannot answer an operationally relevant question.
- For a bug fix, make sure the fix does not hide the original failure, discard useful context, or bypass an existing diagnostic signal.
- For a performance change, keep the optimized and fallback behavior explainable through the existing benchmark and profiling workflow, and ensure observability work does not materially distort the affected hot path.
- Follow the existing ownership and local conventions for names, units, labels, profile hierarchy, trace relationships, lifetimes, batching or sampling, error context, tests, and documentation. Avoid unbounded labels, sensitive payloads, and noisy logs.

## Verification and Handoff

- Exercise the relevant success, failure, and fallback paths and inspect the affected signals. Update focused tests when an existing or new observability surface changes.
- In the final change summary or PR description, name the signals reviewed, describe what was preserved or extended, or explain why the existing observability was already sufficient.

## Exceptions

- No new signal is required when the review shows that the existing observability remains accurate and sufficient; record that conclusion in the change summary or PR description.
- Changes limited to tests, documentation, generated code, Java extensions, or CI/tooling are outside this policy unless they directly change or validate an existing observability surface.

Use the matching domain guide for concrete observability touchpoints:

- [Backend](../domains/backend.md#observability-touchpoints)
- [Frontend](../domains/frontend.md#observability-touchpoints)
