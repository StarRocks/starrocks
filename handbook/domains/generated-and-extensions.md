# Generated and Extensions Domain

## Purpose

Map the generated thrift/protobuf surface in `gensrc/` and the Java extension modules that integrate external systems through FE and BE workflows.

## Entrypoints

- [`gensrc/AGENTS.md`](../../gensrc/AGENTS.md)
- [`java-extensions/AGENTS.md`](../../java-extensions/AGENTS.md)
- [`handbook/architecture/schema-compatibility-harness.md`](../architecture/schema-compatibility-harness.md)
- [`build-support/check_gensrc_schema_compatibility.py`](../../build-support/check_gensrc_schema_compatibility.py)
- [`build-support/schema_compatibility_waivers.json`](../../build-support/schema_compatibility_waivers.json)
- [`docs/en/developers/code-style-guides/thrift-guides.md`](../../docs/en/developers/code-style-guides/thrift-guides.md)
- [`docs/en/developers/code-style-guides/protobuf-guides.md`](../../docs/en/developers/code-style-guides/protobuf-guides.md)

## Commands

- `cd gensrc && make script`
- `cd gensrc && make proto`
- `cd gensrc && make thrift`
- `python3 -m unittest build-support/test_check_gensrc_schema_compatibility.py`
- `python3 build-support/check_gensrc_schema_compatibility.py --mode changed --base origin/main`
- `./build.sh --be`
- `./build.sh --fe`

## Guardrails

- Do not hand-edit generated outputs under `gensrc/build/` or active BE build directories.
- Protobuf and thrift fields stay optional/repeated, and ordinals must never be reused.
- Schema deletions require a reviewed waiver entry instead of an implicit compatibility decision.
- Java extensions should follow FE-side Java conventions and JNI safety rules.

## Test and Validation

- Regenerate code from source definitions, then validate by rebuilding the affected side.
- Run the schema compatibility harness before or alongside generator and build validation when source definitions change.
- Use module-focused Maven or FE build/test commands for Java-extension changes.
- Verify compatibility-sensitive schema edits before they leave the source-definition files.

## Open Gaps

- Generator freshness is not yet tied into a broader handbook/eval registry.
- Generated-source ownership and doc obligations are not described by machine-readable manifests.
- Java-extension change selection still depends on broad FE workflows rather than focused harness metadata.
