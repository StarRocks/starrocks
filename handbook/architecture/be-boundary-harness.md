# BE Boundary Harness

The backend harness introduced in phase 1 enforces a small core module lattice mechanically.

## Source of Truth

- Manifest: `be/module_boundary_manifest.json`
- Checker: `build-support/check_be_module_boundaries.py`
- Frozen legacy debt: `build-support/be_module_boundary_baseline.json`
- Generated AGENTS section: `build-support/render_be_agents.py`

## Commands

```bash
python3 build-support/check_be_module_boundaries.py --mode full
python3 build-support/check_be_module_boundaries.py --mode changed --base origin/main
python3 build-support/check_be_module_boundaries.py --mode changed --base origin/main --enforce-baseline-shrink
python3 build-support/render_be_agents.py --check
```

## Working Rules

- Change the manifest before changing the generated module harness text.
- Keep the baseline shrink-only; reviewed debt should only disappear.
- If a new dependency edge violates the harness, fix the code or update the manifest deliberately instead of extending the baseline by default.
