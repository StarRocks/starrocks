# AGENTS.md - Expression Extensions

This directory contains expression implementations that are intentionally kept outside core `exprs/`.

Use `exprs_ext/` for built-in expression extensions that need integration with higher BE modules such as storage, compute environment, execution runtime, or platform helpers. Keep reusable expression contracts, common scalar helpers, UDF runtime helpers, and aggregate registry mechanics in core `exprs/` instead.

Read the nearest nested guide before changing a group:

- `dict/AGENTS.md` for dictionary expression extensions.
- `table_function/AGENTS.md` for table-function expression extensions.
- `utility/AGENTS.md` for utility expression functions.

After changing module ownership or layering rules, edit `be/module_boundary_manifest.json` first, then run `python3 build-support/render_be_agents.py --write`.
