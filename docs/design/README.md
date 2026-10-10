# Iceberg table encryption — design docs

| File | What it is |
|---|---|
| `iceberg-table-encryption-pme-design.md` | The design: Parquet Modular Encryption for Iceberg data files, Standard/PME key management, Iceberg 1.11.0 |
| `diagrams/*.puml` | **Source of truth for the diagrams** (PlantUML) |
| `*.png` | Generated output — do not edit by hand |

## Regenerating the diagrams

```sh
./render-diagrams.sh
```

Downloads PlantUML on first run into `.plantuml/` (git-ignored) and renders every
`diagrams/*.puml` to a PNG beside the docs. Only a JVM is needed — PlantUML renders sequence
diagrams natively and the two component diagrams use `!pragma layout smetana`, so no Graphviz
and no browser are required.

Already have a jar: `./render-diagrams.sh /path/to/plantuml.jar`, or set `PLANTUML_JAR`. Behind a
proxy that blocks Maven Central, set `PLANTUML_URL` to a mirror.

**Keep the jar out of the repo.** It is ~28 MB. `.plantuml/` is git-ignored for that reason, and it
has been committed by accident once already — if you pass your own path, put it outside the tree.

## Why PlantUML rather than Mermaid

The diagrams began as Mermaid, rendered by hand and saved from a preview. Nothing recorded how to
remake them and the sources were never committed, so when the design changed the PNGs silently kept
stating the old behaviour — one claimed the Parquet cipher was hardcoded long after the code started
reading the table property. Rendering Mermaid needs headless Chromium; PlantUML needs a JVM and a
jar, so the render is scriptable and reproducible.

**Edit the `.puml`, re-run the script, commit both.**
