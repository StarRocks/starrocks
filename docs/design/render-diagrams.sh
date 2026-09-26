#!/bin/bash
# Regenerate docs/design/*.png from docs/design/diagrams/*.puml.
#
# The .puml files are the single source of truth. The PNGs are generated output -- never edit
# them by hand. They were hand-edited once, which is how a diagram came to document a cipher
# the code no longer used.
#
# Usage:  ./render-diagrams.sh [path/to/plantuml.jar]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${1:-${PLANTUML_JAR:-$HERE/.plantuml/plantuml.jar}}"
VERSION="1.2026.6"

if [ ! -f "$JAR" ]; then
    echo "PlantUML jar not found at $JAR; fetching $VERSION..."
    mkdir -p "$(dirname "$JAR")"
    # Set PLANTUML_URL to fetch from an internal mirror or proxy instead.
    curl -fsSL -o "$JAR" \
        "${PLANTUML_URL:-https://repo.maven.apache.org/maven2/net/sourceforge/plantuml/plantuml/${VERSION}/plantuml-${VERSION}.jar}"
fi

# The default jar path lives under .plantuml/, which is git-ignored -- the jar is ~28 MB and
# must never be committed. If you pass your own path, keep it outside the repo.
java -jar "$JAR" -tpng -o "$HERE" "$HERE"/diagrams/*.puml
echo "Rendered:"
ls -1 "$HERE"/*.png
