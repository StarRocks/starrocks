#!/bin/bash

header="chinese-header.txt"
cat > "$header" << EOF
---
displayed_sidebar: "Chinese"
---

EOF

find i18n/zh/docusaurus-plugin-content-docs/current -type d -name assets -prune -o \
  -type f -name "*\.md*" \
  | while read file; do
  cat - "$file" < "$header" > "$file.new" && mv "$file.new" "$file"
done

rm chinese-header.txt
