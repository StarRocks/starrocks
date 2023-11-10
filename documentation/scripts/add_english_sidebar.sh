#!/bin/bash

header="english-header.txt"
cat > "$header" << EOF
---
displayed_sidebar: "English"
---

EOF

find docs -type d -name assets -prune -o \
  -type f -name "*\.md*" \
  | while read file; do
  cat - "$file" < "$header" > "$file.new" && mv "$file.new" "$file"
done

rm english-header.txt
