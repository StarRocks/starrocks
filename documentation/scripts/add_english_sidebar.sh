#!/bin/bash

header="english-header.txt"
cat > "$header" << EOF
---
displayed_sidebar: "English"
---

EOF

find versioned_docs -type d -name _assets -prune -o \
  -type f -name "*\.md*" \
  | while read file; do
  cat - "$file" < "$header" > "$file.new" && mv "$file.new" "$file"
done
find releasenotes -type d -name _assets -prune -o \
  -type f -name "*\.md*" \
  | while read file; do
  cat - "$file" < "$header" > "$file.new" && mv "$file.new" "$file"
done

rm english-header.txt
