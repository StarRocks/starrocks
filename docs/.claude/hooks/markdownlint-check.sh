#!/usr/bin/env bash
# PostToolUse hook: runs markdownlint-cli2 on edited Markdown files and
# feeds results back to Claude as context.

DOCS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v markdownlint-cli2 &>/dev/null; then
    exit 0
fi

if [ ! -f "$DOCS_ROOT/.markdownlint.json" ]; then
    exit 0
fi

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // ""')

if [[ "$file" =~ \.(md|mdx)$ ]] && [[ "$file" =~ /(en|zh|ja)/ || "$file" =~ ^(en|zh|ja)/ ]]; then
    output=$(cd "$DOCS_ROOT" && markdownlint-cli2 --config .markdownlint.json "$file" 2>&1)
    if [ $? -ne 0 ]; then
        message="markdownlint findings for ${file}:\n${output}"
        printf '%s' "$message" | jq -Rs '{"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":.}}'
    fi
fi

exit 0
