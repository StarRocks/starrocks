#!/usr/bin/env bash
# PostToolUse hook: runs Vale on edited Markdown files and feeds results
# back to Claude as context.
#
# Adapted from MongoDB Documentation (https://github.com/mongodb/docs),
# licensed under Creative Commons Attribution-NonCommercial-ShareAlike 3.0
# (https://creativecommons.org/licenses/by-nc-sa/3.0/).

DOCS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v vale &>/dev/null; then
    exit 0
fi

if [ ! -f "$DOCS_ROOT/.vale.ini" ]; then
    exit 0
fi

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // ""')

if [[ "$file" =~ \.(md|mdx)$ ]] && [[ "$file" =~ /(en|zh|ja)/ || "$file" =~ ^(en|zh|ja)/ ]]; then
    output=$(cd "$DOCS_ROOT" && vale --config .vale.ini "$file" 2>/dev/null)
    if [ -n "$output" ]; then
        message="Vale lint results for ${file}:\n${output}"
        printf '%s' "$message" | jq -Rs '{"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":.}}'
    fi
fi

exit 0
