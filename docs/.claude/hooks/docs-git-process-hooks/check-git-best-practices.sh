#!/usr/bin/env bash
# PreToolUse hook: blocks prohibited git operations.
#
# Adapted from MongoDB Documentation (https://github.com/mongodb/docs),
# licensed under Creative Commons Attribution-NonCommercial-ShareAlike 3.0
# (https://creativecommons.org/licenses/by-nc-sa/3.0/).

RULES_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/git-best-practices.json"

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""')

while IFS= read -r rule; do
  pattern=$(printf '%s' "$rule" | jq -r '.pattern')
  message=$(printf '%s' "$rule" | jq -r '.message')
  if printf '%s' "$cmd" | grep -qE "$pattern"; then
    jq -n --arg msg "$message" \
      '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":$msg}}'
    exit 0
  fi
done < <(jq -c '.rules[]' "$RULES_FILE")
