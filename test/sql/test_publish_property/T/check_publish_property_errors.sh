#!/usr/bin/env bash

# The three answers that are not a set of properties. Each reply is HTTP 200 and says how it went in
# its body, so what is read here is `status` rather than the status line -- a caller that went by the
# status line would take all three for successes.
#
# Usage: env mysql_cmd=... bash check_publish_property_errors.sh

node=$({ ${mysql_cmd} -Ne "SHOW BACKENDS"; ${mysql_cmd} -Ne "SHOW COMPUTE NODES"; } |
    awk -F '\t' '$9 == "true" { print $2 ":" $5 }' | head -1)

ask() {
    curl -fsS --connect-timeout 1 --max-time 3 -u root: "http://${node}/api/publish_property?$1"
}

# A tablet id that names no index in memory. This is the only one of the three that could ever be a
# real tablet, which is why it is NOT_FOUND rather than an argument complaint.
echo "absent=$(ask "type=pk&tablet_id=999999999" | jq -r '.status')"
echo "unknown_type=$(ask "type=nosuch" | jq -r '.status')"
echo "missing_type=$(ask "tablet_id=1" | jq -r '.status')"
echo "missing_tablet_id=$(ask "type=pk" | jq -r '.status')"
echo "bad_tablet_id=$(ask "type=pk&tablet_id=abc" | jq -r '.status')"
