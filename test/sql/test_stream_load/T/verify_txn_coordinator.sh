#!/bin/bash

# Step 1: Extract TxnCoordinator IPs
coordinator_ips=$(curl -s "${url}/system?path=//transactions/${db}/finished" -uroot: |
  tr -d '\n' |
  grep -oP '<tr>.*?<td>BE: \K[\d.]+' |
  sort)
echo $(wc -l <<< "$coordinator_ips")

# Step 2: Extract Backend IPs
backend_ips=$(curl -s "${url}/system?path=//backends" -uroot: |
  tr -d '\n' |
  sed 's/<tr>/@/g;s/<\/tr>/@/g' |
  tr '@' '\n' |
  sed -n '/<td>/p' |
  while IFS= read -r line; do
    echo "$line" |
    grep -oP '<td[^>]*>.*?</td>' |
    sed -n '2{s/<[^>]*>//g;p}'
  done |
  sort -u)

# Step 3: Check if all Coordinator IPs exist in Backend IPs
all_exist=true
for ip in $coordinator_ips; do
    if ! grep -qxF "$ip" <<< "$backend_ips"; then
        all_exist=false
        break
    fi
done

echo $all_exist
