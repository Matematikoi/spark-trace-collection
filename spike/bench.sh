#!/usr/bin/env bash
# Latency benchmark for a running LqpServer: sends each .sql file sequentially.
# usage: spike/bench.sh <dir-with-sql-files> [n] [port]
set -euo pipefail

DIR="$1"
N="${2:-60}"
PORT="${3:-8080}"
FILES=$(ls "$DIR"/*.sql | awk -v n="$N" -v total="$(ls "$DIR"/*.sql | wc -l)" \
  'BEGIN { step = total / n } NR >= next_pick && picked < n { print; picked++; next_pick += step }')

printf "file\thttp\tclient_ms\tplanning_ms\n"
start=$(python3 -c 'import time; print(time.time())')
for f in $FILES; do
  hdr=$(mktemp)
  out=$(jq -n --rawfile sql "$f" '{sql: $sql}' |
    curl -s -o /dev/null -D "$hdr" -w "%{http_code}\t%{time_total}" -X POST "localhost:$PORT/lqp" --data-binary @-)
  code=${out%%$'\t'*}
  client_ms=$(awk -v t="${out#*$'\t'}" 'BEGIN { printf "%.1f", t * 1000 }')
  plan_ms=$(grep -i x-planning-time-ms "$hdr" | tr -d '\r' | awk '{ print $2 }')
  rm -f "$hdr"
  printf "%s\t%s\t%s\t%s\n" "$(basename "$f")" "$code" "$client_ms" "${plan_ms:--}"
done
end=$(python3 -c 'import time; print(time.time())')
python3 -c "print(f'# batch wall time: {($end - $start):.2f} s')"
