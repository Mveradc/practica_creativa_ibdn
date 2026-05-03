#!/bin/bash

docker cp data/origin_dest_distances.jsonl cassandra:/tmp/origin_dest_distances.jsonl

docker exec -i cassandra python3 - <<'PY'
import json
INFILE = '/tmp/origin_dest_distances.jsonl'
OUTFILE = '/tmp/import_origin_dest.cql'

with open(INFILE) as fin, open(OUTFILE, 'w') as fout:
    for line in fin:
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        row = {
            'origin': obj['Origin'],
            'dest': obj['Dest'],
            'distance': obj['Distance'],
        }
        fout.write(
            "INSERT INTO agile_data_science.origin_dest_distances JSON '%s';\n"
            % json.dumps(row)
        )
PY

docker exec -i cassandra cqlsh -f /tmp/import_origin_dest.cql

echo "DONE"
