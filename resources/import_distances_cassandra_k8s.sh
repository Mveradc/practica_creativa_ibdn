#!/bin/bash
set -eo pipefail

NAMESPACE="${NAMESPACE:-flight-prediction}"
POD="${POD:-cassandra-0}"
SRC="${SRC:-data/origin_dest_distances.jsonl}"

if [ ! -f "$SRC" ]; then
  echo "❌ No existe $SRC"
  exit 1
fi

if ! kubectl get pod "$POD" -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "❌ Pod $POD no encontrado en namespace $NAMESPACE"
  exit 1
fi

echo "📤 Copiando $SRC a $POD:/tmp/origin_dest_distances.jsonl ..."
kubectl cp "$SRC" "$NAMESPACE/$POD:/tmp/origin_dest_distances.jsonl"

echo "🛠  Generando CQL dentro del pod ..."
kubectl exec -i -n "$NAMESPACE" "$POD" -- python3 - <<'PY'
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

echo "📥 Cargando CQL en Cassandra ..."
kubectl exec -i -n "$NAMESPACE" "$POD" -- cqlsh -f /tmp/import_origin_dest.cql

echo "✅ DONE"
