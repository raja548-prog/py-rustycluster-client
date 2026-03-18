#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# generate_proto.sh
# Regenerates Python gRPC stubs from proto/rustycluster.proto.
#
# Usage:
#   ./scripts/generate_proto.sh
#
# Requirements:
#   pip install grpcio-tools
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PROJECT_ROOT/proto"
OUT_DIR="$PROJECT_ROOT/rustycluster/proto"

echo "▶  Generating proto stubs..."
echo "   Source : $PROTO_DIR/rustycluster.proto"
echo "   Output : $OUT_DIR"

mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
  -I"$PROTO_DIR" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_DIR/rustycluster.proto"

# Fix import path in generated grpc file so it works as a package
# Compatible with both macOS (BSD sed) and Linux (GNU sed)
if sed --version 2>/dev/null | grep -q GNU; then
  sed -i 's/^import rustycluster_pb2 as rustycluster__pb2$/from rustycluster.proto import rustycluster_pb2 as rustycluster__pb2/' \
    "$OUT_DIR/rustycluster_pb2_grpc.py"
else
  sed -i '' 's/^import rustycluster_pb2 as rustycluster__pb2$/from rustycluster.proto import rustycluster_pb2 as rustycluster__pb2/' \
    "$OUT_DIR/rustycluster_pb2_grpc.py"
fi

# Ensure __init__.py exists
if [ ! -f "$OUT_DIR/__init__.py" ]; then
  echo "# Auto-generated proto stubs" > "$OUT_DIR/__init__.py"
fi

echo "✔  Done. Generated files:"
ls -1 "$OUT_DIR"