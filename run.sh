#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "Running ETL pipeline..."
python pipeline.py

echo ""
echo "Launching dashboard..."
streamlit run app.py --server.port 8501
