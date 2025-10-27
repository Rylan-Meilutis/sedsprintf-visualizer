#!/bin/bash
set -e

source venv/bin/activate

# Start background process
python3 telemetry_ingest.py &
INGEST_PID=$!

# Ensure the background job is killed on script exit
trap "kill $INGEST_PID 2>/dev/null" EXIT

# Run the main visualizer (foreground)
python3 telemetry_visualizer.py --db telemetry.db --refresh-sec 0.3
