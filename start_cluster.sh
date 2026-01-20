#!/bin/bash
# Start/stop a 3-node RAFT cluster for local testing
# Usage: ./start_cluster.sh [stop]

if [ "$1" = "stop" ]; then
    pkill -TERM -f "server.py.*9001" 2>/dev/null
    pkill -TERM -f "server.py.*9002" 2>/dev/null
    pkill -TERM -f "server.py.*9003" 2>/dev/null
    echo "Cluster stopped"
    exit 0
fi

python3 app/server.py --node-id node-1 --port 9001 \
    --peers "node-2:localhost:9002,node-3:localhost:9003" &

python3 app/server.py --node-id node-2 --port 9002 \
    --peers "node-1:localhost:9001,node-3:localhost:9003" &

python3 app/server.py --node-id node-3 --port 9003 \
    --peers "node-1:localhost:9001,node-2:localhost:9002" &

echo "Started 3-node cluster on ports 9001, 9002, 9003"
echo "Stop with: $0 stop"
