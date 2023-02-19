#!/bin/bash

function handle_sigterm {
    echo "SIGTERM caught"
}

trap handle_sigterm SIGTERM

for i in {1..1000}; do
    echo "mock-service-2: ${i}"
    sleep 1
done