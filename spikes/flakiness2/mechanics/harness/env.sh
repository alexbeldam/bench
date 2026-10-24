#!/usr/bin/env bash
# One log line with the runner facts: CPU model, image, kernel.
cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')
echo "MECH_ENV {\"cpu\":\"$cpu\",\"nproc\":$(nproc),\"image\":\"${ImageOS:-}-${ImageVersion:-}\",\"kernel\":\"$(uname -r)\",\"time\":\"$(date -u +%FT%TZ)\"}"
