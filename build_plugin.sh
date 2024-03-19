#!/bin/bash
set -e
docker build -t logsqlite_rootfs .

id=$(docker create logsqlite_rootfs true)

mkdir -p plugin/rootfs

docker export "$id" | sudo tar -x -C plugin/rootfs

docker rm -vf "$id"

docker plugin create daschr/logsqlite ./plugin/
