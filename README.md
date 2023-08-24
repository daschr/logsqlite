# logsqlite
Moby/Docker logging driver plugin which uses sqlite3 databases.

Allows **faster** querying of logs (f.e using `docker logs --since/--until`) than the default JSON File logging driver

# Installation (systemd)
1. `cargo b --release`
2. `cp logsqlite.service /etc/systemd/system/ && systemctl daemon-reload`
3. `mkdir /var/spool/logsqlite/`
4. `systemctl start logsqlite`

# Using the driver
- as the default logging driver:
  - add `"log-driver": "logsqlite"` in the `daemon.json`
- per container
  - `docker run --log-driver logsqlite`
  - or in the docker-compose: https://docs.docker.com/compose/compose-file/compose-file-v3/#logging  
