<p align="center"><img src="https://github.com/daschr/logsqlite/raw/resources/images/LogSQlite%20Main%20Logo.png" alt="Logo" width="600"/></p>

# LogSQlite
Moby/Docker logging driver plugin which uses sqlite3 databases.

Allows **faster** querying of logs (f.e using `docker logs --since/--until`) than the default JSON File logging driver

# Building
* `cargo b --release`

# Installation (systemd)
1. `cp logsqlite /usr/local/bin/`
2. `mkdir /etc/logsqlite && cp conf.ini /etc/logsqlite/`
3. `cp logsqlite.service /etc/systemd/system/ && systemctl daemon-reload`
4. `mkdir /var/spool/logsqlite/`
5. `systemctl enable logsqlite && systemctl start logsqlite`

# Configuration
See `conf.ini`

# Using the driver
- as the default logging driver:
  - add `"log-driver": "logsqlite"` in the `daemon.json`
- per container
  - `docker run --log-driver logsqlite`
  - or in the docker-compose: https://docs.docker.com/compose/compose-file/compose-file-v3/#logging  
