<p align="center"><img src="https://github.com/daschr/logsqlite/raw/resources/images/LogSQlite%20Main%20Logo.png" alt="Logo" width="600"/></p>

# LogSQlite
Logging driver plugin for fast querying on huge logs.

Ever had to query container logs containing thousands of lines using `docker logs --since <date> --until <data> <your container>` and experienced it's ridiculously long seek time?<br>
But you don't want to setup a whole log management platform?

*Then you may use this plugin!*

# Installation
Just `docker plugin install daschr/logsqlite && docker plugin enable daschr/logsqlite`

# Using the driver
## with docker
`docker run --log-driver daschr/logsqlite hello-world`
## with docker-compose
```
version: "3"

services:
    container:
        image: hello-world
        logging:
            driver: "daschr/logsqlite"
            # the following is purely optional; the set values are the default ones
            options:
                # in ms, timeout value for a new log message of a message burst
                message_read_timeout: "100"
                # maximum number of lines a transaction of a burst can have before it gets committed
                max_lines_per_tx: "10000"
                # maximum size a burst of messages (which forms a transaction) can have
                # before it gets committed
                # use G(iga), M(ega), K(ilo) bytes
                max_size_per_tx: "10M"
                # the maximum age of an log entry (else it will be purged)
                # comment out to not limit by age
                # use w(eeks), d(ays), h(ours), m(onths), s(econds)
                # cleanup_age: 30d

                # limit the logs by number of lines
                cleanup_max_lines: "10000000"
                # whether the log of the container is deleted or not (default: true)
                delete_when_stopped: "true"
```

# Options
See the *docker-compose* example.

# Source Code
See https://github.com/daschr/logsqlite
