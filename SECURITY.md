# Security

flight-lakehouse is a portfolio and learning project, not a production system. It is meant
to run locally in Docker Compose: the services bind to `127.0.0.1`, and several of them run
without authentication (Grafana as an anonymous admin, Airflow with every user an admin,
Trino with no login). Don't expose the stack to a network you don't trust.

The credentials in `.env.example` are placeholders for that local stack. Replace them if you
run it anywhere else.

## Reporting a vulnerability

Please report a vulnerability privately through
[GitHub's private vulnerability reporting](https://github.com/javigarlop/flight-lakehouse/security/advisories/new)
rather than a public issue. Include what you found, how to reproduce it and the commit you
tested. I'll reply as soon as I can; this is a one-person project, so there is no fixed
response time.

Only the latest commit on `main` is supported.
