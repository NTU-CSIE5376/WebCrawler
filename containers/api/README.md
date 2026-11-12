# API database deployment

The checked-in API configuration routes against the crawler Yugabyte database
at `172.16.191.2:5433`, database `crawlerdb`. This endpoint is configuration in
Git; database credentials are external to the repository. There is no implicit
fallback to the old PostgreSQL endpoint.

Compose mounts the `crawler_api_database` secret at
`/run/secrets/crawler_api_database`. Its host source defaults to
`/data/secrets/webcrawler/api-database.json`. Set
`CRAWLER_API_DB_CREDENTIALS_FILE` only when the credential file lives elsewhere.
The file is a JSON object containing exactly `username` and `password`.
`username` must be a non-empty string. `password` must be a non-empty string or
explicit JSON `null`. The latter preserves an intentionally passwordless
connection, as used by the currently working production API; it does not borrow
another service's credentials. The password field is still required, and an
empty string is rejected. Do not add endpoint settings or a DSN to the secret;
those belong in the checked-in configuration.

Provision this file with restrictive permissions readable by the API runtime
user. Preserve the currently deployed credentials when aligning an existing
installation. Do not paste credentials into shell history, PRs, or logs.

For an existing API container, stage the same credential file at the expected
container path **before** promoting this configuration or reloading the API
programs. For subsequent container creation, Compose mounts the external file.
Keep the existing API exposure, proxy authentication, and durable job/result
directories unchanged. Both API workers and the job GC load this configuration.
The loader fails clearly when credentials are absent, unreadable, or malformed.

Validate startup configuration without logging the resulting URL or password;
report only driver, hostname, port and database. Then verify routing with a
small test job. This change does not migrate or modify database data.

An explicit `postgres.dsn` is still accepted in an isolated custom configuration
selected with `API_CONFIG_PATH`. It cannot be combined with the structured
endpoint settings. Production should use the checked-in configuration and the
credential-only secret rather than a private configuration fork.
