# Production pipeline deployment

Use `compose.sh` to combine the checked-in root Compose file with this production
profile. It names the existing `webcrawler` project and never implicitly reads
`docker-compose.override.yml` or a repository `.env` file. PostgreSQL is behind
the `legacy-postgres` profile; Loki/Grafana are behind `infrastructure`. The
wrapper does not accept these infrastructure services as mutation targets.

Keep a private machine-binding file outside the repository. It contains exactly
the five image bindings below and optionally `CRAWLER_API_DB_CREDENTIALS_FILE`:

```text
WEBCRAWLER_CRAWLER_IMAGE=sha256:<64 lowercase hexadecimal characters>
WEBCRAWLER_API_IMAGE=sha256:<64 lowercase hexadecimal characters>
WEBCRAWLER_SCHEDULER_CONTROL_IMAGE=sha256:<64 lowercase hexadecimal characters>
WEBCRAWLER_SCHEDULER_INGEST_IMAGE=sha256:<64 lowercase hexadecimal characters>
WEBCRAWLER_SITEMAP_PATROLLER_IMAGE=sha256:<64 lowercase hexadecimal characters>
```

Read each deployed container's immutable `Image` ID when preparing an alignment
release. Image repository digests (`repository@sha256:...`) are also accepted;
mutable tags, variable expansion, shell commands, duplicates and policy settings
are rejected. The file contains bindings, not runtime tuning or credentials.
Keep policy changes in main. Database credentials use the separate credential-only
file described in [the API deployment guide](../../containers/api/README.md).

```bash
ops/production/compose.sh /absolute/path/runtime.env
ops/production/compose.sh /absolute/path/runtime.env config --format json
ops/production/compose.sh /absolute/path/runtime.env ps
ops/production/compose.sh /absolute/path/runtime.env up api
ops/production/compose.sh /absolute/path/runtime.env recreate sitemap_patroller
```

The default command is read-only configuration rendering. Do not paste full
rendered configurations into logs or PRs; root development services may contain
connection credentials. Mutations require explicit pipeline service names.
`up` and `recreate` never build, pull, or start dependencies; `recreate` explicitly
restarts the named service even when only bind-mounted source changed. There is
no wrapper operation for an unscoped stack deployment, `down`, or image builds.

Before recreating crawlers, preserve their in-memory requests and API progress.
Before recreating ingestion services, finish or safely checkpoint active folders.
This wrapper does not perform either drain automatically. Stage required secrets
before the API restart, and verify a small end-to-end job afterwards.

Every pipeline container starts Supervisor with the root configuration in this
directory. Its include path is a fixed per-service value in `compose.yml`, so
container starts and Docker restarts read the service configuration from the
current `/app` checkout. This avoids a configuration baked into an old image or
an individual bind-mounted file retaining an old inode after Git replaces it.
Do not change Supervisor groups under running crawlers without first preserving
their work; the initial transition to this profile requires a controlled recreate.
For status inside a container, use
`supervisorctl -c /app/ops/production/supervisord.conf status` so the client also
uses the checked-in socket configuration rather than the image's default file.
When rolling back to a release from before this profile existed, restore its
Compose entry point and mount definitions together with its source. Checking out
old source alone would remove the root configuration used by the new entry point.

Existing root Compose volumes, model artifacts under `/app/artifacts`, logging,
API exposure (`127.0.0.1:4000`), resources and durable job directories are retained.
The wrapper clears ambient policy/binding interpolation variables and accepts no
alternative API port or data directory in the machine-binding file. This profile
does not enable the unmerged parser experiment or change descriptor limits.
