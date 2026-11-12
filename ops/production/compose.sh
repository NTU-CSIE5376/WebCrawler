#!/usr/bin/env bash
# Keep policy in main and restrict production mutations to named pipeline services.
set -euo pipefail

usage() {
  printf '%s\n' \
    'Usage: ops/production/compose.sh ENV_FILE [config [--format json|--services]|ps [SERVICE...]|up SERVICE...|recreate SERVICE...|stop SERVICE...]' \
    'ENV_FILE contains five immutable image bindings and optionally the API credential-file path.' \
    'Default command: config. Infrastructure and unscoped mutations are rejected.'
}

fail() { printf 'production compose: %s\n' "$*" >&2; exit 2; }

if [[ ${1:-} == --help ]]; then usage; exit 0; fi
[[ $# -ge 1 ]] || { usage >&2; exit 2; }
env_file=$1
shift
[[ -f $env_file && -r $env_file ]] || fail 'ENV_FILE must be a readable file'
env_file=$(cd -- "$(dirname -- "$env_file")" && printf '%s/%s' "$PWD" "$(basename -- "$env_file")")
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)

image_keys=(WEBCRAWLER_CRAWLER_IMAGE WEBCRAWLER_API_IMAGE WEBCRAWLER_SCHEDULER_CONTROL_IMAGE WEBCRAWLER_SCHEDULER_INGEST_IMAGE WEBCRAWLER_SITEMAP_PATROLLER_IMAGE)
declare -A seen=()
while IFS= read -r line || [[ -n $line ]]; do
  [[ $line =~ ^[[:space:]]*$ || $line =~ ^[[:space:]]*# ]] && continue
  [[ $line =~ ^([A-Z][A-Z0-9_]*)=(.+)$ ]] || fail 'Use literal KEY=value lines without shell syntax'
  key=${BASH_REMATCH[1]}
  value=${BASH_REMATCH[2]}
  [[ ! -v seen[$key] ]] || fail "Duplicate binding: $key"
  case $key in
    WEBCRAWLER_CRAWLER_IMAGE|WEBCRAWLER_API_IMAGE|WEBCRAWLER_SCHEDULER_CONTROL_IMAGE|WEBCRAWLER_SCHEDULER_INGEST_IMAGE|WEBCRAWLER_SITEMAP_PATROLLER_IMAGE)
      [[ $value =~ ^([A-Za-z0-9._/:-]+@)?sha256:[a-f0-9]{64}$ ]] || fail "$key must be an immutable sha256 image ID or digest"
      ;;
    CRAWLER_API_DB_CREDENTIALS_FILE)
      [[ $value =~ ^/[A-Za-z0-9._/-]+$ ]] || fail 'Credential-file binding must be a literal absolute path'
      ;;
    *) fail "Binding is not permitted: $key" ;;
  esac
  seen[$key]=1
done < "$env_file"
for key in "${image_keys[@]}"; do
  [[ -v seen[$key] ]] || fail "Missing image binding: $key"
done

# Host shell values take precedence over --env-file in Compose. Clear every
# interpolation input used by this profile so the reviewed defaults and the
# explicit machine-binding file are authoritative. Never source the file.
unset "${image_keys[@]}" CRAWLER_API_DB_CREDENTIALS_FILE
unset API_BIND_ADDRESS API_PORT MANUAL_BULK_CRAWL_DATA_DIR MANUAL_BULK_CRAWL_SCHEDULING_POLICY
unset COMPOSE_FILE COMPOSE_PROJECT_NAME COMPOSE_PROFILES COMPOSE_ENV_FILES COMPOSE_DISABLE_ENV_FILE

operation=${1:-config}
if [[ $# -gt 0 ]]; then shift; fi
services=(crawler api scheduler_control scheduler_ingest sitemap_patroller)
validate_services() {
  local selected
  [[ $# -gt 0 ]] || fail 'Name at least one pipeline service'
  for selected in "$@"; do
    case $selected in
      crawler|api|scheduler_control|scheduler_ingest|sitemap_patroller) ;;
      *) fail "Service is not permitted: $selected" ;;
    esac
  done
}

case $operation in
  config)
    if [[ $# -eq 0 ]]; then args=(config)
    elif [[ $# -eq 1 && $1 == --services ]]; then args=(config --services)
    elif [[ $# -eq 2 && $1 == --format && $2 == json ]]; then args=(config --format json)
    else fail 'config accepts only --services or --format json'; fi
    ;;
  ps)
    if [[ $# -eq 0 ]]; then set -- "${services[@]}"; fi
    validate_services "$@"
    args=(ps -- "$@")
    ;;
  up|recreate)
    validate_services "$@"
    args=(up --detach --no-build --pull never --no-deps)
    if [[ $operation == recreate ]]; then args+=(--force-recreate); fi
    args+=(-- "$@")
    ;;
  stop)
    validate_services "$@"
    args=(stop -- "$@")
    ;;
  *) fail "Operation is not permitted: $operation" ;;
esac

exec docker compose --project-name webcrawler --project-directory "$repo_root" \
  --env-file "$env_file" -f "$repo_root/docker-compose.yml" \
  -f "$repo_root/ops/production/compose.yml" "${args[@]}"
