#!/usr/bin/env bash
# Produces ignition81/build/demo.gwbk (or $GWBK_NAME): a stock Ignition 8.1 gateway backup with the Snowpipe
# Streaming REST project, simulated tags and tag scripts added by build_gwbk.py.
# A stock backup is taken from a throwaway container with gwcmd, so nothing binary is committed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
version="${IGNITION81_VERSION:-8.1.42}"
out="$here/build"; mkdir -p "$out"
name="ign81-base-$$"

docker run -d --name "$name" -e ACCEPT_IGNITION_EULA=Y -e GATEWAY_ADMIN_USERNAME=admin \
  -e GATEWAY_ADMIN_PASSWORD="$(openssl rand -hex 12)" -e IGNITION_EDITION=standard \
  "inductiveautomation/ignition:$version" >/dev/null
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 90); do
  docker exec "$name" curl -fs localhost:8088/StatusPing 2>/dev/null | grep -q RUNNING && break
  sleep 3
done
docker exec "$name" /usr/local/bin/ignition/gwcmd.sh -b /tmp/base.gwbk -y >/dev/null
docker cp "$name:/tmp/base.gwbk" "$out/base.gwbk"
python3 "$here/build_gwbk.py" "$out/base.gwbk" "$out/${GWBK_NAME:-demo.gwbk}"
