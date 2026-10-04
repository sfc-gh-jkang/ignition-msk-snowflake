#!/usr/bin/env bash
# Runs openflow-dcp/snowflake/dcp_teardown.sql in order, waiting for each TERMINATE to finish.
# TERMINATE is asynchronous and DROP fails with 513216 until the object reports TERMINATED,
# which is why the .sql file cannot simply be run with `snow sql -f`.
#   dcp_teardown.sh <connection>
set -euo pipefail
conn="${1:?usage: dcp_teardown.sh <connection>}"
rt=IGNITION_OPENFLOW.INFRA.IGNITION_SQLSERVER_MED_RT
dep=IGNITION_DCP_DEPLOYMENT

q() { snow sql -c "$conn" --role OPENFLOW_ADMIN -q "$1" >/dev/null; }
status() {  # status <SHOW statement> <name>; prints the status, or GONE
  snow sql -c "$conn" --role OPENFLOW_ADMIN --format json -q "$1" |
    python3 -c 'import json,sys; n=sys.argv[1]; r=[x["status"] for x in json.load(sys.stdin) if x["name"]==n]; print(r[0] if r else "GONE")' "$2"
}
wait_terminated() {
  for _ in $(seq 1 60); do
    s="$(status "$1" "$2")"; [ "$s" = TERMINATED ] || [ "$s" = GONE ] && return 0
    echo "  $2: $s" >&2; sleep 20
  done
  echo "timed out waiting for $2 to terminate" >&2; exit 1
}

echo "terminating runtime" >&2
s="$(status 'SHOW OPENFLOW RUNTIMES IN ACCOUNT' IGNITION_SQLSERVER_MED_RT)"
[ "$s" = GONE ] || {
  # Re-issuing TERMINATE while TERMINATING is rejected (513216), so only start it from a live state.
  case "$s" in TERMINATING|TERMINATED) ;; *) q "ALTER OPENFLOW RUNTIME IF EXISTS $rt TERMINATE CASCADE" ;; esac
  wait_terminated 'SHOW OPENFLOW RUNTIMES IN ACCOUNT' IGNITION_SQLSERVER_MED_RT
  q "DROP OPENFLOW RUNTIME IF EXISTS $rt"
}
echo "terminating deployment" >&2
s="$(status 'SHOW OPENFLOW DEPLOYMENTS' $dep)"
[ "$s" = GONE ] || {
  case "$s" in TERMINATING|TERMINATED) ;; *) q "ALTER OPENFLOW DEPLOYMENT $dep TERMINATE" ;; esac
  wait_terminated 'SHOW OPENFLOW DEPLOYMENTS' $dep
  q "DROP OPENFLOW DEPLOYMENT IF EXISTS $dep"
}
echo "dropping DCP, EAI, destination, warehouse, role" >&2
q "DROP DATA CONNECTIVITY PROXY IF EXISTS IGNITION_PLANT_DCP"
# OPENFLOW_ADMIN created these, so it can drop them; no ACCOUNTADMIN needed.
q "DROP INTEGRATION IF EXISTS PLANT_SQLSERVER_DCP_EAI"
q "DROP DATABASE IF EXISTS IGNITION_OPENFLOW"
q "DROP WAREHOUSE IF EXISTS SFE_IGNITION_OPENFLOW_WH"
q "DROP ROLE IF EXISTS OPENFLOW_IGNITION_RT_EXECUTE_AS_RL"
echo "remaining: deployment=$(status 'SHOW OPENFLOW DEPLOYMENTS' $dep) dcp=$(status 'SHOW DATA CONNECTIVITY PROXIES IN ACCOUNT' IGNITION_PLANT_DCP)"
