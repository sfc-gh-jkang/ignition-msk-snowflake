#!/usr/bin/env python3
"""Builds an Ignition 8.1 gateway backup for path 0: batched JDBC writes to Snowflake.

TEST HARNESS ONLY. It builds a fresh trial gateway for measuring credits; never restore its
output onto a production gateway, because restoring a backup replaces that gateway's whole config.
On a real gateway, add the buffer, flush timer and connection by hand (docs/ignition81-batch.md).

The gateway writes one row a second, the way a SQL Bridge transaction group does, through the
store-and-forward engine of a Snowflake database connection. The engine's forward settings decide
how often rows reach Snowflake, and so how often the warehouse wakes. Starts from a stock backup
and adds, using only the Python standard library:
  - the Snowflake JDBC driver (jar copied into user-lib/jdbc)
  - a "SnowflakeBatch" connection using key-pair auth (the key file is mounted, not embedded)
  - a store-and-forward engine for that connection: forward every FORWARD_SECONDS or every
    FORWARD_ROWS records, whichever comes first
  - a 1-second tag whose script calls system.db.runSFPrepUpdate into that connection

Usage: build_batch_gwbk.py <stock.gwbk> <out.gwbk> <snowflake-jdbc.jar>
Env:   SNOWFLAKE_ACCOUNT_HOST, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_BATCH_WAREHOUSE,
       FORWARD_SECONDS (default 900), FORWARD_ROWS (default 1000), MODE (sf | multirow | group), BATCH_TABLE, GROUP_MINUTES
"""
import json
import os
import pathlib
import shutil
import sqlite3
import sys
import tempfile
import uuid
import zipfile

DB = "SnowflakeBatch"
KEY_IN_CONTAINER = "/run/secrets/snowflake_key.p8"
TABLE = os.environ.get("BATCH_TABLE", "SNOWFLAKE_EXAMPLE.IGNITION_KAFKA_DEMO.LINE1_TG_BATCH")


def add_connection(con, jar_name):
    driver_id = con.execute("SELECT MAX(JDBCDRIVERS_ID) + 1 FROM JDBCDRIVERS").fetchone()[0]
    # The translator only matters for DDL the gateway generates (historian, alarm journal); plain
    # inserts from scripts and transaction groups pass through as written.
    translator = con.execute("SELECT DBTRANSLATORS_ID FROM DBTRANSLATORS WHERE NAME = 'POSTGRES'").fetchone()[0]
    con.execute("INSERT INTO JDBCDRIVERS (JDBCDRIVERS_ID, NAME, DESCRIPTION, DBTYPE, DEFAULTTRANSLATORID,"
                " CLASSNAME, JARFILE, URLFORMAT, DEFAULTVALIDATIONQUERY) VALUES (?,?,?,?,?,?,?,?,?)",
                (driver_id, "Snowflake", "Snowflake JDBC driver", "GENERIC", translator,
                 "net.snowflake.client.jdbc.SnowflakeDriver", jar_name,
                 "jdbc:snowflake://<account>.snowflakecomputing.com/", "SELECT 1"))
    con.execute("UPDATE SEQUENCES SET val = ? WHERE name = 'JDBCDRIVERS_SEQ'", (driver_id,))

    ds_id = con.execute("SELECT COALESCE(MAX(DATASOURCES_ID), -1) + 1 FROM DATASOURCES").fetchone()[0]
    url = "jdbc:snowflake://%s/" % os.environ["SNOWFLAKE_ACCOUNT_HOST"]
    props = ";".join([
        "authenticator=SNOWFLAKE_JWT",
        "private_key_file=" + KEY_IN_CONTAINER,
        "role=" + os.environ["SNOWFLAKE_ROLE"],
        "warehouse=" + os.environ["SNOWFLAKE_BATCH_WAREHOUSE"],
        "JDBC_QUERY_RESULT_FORMAT=JSON",
        "db=" + TABLE.split(".")[0],
        "schema=" + TABLE.split(".")[1],
    ])
    # SELECT 1 on borrow would wake the warehouse on every write; Snowflake answers it from the
    # cloud services layer, so it does not, but keep the pool small and idle connections evicted.
    con.execute("INSERT INTO DATASOURCES (DATASOURCES_ID, NAME, DRIVERID, TRANSLATORID, CONNECTURL, USERNAME,"
                " CONNECTIONPROPS, POOLMAXACTIVE, POOLMAXIDLE) VALUES (?,?,?,?,?,?,?,?,?)",
                (ds_id, DB, driver_id, translator, url, os.environ["SNOWFLAKE_USER"], props, 2, 1))
    con.execute("UPDATE SEQUENCES SET val = ? WHERE name = 'DATASOURCES_SEQ'", (ds_id,))

    seconds = int(os.environ.get("FORWARD_SECONDS", "900"))
    rows = int(os.environ.get("FORWARD_ROWS", "1000"))
    sf_id = con.execute("SELECT COALESCE(MAX(STOREANDFORWARDSYSSETTINGS_ID), -1) + 1"
                        " FROM STOREANDFORWARDSYSSETTINGS").fetchone()[0]
    # Store-and-forward engines are matched to connections by name. The memory buffer must hold a
    # full forward interval, or records spill to the local disk cache first.
    con.execute("INSERT INTO STOREANDFORWARDSYSSETTINGS (STOREANDFORWARDSYSSETTINGS_ID, NAME, BUFFERSIZE,"
                " ENABLEDISKSTORE, STOREMAXRECORDS, STOREWRITESIZE, STOREWRITETIME, FORWARDFROMSTORE,"
                " FORWARDWRITESIZE, FORWARDWRITETIME) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (sf_id, DB, rows * 2, 1, 100000, 25, 5000, 0, rows, seconds * 1000)
                if os.environ.get("MODE", "sf") != "group" else
                # group mode: the engine the gateway creates with a new connection, at its defaults
                (sf_id, DB, 250, 1, 25000, 25, 5000, 0, 25, 1000))
    con.execute("UPDATE SEQUENCES SET val = ? WHERE name = 'STOREANDFORWARDSYSSETTINGS_SEQ'", (sf_id,))


def timer_tag(name, rate_ms, script):
    return {"name": name, "tagType": "AtomicTag", "valueSource": "expr", "dataType": "Int8",
            "expression": "toMillis(now(%d))" % rate_ms, "executionMode": "FixedRate", "executionRate": rate_ms,
            "eventScripts": [{"eventid": "valueChanged", "script": script}]}


# MODE=sf (default): one INSERT per reading through the connection's store-and-forward engine,
# which is how SQL Bridge transaction groups write. MODE=multirow: readings are buffered in gateway
# memory and written every FORWARD_SECONDS as ONE multi-row INSERT, then the buffer is cleared only
# after the insert returns.
SF_SCRIPT = ('\tif not initialChange:\n'
             '\t\tsystem.db.runSFPrepUpdate("INSERT INTO %s (T_STAMP, TEMPERATURE, SPEED, RUNNING)'
             ' VALUES (?, ?, ?, ?)", [system.date.now(), 180 + (currentValue.value %% 7) / 10.0, 120.0, True],'
             ' ["%s"])')
BUFFER_SCRIPT = ('\tif not initialChange:\n'
                 '\t\tg = system.util.getGlobals()\n'
                 '\t\tg.setdefault("batch", []).append([system.date.now(), 180 + (currentValue.value % 7) / 10.0,'
                 ' 120.0, True])\n')
FLUSH_SCRIPT = ('\tif not initialChange:\n'
                '\t\tg = system.util.getGlobals()\n'
                '\t\trows = list(g.get("batch", []))\n'
                '\t\tif rows:\n'
                '\t\t\tsql = "INSERT INTO %s (T_STAMP, TEMPERATURE, SPEED, RUNNING) VALUES "'
                ' + ",".join(["(?, ?, ?, ?)"] * len(rows))\n'
                '\t\t\tsystem.db.runPrepUpdate(sql, [v for r in rows for v in r], "%s")\n'
                '\t\t\tdel g["batch"][:len(rows)]\n'
                '\t\t\tsystem.util.getLogger("batch").info("flushed %%d rows" %% len(rows))\n')


# MODE=group: a real SQL Bridge historical transaction group, created at gateway start through the
# module's own config classes and serializer (8.1 stores groups as gzipped XML that only the
# Designer normally writes). It records [default]LineSim/Line1/Speed, which changes every second,
# into TABLE every GROUP_MINUTES minutes. The connection's store-and-forward stays at the defaults.
GROUP_PROJECT = "TxgTest"
CREATE_GROUP_SCRIPT = """\timport os
\tfrom java.lang import String, Double, Boolean
\tfrom java.lang.reflect import Array
\tfrom com.inductiveautomation.ignition.gateway import IgnitionGateway
\tfrom com.inductiveautomation.ignition.common.util import TimeUnits
\tfrom com.inductiveautomation.ignition.common.sqltags.model.types import DataType
\tfrom com.inductiveautomation.ignition.common.sqltags.model import TagProp
\tlog = system.util.getLogger("txg")
\td = "/usr/local/bin/ignition/data/projects/%(project)s/com.inductiveautomation.sqlbridge/transaction-groups/Line1TG"
\tif os.path.exists(d + "/data.bin"):
\t\treturn
\tgw = IgnitionGateway.get()
\tmm = gw.getModuleManager()
\tP = "com.inductiveautomation.factorysql.common."
\tdef cls(n): return mm.resolveClass(P + n)
\ttry:
\t\tcls("config.GroupConfig")
\texcept:
\t\tlog.info("SQL Bridge classes not resolvable yet")
\t\treturn
\tdef const(n, f): return cls(n).getField(f).get(None)
\tG = lambda f: const("config.CommonGroupProperties", f)
\tI = lambda f: const("config.CommonItemProperties", f)
\titemCls = cls("config.ItemConfig")
\titem = itemCls.getConstructor([String, String]).newInstance(["grouptag_sqltref", "SPEED"])
\tfor k, v in [("DRIVING_TAG_PATH", "[default]LineSim/Line1/Speed"), ("VALUE_PROPERTY", TagProp.Value),
\t             ("TARGET_TYPE", const("types.ItemTargetTypes", "DB_FIELD")), ("TARGET_NAME", "SPEED"),
\t             ("TARGET_DATA_TYPE", DataType.Float8)]:
\t\titem.setPropertyValue(I(k), v)
\titems = Array.newInstance(itemCls, 1)
\tArray.set(items, 0, item)
\tg = cls("config.GroupConfig").getConstructor([String, String]).newInstance(["historical", "Line1TG"])
\tfor k, v in [("EXECUTION_ENABLED", Boolean(True)), ("UPDATE_RATE", Double(%(minutes)s.0)), ("UPDATE_UNITS", TimeUnits.valueOf("MIN")),
\t             ("EXECUTION_SCHEDULE_MODE", const("types.ExecutionScheduleMode", "RATE")), ("DATA_SOURCE", "%(db)s"),
\t             ("TABLE_NAME", "%(table)s"), ("AUTO_CREATE_TABLE", Boolean(False)), ("STORE_TIMESTAMP", Boolean(True)),
\t             ("TIMESTAMP_COLUMN", "T_STAMP"), ("INDEX_COLUMN", "%(table)s_NDX"),
\t             ("REC_MODE", const("types.RecordMode", "INSERT_ALL")), ("CONFIGURED_ITEMS", items)]:
\t\tg.setPropertyValue(G(k), v)
\tfrom com.inductiveautomation.ignition.common.xmlserialization.serialization import XMLSerializer
\tser = XMLSerializer().initDefaults()
\tser.addObject(g)
\tdata = ser.serializeXMLAndGZip()
\tif not os.path.isdir(d):
\t\tos.makedirs(d)
\tf = open(d + "/data.bin", "wb"); f.write(data.tostring()); f.close()
\tf = open(d + "/resource.json", "w"); f.write('{"scope":"G","version":1,"restricted":false,"overridable":true,"files":["data.bin"],"attributes":{}}'); f.close()
\tsystem.project.requestScan()
\tlog.info("created transaction group Line1TG, %(minutes)s min, %%d bytes" %% len(data))
"""


def add_tags(con):
    sim, line = str(uuid.uuid4()), str(uuid.uuid4())
    tags = [(sim, None, {"name": "LineSim", "tagType": "Folder"}, "LineSim"),
            (line, sim, {"name": "Line1", "tagType": "Folder"}, "Line1")]
    mode = os.environ.get("MODE", "sf")
    if mode == "group":
        speed = {"name": "Speed", "tagType": "AtomicTag", "valueSource": "expr", "dataType": "Float8",
                 "expression": "120 + 10*cos(toMillis(now(1000))/45000.0)", "executionMode": "FixedRate",
                 "executionRate": 1000}
        tags.append((str(uuid.uuid4()), line, speed, "Speed"))
        script = CREATE_GROUP_SCRIPT % {"project": GROUP_PROJECT, "db": DB, "table": TABLE.split(".")[-1],
                                         "minutes": int(os.environ.get("GROUP_MINUTES", "15"))}
        tags.append((str(uuid.uuid4()), sim, timer_tag("_CreateGroup", 30000, script), "_CreateGroup"))
    elif mode == "multirow":
        seconds = int(os.environ.get("FORWARD_SECONDS", "900"))
        tags.append((str(uuid.uuid4()), line, timer_tag("_Reading", 1000, BUFFER_SCRIPT), "_Reading"))
        tags.append((str(uuid.uuid4()), line, timer_tag("_Flush", seconds * 1000, FLUSH_SCRIPT % (TABLE, DB)),
                     "_Flush"))
    else:
        tags.append((str(uuid.uuid4()), line, timer_tag("_TransactionGroup", 1000, SF_SCRIPT % (TABLE, DB)),
                     "_TransactionGroup"))
    for rank, (tid, folder, cfg, name) in enumerate(tags):
        con.execute("INSERT INTO TAGCONFIG (ID, PROVIDERID, FOLDERID, CFG, RANK, NAME) VALUES (?,?,?,?,?,?)",
                    (tid, 0, folder, json.dumps(cfg, indent=2), rank, name))


def main(src, dst, jar):
    work = pathlib.Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(src) as z:
            z.extractall(work)
        jar = pathlib.Path(jar)
        con = sqlite3.connect(work / "db_backup_sqlite.idb")
        add_connection(con, jar.name)
        add_tags(con)
        con.commit()
        con.close()
        extra = work / "user-lib" / "jdbc" / jar.name
        shutil.copy(jar, extra)
        with zipfile.ZipFile(src) as stock, zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
            for info in stock.infolist():
                if info.filename.endswith("/"):
                    z.writestr(info, b"")
                else:
                    z.write(work / info.filename, info.filename)
            z.write(extra, extra.relative_to(work).as_posix())
            if os.environ.get("MODE", "sf") == "group":
                z.writestr("projects/%s/project.json" % GROUP_PROJECT, json.dumps(
                    {"title": GROUP_PROJECT, "description": "SQL Bridge transaction group test", "parent": "",
                     "enabled": True, "inheritable": False}))
        print(f"wrote {dst}")
    finally:
        shutil.rmtree(work)


if __name__ == "__main__":
    main(*sys.argv[1:4])
