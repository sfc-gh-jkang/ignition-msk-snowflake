#!/usr/bin/env python3
"""Builds an Ignition 8.1 gateway backup (.gwbk) with the Snowpipe Streaming REST demo baked in.

8.1 keeps tags in the gateway's internal SQLite database and stores gateway event scripts as
binary, so neither can be committed as plain text. This script starts from a stock backup and
adds, using only the Python standard library:
  - the ScadaToSnowflakeRest project (plain-text script library) under projects/
  - simulated line tags whose value-change scripts call snowstream.enqueue()
  - a 5-second ticker tag whose value-change script calls snowstream.flush()
  - the project as the gateway scripting project, so tag scripts can import it

With PLANT_SQL_PASSWORD set (Option B), it also adds:
  - a "PlantSQL" database connection to sqlserver.plant.local (bundled MSSQL driver)
  - a 1-second tag whose script inserts one row into IGNITION.dbo.LINE1_TG through that
    connection, in the shape a SQL Bridge transaction group writes (ndx, t_stamp, one column per tag)

Usage: build_gwbk.py <stock.gwbk> <out.gwbk>
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

HERE = pathlib.Path(__file__).resolve().parent
PROJECT = "ScadaToSnowflakeRest"
DEFAULT_PROVIDER_ID = 0  # TAGPROVIDERSETTINGS_ID of the "default" provider in a stock gateway
PLANT_DB = "PlantSQL"

LINE_TAGS = [
    ("Temperature", "Float8", "180 + 5*sin(toMillis(now(1000))/60000.0) + (toMillis(now(1000))%7)/10.0"),
    ("Speed", "Float8", "120 + 10*cos(toMillis(now(1000))/45000.0)"),
    ("Running", "Boolean", "(toMillis(now(1000))/1000)%300 < 270"),
]


def tag(name, dtype, expr, script):
    return {"name": name, "tagType": "AtomicTag", "valueSource": "expr", "dataType": dtype,
            "expression": expr, "executionMode": "FixedRate", "executionRate": 1000,
            "eventScripts": [{"eventid": "valueChanged", "script": "\t" + script}]}


def add_plant_sql(con, password):
    """Adds the PlantSQL connection with the gateway's bundled Microsoft SQL Server driver."""
    driver, translator = con.execute(
        "SELECT JDBCDRIVERS_ID, DEFAULTTRANSLATORID FROM JDBCDRIVERS WHERE DBTYPE = 'MSSQL'").fetchone()
    ds_id = con.execute("SELECT COALESCE(MAX(DATASOURCES_ID), -1) + 1 FROM DATASOURCES").fetchone()[0]
    # The gateway decrypts PASSWORDE and ignores the plain PASSWORD column, so the password goes in
    # the connection's extra JDBC properties, which are handed straight to the driver.
    # DEMO ONLY: this leaves the password in plain text inside the generated .gwbk (which is
    # git-ignored under ignition81/build/). Ignition 8.1 has no secret providers; on a real gateway,
    # enter the password in Config > Databases > Connections so the gateway stores it encrypted.
    con.execute("INSERT INTO DATASOURCES (DATASOURCES_ID, NAME, DRIVERID, TRANSLATORID, CONNECTURL, USERNAME,"
                " CONNECTIONPROPS) VALUES (?,?,?,?,?,?,?)",
                (ds_id, PLANT_DB, driver, translator,
                 "jdbc:sqlserver://sqlserver.plant.local:1433;databaseName=IGNITION;encrypt=false",
                 "ignition", "password=%s" % password))
    con.execute("UPDATE SEQUENCES SET val = ? WHERE name = 'DATASOURCES_SEQ'", (ds_id,))


def add_tags(db_path):
    con = sqlite3.connect(db_path)
    rows = []
    sim, line = str(uuid.uuid4()), str(uuid.uuid4())
    rows.append((sim, None, {"name": "LineSim", "tagType": "Folder"}, "LineSim"))
    rows.append((line, sim, {"name": "Line1", "tagType": "Folder"}, "Line1"))
    for name, dtype, expr in LINE_TAGS:
        rows.append((str(uuid.uuid4()), line,
                     tag(name, dtype, expr, "snowstream.enqueue(tagPath, currentValue, initialChange)"), name))
    flush = tag("_Flush", "Int8", "toMillis(now(5000))", "snowstream.flush()")
    flush["executionRate"] = 5000
    rows.append((str(uuid.uuid4()), sim, flush, "_Flush"))
    plant_password = os.environ.get("PLANT_SQL_PASSWORD")
    if plant_password:
        add_plant_sql(con, plant_password)
        paths = ", ".join('"[default]LineSim/Line1/%s"' % n for n, _, _ in LINE_TAGS)
        insert = ('v = system.tag.readBlocking([%s]); system.db.runPrepUpdate('
                  '"INSERT INTO dbo.LINE1_TG (t_stamp, temperature, speed, running) '
                  'VALUES (SYSUTCDATETIME(), ?, ?, ?)", [x.value for x in v], "%s")' % (paths, PLANT_DB))
        rows.append((str(uuid.uuid4()), sim, tag("_TransactionGroup", "Int8", "toMillis(now(1000))", insert),
                     "_TransactionGroup"))
    for rank, (tid, folder, cfg, name) in enumerate(rows):
        con.execute("INSERT INTO TAGCONFIG (ID, PROVIDERID, FOLDERID, CFG, RANK, NAME) VALUES (?,?,?,?,?,?)",
                    (tid, DEFAULT_PROVIDER_ID, folder, json.dumps(cfg, indent=2), rank, name))
    con.execute("UPDATE SYSPROPS SET GATEWAYSCRIPTINGPROJECT = ?", (PROJECT,))
    con.commit()
    con.close()


def main(src, dst):
    work = pathlib.Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(src) as z:
            z.extractall(work)
        add_tags(work / "db_backup_sqlite.idb")
        target = work / "projects" / PROJECT
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(HERE / "project", target)
        # Rebuild from the stock archive's entry order, then add the project files. Directory
        # entries for paths the stock archive lacks would be restored as files and fault the gateway.
        with zipfile.ZipFile(src) as stock, zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
            names = [i.filename for i in stock.infolist()]
            for name in names:
                path = work / name
                if name.endswith("/"):
                    z.writestr(stock.getinfo(name), b"")
                else:
                    z.write(path, name)
            for p in sorted(target.rglob("*")):
                if p.is_file():
                    z.write(p, p.relative_to(work).as_posix())
        print(f"wrote {dst}")
    finally:
        shutil.rmtree(work)


if __name__ == "__main__":
    main(*sys.argv[1:3])
