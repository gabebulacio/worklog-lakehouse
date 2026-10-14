# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Land: synthetic source systems
# Stands in for the four source systems and writes what each API would return for this
# run into `Files/landing/<system>/<entity>/`. In production this notebook is replaced by
# Copy jobs / pipeline Copy activities landing the same envelopes; nothing downstream changes.
#
# | system | entity | shape | quirks it reproduces |
# |---|---|---|---|
# | `timesheet_app` | `timesheets` | flat rows | blank `UserEmail` for deactivated users, three `ModifiedOn` formats, client-name spelling variants, blank clients |
# | `timesheet_app` | `users` | snapshot | deactivated users carry an ` (INACTIVE)` suffix on the email |
# | `issue_tracker` | `worklogs` | nested JSON | author is an opaque key, legacy-domain or missing emails, page-overlap duplicates |
# | `issue_tracker` | `users` | snapshot | key -> email/name |
# | `directory` | `users` | snapshot (Entra ID shape) | department changes over time, leavers with `employeeLeaveDateTime` |
# | `crm` | `accounts`, `opportunities`, `deliverables`, `assignments` | snapshots | assignments identified by alternate emails or by name only |
#
# The world is deterministic (`SyntheticSeed`) and evolves with `run_date`: entries appear
# when they are created, change when edited and disappear when deleted, so incremental
# and full extracts behave like the real APIs, including their blind spot (a person-day
# whose entries were all deleted is invisible to an incremental "modified since" pull).
# Every person, client and email is fictional.

# PARAMETERS CELL ********************

run_id = "manual"
run_date = ""            # YYYY-MM-DD, the day the pipeline runs (default: today)
mode = "incremental"     # incremental: person-days changed since updated_since_hours; full: date window
date_from = ""           # full mode window (work dates)
date_to = ""
updated_since_hours = 48

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- shared bootstrap: source of truth is fabric/shared/bootstrap.py (python run.py sync) ---
# Same cell in every notebook so each one also runs on its own. Works on Fabric (abfss://
# OneLake paths + storage token) and locally (plain folders, see local/fabric_local.py).
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import duckdb
import notebookutils
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

VARLIB = "wl_settings"


def var(name, default=None):
    """A wl_settings variable; falls back to an environment variable, then the default."""
    try:
        value = notebookutils.variableLibrary.get(f"$(/**/{VARLIB}/{name})")
        if value not in (None, ""):
            return value
    except Exception:
        pass
    return os.getenv(name, default)


LAKEHOUSE = var("LakehouseName", "wl_lakehouse")
BASE = notebookutils.lakehouse.getWithProperties(LAKEHOUSE)["properties"]["abfsPath"]
ON_FABRIC = BASE.startswith("abfss://")
TABLES = f"{BASE}/Tables"
STORAGE = ({"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}
           if ON_FABRIC else None)
MOUNT = "/wl"
try:
    notebookutils.fs.mount(BASE, MOUNT)
except Exception:
    pass  # already mounted in this session
FILES = os.path.join(notebookutils.fs.getMountPath(MOUNT), "Files")


def table_exists(name):
    try:
        DeltaTable(f"{TABLES}/{name}", storage_options=STORAGE)
        return True
    except Exception:
        return False


def read_table(name):
    """Delta table as a pyarrow Table, or None if it does not exist yet."""
    try:
        return DeltaTable(f"{TABLES}/{name}", storage_options=STORAGE).to_pyarrow_table()
    except Exception as exc:
        if "not found" in str(exc).lower() or "no files in log" in str(exc).lower() or type(exc).__name__ == "TableNotFoundError":
            return None
        raise


def _to_arrow(data):
    """pandas / DuckDB relation / Arrow -> Arrow, with naive timestamps stamped as UTC."""
    if isinstance(data, pd.DataFrame):
        data = pa.Table.from_pandas(data, preserve_index=False)
    elif not isinstance(data, pa.Table):
        data = data.to_arrow_table() if hasattr(data, "to_arrow_table") else data.fetch_arrow_table()
    fields = []
    for f in data.schema:
        if pa.types.is_timestamp(f.type):
            f = f.with_type(pa.timestamp("us", tz="UTC"))
        elif pa.types.is_large_string(f.type):
            f = f.with_type(pa.string())
        fields.append(f)
    return data.cast(pa.schema(fields))


def write_table(data, name, mode="overwrite", attempts=4):
    """Write to a Delta table. Retries commit conflicts (concurrent writers); writes are idempotent."""
    arrow = _to_arrow(data)
    kwargs = {"mode": mode, "storage_options": STORAGE}
    kwargs["schema_mode"] = "merge" if mode == "append" else "overwrite"
    for attempt in range(1, attempts + 1):
        try:
            write_deltalake(f"{TABLES}/{name}", arrow, **kwargs)
            break
        except Exception as exc:
            conflict = "commit" in str(exc).lower() and "fail" in str(exc).lower()
            if not conflict or attempt == attempts:
                raise
            time.sleep(5 * attempt)
    print(f"{name}: {arrow.num_rows:,} rows ({mode})")
    return arrow.num_rows


db = duckdb.connect()


def register(**tables):
    """Expose Delta tables (by name) or in-memory frames to DuckDB SQL under the given aliases."""
    for alias, source in tables.items():
        if isinstance(source, str):
            arrow = read_table(source)
            if arrow is None:
                raise RuntimeError(f"table {source} does not exist yet - run the upstream notebook first")
            source = arrow
        db.register(alias, source)


def sql(query):
    """Run DuckDB SQL; returns a pyarrow Table."""
    rel = db.sql(query)
    return rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table()


def scalar(query):
    return db.sql(query).fetchone()[0]


def secret(name, default=""):
    """Credential from Key Vault (KeyVaultUri); environment variable locally."""
    vault = var("KeyVaultUri", "")
    if vault:
        return notebookutils.credentials.getSecret(vault, name)
    return os.getenv(name, default)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- the synthetic world: people, clients, CRM ----------------------------------------
import csv
import random
import uuid

SEED = int(var("SyntheticSeed", 42))
DOMAIN = var("DefaultDomain", "lumen.studio")
LEGACY = "lumenlabs.io"          # old domain still used by some logins
PARTNER = "northpeak-dev.com"    # external contractors
START = date(2026, 1, 1)
NS = uuid.UUID("5f0c8d2e-3b1a-4c6d-9e7f-102938475600")

FIRST = ["Ana", "Bruno", "Camila", "Diego", "Elena", "Facundo", "Gabriela", "Hernan", "Ines", "Joaquin",
         "Karina", "Lucas", "Martina", "Nicolas", "Olivia", "Pablo", "Renata", "Santiago", "Tamara", "Valentin",
         "Ximena", "Yamil", "Zoe", "Agustin", "Belen", "Cristian", "Daniela", "Emilio", "Florencia", "Gonzalo",
         "Julieta", "Leandro", "Mora", "Nahuel", "Paula", "Ramiro", "Sofia", "Tomas", "Victoria", "Lautaro"]
LAST = ["Acosta", "Benitez", "Castro", "Dominguez", "Escobar", "Ferreyra", "Gimenez", "Herrera", "Ibarra",
        "Juarez", "Ledesma", "Medina", "Navarro", "Ortiz", "Paz", "Quiroga", "Rios", "Sosa", "Toledo", "Vera",
        "Aguirre", "Bravo", "Cabrera", "Duarte", "Figueroa", "Godoy", "Luna", "Molina", "Peralta", "Romero"]
DEPTS = [("Engineering", 0.34, "Software Engineer"), ("Design", 0.14, "Product Designer"),
         ("Data", 0.14, "Data Engineer"), ("Strategy", 0.10, "Strategist"),
         ("Account Management", 0.16, "Account Manager"), ("Operations", 0.12, "Operations Analyst")]
COUNTRIES = [("Argentina", 0.55), ("Mexico", 0.2), ("Brazil", 0.15), ("Colombia", 0.1)]
ACCOUNTS = [("Contoso", "CON"), ("Fabrikam", "FAB"), ("Northwind Traders", "NWT"), ("Adventure Works", "ADW"),
            ("Tailspin Toys", "TST"), ("Wide World Importers", "WWI"), ("Woodgrove Bank", "WGB"),
            ("Litware", "LIT"), ("Proseware", "PRS"), ("Alpine Ski House", "ASH")]
INTERNAL = ("Internal", "INT")
TASKS = ["Sprint work", "Code review", "Client workshop", "Discovery", "Bug fixing", "Design review",
         "Data pipeline", "Reporting", "Planning", "Stakeholder sync", "QA", "Documentation"]


def pick(r, weighted):
    x, acc = r.random(), 0.0
    for item in weighted:
        acc += item[1]
        if x < acc:
            return item
    return weighted[-1]


def build_people():
    r = random.Random(SEED)
    people, seen = [], set()
    for i in range(120):
        first, last = r.choice(FIRST), r.choice(LAST)
        local = f"{first}.{last}".lower()
        n = 2
        while local in seen:
            local, n = f"{first}.{last}{n}".lower(), n + 1
        seen.add(local)
        dept, _, title = pick(r, DEPTS) if i else ("Operations", 1, "Managing Director")
        hire = (date(2019, 1, 1) + timedelta(days=r.randrange(2500))) if r.random() < 0.85 or i == 0 \
            else START + timedelta(days=r.randrange(300))
        hire = min(hire, date(2025, 12, 31)) if hire.year < 2026 else hire
        leave = None
        if i and r.random() < 0.08:
            leave = max(hire, START) + timedelta(days=30 + r.randrange(300))
        people.append({
            "i": i, "id": str(uuid.uuid5(NS, f"user-{i}")), "first": first, "last": last, "local": local,
            "mail": f"{local}@{DOMAIN}", "dept": dept, "title": title, "country": pick(r, COUNTRIES)[0],
            "hire": hire, "leave": leave, "employee_id": f"E{1000 + i}",
            "dir_has_employee_id": r.random() > 0.1, "upn_legacy": r.random() < 0.05,
            "exempt": dept == "Operations",
        })
    # department leads report to person 0; everyone else to their department lead
    leads = {}
    for p in people[1:]:
        leads.setdefault(p["dept"], p)
    for p in people:
        lead = leads.get(p["dept"])
        p["manager"] = None if p["i"] == 0 else (people[0] if lead is None or lead is p else lead)["id"]
        p["is_lead"] = lead is p
    # six department moves in early October (the history SCD2 captures)
    movers = [p for p in people if not p["is_lead"] and p["i"] and not p["leave"] and not p["exempt"]][:6]
    for k, p in enumerate(movers):
        new_dept = next(d for d, _, _ in DEPTS if d not in (p["dept"], "Operations"))
        if k % 2:
            new_dept = "Data" if p["dept"] != "Data" else "Engineering"
        p["move"] = (date(2026, 10, 2) + timedelta(days=k), new_dept, leads[new_dept]["id"])
    # how each person logs time
    for p in people:
        x = r.random()
        p["system"] = None if p["exempt"] else ("timesheet_app" if x < 0.55 else "issue_tracker" if x < 0.9 else "both")
        p["accounts"] = r.sample(range(len(ACCOUNTS)), k=1 + (r.random() < 0.6) + (r.random() < 0.2))
        p["ts_id"] = 5000 + p["i"]
        p["trk_key"] = f"TRK{10000 + 7 * p['i']}"
        p["ts_email_style"] = p["trk_email_style"] = "normal"
        p["stops_logging"] = None
    expected = [p for p in people if p["system"] and not p["leave"] and p["hire"] < date(2026, 6, 1)]
    for p in r.sample(expected, 5):
        p["stops_logging"] = date(2026, 8, 15)
    for p in r.sample([p for p in expected if not p["stops_logging"]], 2):
        p["stops_logging"] = START  # never logs
    # identity quirks on fixed people (not probabilities), so every scenario always exists
    active = [p for p in expected if not p["stops_logging"]]
    on_ts = [p for p in active if p["system"] in ("timesheet_app", "both")]
    on_trk = [p for p in active if p["system"] in ("issue_tracker", "both")]
    for k, p in enumerate(r.sample(on_ts, 7)):
        p["ts_email_style"] = "swapped" if k < 4 else "legacy"          # last.first@ / @legacy domain
    # and one swapped login that belongs to someone with a namesake: the pipeline must NOT guess it
    namesake = next((p for p in on_ts if p["local"][-1].isdigit() and p["ts_email_style"] == "normal"), None)
    if namesake:
        namesake["ts_email_style"] = "swapped"
    for k, p in enumerate(r.sample(on_trk, 8)):
        p["trk_email_style"] = "legacy_mapped" if k < 3 else "legacy" if k < 5 else "missing"
    # contractors: only in the issue tracker; two are in the mapping file, one is not
    for k, (first, last) in enumerate([("Iris", "Calloway"), ("Mateo", "Lindqvist"), ("Noor", "Haddad")]):
        local = f"{first}.{last}".lower()
        people.append({
            "i": 200 + k, "id": None, "first": first, "last": last, "local": local, "mail": f"{local}@{PARTNER}",
            "dept": None, "title": "Contractor", "country": "Uruguay", "hire": START, "leave": None,
            "employee_id": None, "dir_has_employee_id": False, "upn_legacy": False, "exempt": False,
            "manager": None, "is_lead": False, "system": "issue_tracker", "accounts": [k % len(ACCOUNTS)],
            "ts_id": None, "trk_key": f"TRK{20000 + k}", "ts_email_style": "normal",
            "trk_email_style": "normal", "stops_logging": None, "contractor": True, "in_mapping": k < 2,
        })
    return people


PEOPLE = build_people()


def build_crm():
    r = random.Random(SEED + 1)
    accounts = [{"accountid": str(uuid.uuid5(NS, f"acc-{a}")), "name": name, "tracker_project_key": key,
                 "owner": f"{PEOPLE[1 + a]['first']} {PEOPLE[1 + a]['last']}",
                 "statecode": "Active"} for a, (name, key) in enumerate(ACCOUNTS)]
    opportunities, deliverables, assignments = [], [], []
    for a, acc in enumerate(accounts):
        opp = {"opportunityid": str(uuid.uuid5(NS, f"opp-{a}")), "accountid": acc["accountid"],
               "name": f"{acc['name']} 2026 retainer", "stage": "Won", "close_date": "2025-12-15"}
        opportunities.append(opp)
        staff = [p for p in PEOPLE if a in p["accounts"] and p["system"] and not p.get("contractor")]
        for k, p in enumerate(staff):
            if r.random() < 0.15:
                continue  # logs on the account without a sold position (logging_unassigned)
            start = max(START, p["hire"])
            end = p["leave"] or date(2026, 12, 31)
            d_id = str(uuid.uuid5(NS, f"del-{a}-{k}"))
            deliverables.append({"deliverableid": d_id, "opportunityid": opp["opportunityid"],
                                 "role": p["title"], "start_date": str(start), "end_date": str(end),
                                 "monthly_hours": 160, "rate": round(35 + r.random() * 40, 2), "approved": True})
            style = r.random()
            assignments.append({
                "assignmentid": str(uuid.uuid5(NS, f"asg-{a}-{k}")), "deliverableid": d_id,
                "resource_email": "" if style < 0.05 else f"{p['local']}@{LEGACY}" if style < 0.15 else p["mail"],
                "resource_name": f"{p['first']} {p['last']}", "start_date": str(start), "end_date": str(end),
                "allocation": round(1 / len(p["accounts"]), 2),
            })
    return accounts, opportunities, deliverables, assignments


CRM = build_crm()
print(f"world: {len(PEOPLE)} people, {len(CRM[0])} accounts, {len(CRM[3])} assignments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- worklog entries with their full edit/delete history -------------------------------
run_day = date.fromisoformat(run_date) if run_date else date.today()
RUN_TS = datetime(run_day.year, run_day.month, run_day.day, 6, 0, tzinfo=timezone.utc)
CUTOFF = RUN_TS - timedelta(hours=int(updated_since_hours))


def at(day, rr, lo=8, hi=20):
    return datetime(day.year, day.month, day.day, rr.randrange(lo, hi), rr.randrange(60), tzinfo=timezone.utc)


def person_day_entries(p, d):
    """Entries one person logged for one work date. Seeded per (person, date): independent of run_date."""
    rr = random.Random(f"{SEED}-{p['i']}-{d.toordinal()}")
    if d.weekday() >= 5 or rr.random() < 0.07:
        return []
    system = p["system"] if p["system"] != "both" else rr.choice(["timesheet_app", "issue_tracker"])
    n = rr.choice([1, 2, 2, 3])
    total = rr.choice([8, 8, 8, 7.5, 8.5, 6, 9])
    cuts = sorted(rr.choice([1, 1.5, 2, 2.5, 3, 4]) for _ in range(n - 1))
    hours = [round(b - a, 2) for a, b in zip([0, *cuts], [*cuts, total], strict=True) if b - a > 0]
    lag = rr.choices([0, 1, 2, 4, 6, 12, 25], weights=[58, 15, 10, 6, 4, 4, 3])[0]
    created_day = d + timedelta(days=lag)
    day_deleted = rr.random() < 0.003
    out = []
    for k, h in enumerate(hours):
        x = rr.random()
        if x < 0.8:
            acc = rr.choice(p["accounts"])
            client, key = ACCOUNTS[acc]
        elif x < 0.97:
            client, key = INTERNAL
        else:
            client, key = ACCOUNTS[rr.randrange(len(ACCOUNTS))]
        created = at(created_day, rr)
        versions = [(created, h)]
        if rr.random() < 0.04:
            versions.append((created + timedelta(days=rr.randint(1, 12), hours=rr.randint(0, 5)),
                             max(0.25, h + rr.choice([-1, -0.5, 0.5, 1]))))
        deleted = None
        if day_deleted:
            deleted = created + timedelta(days=rr.randint(1, 5))
        elif rr.random() < 0.01:
            deleted = created + timedelta(days=rr.randint(1, 6))
        out.append({
            "id": f"{p['i']}-{d.toordinal()}-{k}", "num": (p["i"] * 1_000_003 + d.toordinal() * 7 + k) % 900_000_000,
            "person": p, "system": system, "date": d, "client": client, "key": key,
            "blank_client": system == "timesheet_app" and rr.random() < 0.004,
            "client_variant": rr.random(), "task": rr.choice(TASKS), "issue_no": rr.randint(1, 400),
            "versions": versions, "deleted": deleted, "fmt": rr.random(),
        })
    return out


def visible(e, ts):
    """Current state of an entry as the source shows it at ts, or None."""
    if e["versions"][0][0] > ts or (e["deleted"] and e["deleted"] <= ts):
        return None
    mod, hours = max(v for v in e["versions"] if v[0] <= ts)
    return mod, hours


def changed_since(e, since, ts):
    stamps = [v[0] for v in e["versions"]] + ([e["deleted"]] if e["deleted"] else [])
    return any(since < s <= ts for s in stamps)


entries = []
for p in PEOPLE:
    if not p["system"]:
        continue
    first_day = max(START, p["hire"])
    last_day = min(run_day, (p["leave"] - timedelta(days=1)) if p["leave"] else run_day,
                   (p["stops_logging"] - timedelta(days=1)) if p["stops_logging"] else run_day)
    d = first_day
    while d <= last_day:
        entries.extend(person_day_entries(p, d))
        d += timedelta(days=1)

if mode == "full":
    w_to = date.fromisoformat(date_to) if date_to else run_day
    if date_from:
        w_from = date.fromisoformat(date_from)
    else:
        w_from = run_day.replace(day=1)
        for _ in range(int(var("FullWindowMonths", 3))):
            w_from = (w_from - timedelta(days=1)).replace(day=1)
    selected = [e for e in entries if w_from <= e["date"] <= w_to]
else:
    w_from = w_to = None
    by_pd = {}
    for e in entries:
        by_pd.setdefault((e["person"]["i"], e["system"], e["date"]), []).append(e)
    selected = [e for group in by_pd.values() if any(changed_since(x, CUTOFF, RUN_TS) for x in group) for e in group]
current = [(e, s) for e in selected if (s := visible(e, RUN_TS))]
print(f"run_date={run_day} mode={mode} entries in world={len(entries):,} extracted={len(current):,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- render each API's payload and land it ------------------------------------------------
def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def ts_modified(e, mod):
    if e["fmt"] < 0.7:
        return iso(mod)
    if e["fmt"] < 0.9:
        return mod.strftime("%m/%d/%Y %H:%M:%S")
    return mod.strftime("%Y-%m-%d %H:%M:%S.") + f"{mod.microsecond // 1000:03d}"


def gone(p):
    return p["leave"] is not None and p["leave"] <= run_day


def ts_email(p):
    if gone(p):
        return ""
    if p["ts_email_style"] == "swapped":
        return f"{p['last']}.{p['first']}@{DOMAIN}".lower()
    if p["ts_email_style"] == "legacy":
        return f"{p['local']}@{LEGACY}"
    return p["mail"]


def dept_at(p):
    if p.get("move") and p["move"][0] <= run_day:
        return p["move"][1], p["move"][2]
    return p["dept"], p["manager"]


timesheets, worklogs = [], []
for e, (mod, hours) in current:
    p = e["person"]
    if e["system"] == "timesheet_app":
        client = "" if e["blank_client"] else e["client"]
        if client and e["client_variant"] < 0.05:
            client = client.lower()
        elif client and e["client_variant"] < 0.08:
            client = client + " "
        timesheets.append({"Id": f"TS-{e['id']}", "UserId": p["ts_id"], "User": f"{p['first']} {p['last']}",
                           "UserEmail": ts_email(p), "Client": client, "Job": f"{e['client']} - {e['task']}",
                           "Day": f"{e['date']}T00:00:00", "Hours": hours, "ModifiedOn": ts_modified(e, mod)})
    else:
        worklogs.append({"worklogId": e["num"],
                         "issue": {"key": f"{e['key']}-{e['issue_no']}", "projectKey": e["key"],
                                   "summary": f"{e['task']} ({e['client']})"},
                         "author": {"accountKey": p["trk_key"]}, "startDate": str(e["date"]),
                         "timeSpentSeconds": int(round(hours * 3600)), "description": e["task"],
                         "dateCreated": iso(e["versions"][0][0]), "dateUpdated": iso(mod)})
if mode == "full":  # page-overlap duplicates the API returns on long pulls
    worklogs += [w for w in worklogs if w["worklogId"] % 503 == 0]

on_payroll = [p for p in PEOPLE if not p.get("contractor") and p["hire"] <= run_day]
ts_users = [{"UserId": p["ts_id"], "Name": f"{p['first']} {p['last']}",
             "Email": (f"{p['mail']} (INACTIVE)" if gone(p) else ts_email(p))}
            for p in on_payroll if p["system"] in ("timesheet_app", "both")]


def trk_email(p):
    style = p["trk_email_style"]
    return "" if style == "missing" else f"{p['local']}@{LEGACY}" if style.startswith("legacy") else p["mail"]


trk_users = [{"key": p["trk_key"], "emailAddress": trk_email(p), "displayName": f"{p['first']} {p['last']}",
              "active": not gone(p)}
             for p in PEOPLE if p["system"] in ("issue_tracker", "both") and p["hire"] <= run_day
             and p["i"] != 202]  # one contractor's account was deleted from the tracker: unresolvable key
directory = []
for p in on_payroll:
    dept, manager = dept_at(p)
    directory.append({
        "id": p["id"], "displayName": f"{p['first']} {p['last']}", "givenName": p["first"], "surname": p["last"],
        "mail": p["mail"], "userPrincipalName": f"{p['local']}@{LEGACY if p['upn_legacy'] else DOMAIN}",
        "department": dept, "jobTitle": p["title"], "country": p["country"], "accountEnabled": not gone(p),
        "employeeId": p["employee_id"] if p["dir_has_employee_id"] else None,
        "employeeHireDate": f"{p['hire']}T00:00:00Z",
        "employeeLeaveDateTime": f"{p['leave']}T00:00:00Z" if gone(p) else None,
        "manager": {"id": manager} if manager else None,
    })


def land(system, entity, records, extract_type):
    folder = os.path.join(FILES, "landing", system, entity)
    os.makedirs(folder, exist_ok=True)
    name = f"{RUN_TS:%Y%m%dT%H%M%S}_{extract_type}_{run_id}.json"
    envelope = {"system": system, "entity": entity, "extract_type": extract_type, "extracted_at": iso(RUN_TS),
                "window_from": str(w_from) if w_from else None, "window_to": str(w_to) if w_to else None,
                "updated_since": iso(CUTOFF) if extract_type == "incremental" else None, "records": records}
    with open(os.path.join(folder, name), "w", encoding="utf-8") as fh:
        json.dump(envelope, fh, ensure_ascii=False)
    return len(records)


accounts, opportunities, deliverables, assignments = CRM
landed = {
    "timesheet_app/timesheets": land("timesheet_app", "timesheets", timesheets, mode),
    "timesheet_app/users": land("timesheet_app", "users", ts_users, "snapshot"),
    "issue_tracker/worklogs": land("issue_tracker", "worklogs", worklogs, mode),
    "issue_tracker/users": land("issue_tracker", "users", trk_users, "snapshot"),
    "directory/users": land("directory", "users", directory, "snapshot"),
    "crm/accounts": land("crm", "accounts", accounts, "snapshot"),
    "crm/opportunities": land("crm", "opportunities", opportunities, "snapshot"),
    "crm/deliverables": land("crm", "deliverables", deliverables, "snapshot"),
    "crm/assignments": land("crm", "assignments", assignments, "snapshot"),
}

# The mapping file is maintained by people, not by the pipeline: write a starting copy once.
mapping_path = os.path.join(FILES, "config", "user_mapping.csv")
if not os.path.exists(mapping_path):
    os.makedirs(os.path.dirname(mapping_path), exist_ok=True)
    rows = []
    for p in PEOPLE:
        row = {"email": p["mail"], "tracker_key": "", "alt_emails": "", "is_timesheet_exempt": int(p["exempt"]),
               "employee_id": "", "notes": ""}
        if p["trk_email_style"] == "legacy_mapped":
            row["alt_emails"], row["notes"] = f"{p['local']}@{LEGACY}", "tracker login on the legacy domain"
        if p["trk_email_style"] == "missing" and p["system"] in ("issue_tracker", "both"):
            row["tracker_key"], row["notes"] = p["trk_key"], "tracker account has no email"
        if not p["dir_has_employee_id"] and p["employee_id"]:
            row["employee_id"] = p["employee_id"]
        if p.get("contractor"):
            if not p["in_mapping"]:
                continue
            row.update(tracker_key=p["trk_key"], notes="contractor (not in the directory)")
        if row["is_timesheet_exempt"] or any(row[k] for k in ("tracker_key", "alt_emails", "employee_id")):
            rows.append(row)
    with open(mapping_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote starting {mapping_path} ({len(rows)} rows)")

print(landed)
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "mode": mode, "landed": landed}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
