"""Check every DAX measure against the same number computed independently in DuckDB.

  python local/build_model.py --import        # local Import copy of the model over Parquet exports
  pbi connect -d localhost:<port>             # a blank Power BI Desktop with that copy imported + refreshed
  python local/verify_model.py                # DAX (through pbi-cli) vs DuckDB, by month, by account and in total

Exits 1 on any difference larger than a rounding error.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PARQUET = ROOT / ".lake" / "model_import" / "parquet"


def dax(query: str) -> list[dict]:
    with tempfile.NamedTemporaryFile("w", suffix=".dax", delete=False, encoding="utf-8") as fh:
        fh.write(query)
    out = subprocess.run(["pbi", "--json", "dax", "execute", "--file", fh.name], capture_output=True, text=True, check=True)
    rows = json.loads(out.stdout)["rows"]
    return [{k.split("[")[-1].rstrip("]"): v for k, v in r.items()} for r in rows]


def day(v):
    """DAX returns dates as '1/1/2026 12:00:00 AM'."""
    m, d, y = v.split()[0].split("/")
    return f"{y}-{int(m):02d}-{int(d):02d}"


def num(v):
    return None if v in (None, "") else float(v)


db = duckdb.connect()
for f in PARQUET.glob("*.parquet"):
    db.execute(f"create view {f.stem} as select * from read_parquet('{f.as_posix()}')")


def duck(query):
    cur = db.execute(query)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


failures, checked = [], 0


def compare(label, key, got, want, tol=1e-6):
    global checked
    checked += 1
    if got is None and want in (None, 0):
        return
    if got is None or want is None or abs(got - want) > tol * max(1.0, abs(want)):
        failures.append(f"{label} [{key}]: DAX={got} DuckDB={want}")


# ---- by month -------------------------------------------------------------------------------
MONTH_MEASURES = ["Hours Logged", "Billable Hours", "Capacity Hours", "Utilization %", "Expected People", "Logging People",
                  "Coverage %", "Unattributed Hours", "Logged Within 7 Days %", "Hours Logged PM", "Hours Logged YTD",
                  "Assigned Hours", "People Not Logging", "Utilization % PM"]
cols = ", ".join(f'"{m}", [{m}]' for m in MONTH_MEASURES)
got = {day(r["Month"]): r for r in dax(f"EVALUATE SUMMARIZECOLUMNS ( 'Date'[Month], {cols} )")}
want = {str(r["month"]): r for r in duck("""
    with w as (select date_trunc('month', date)::date as month, * from gold_fact_worklog),
    c as (select date_trunc('month', date)::date as month, * from gold_fact_capacity),
    hours as (select month, sum(hours) as hl, sum(billable_hours) as bh,
                     sum(hours) filter (where not matched) as uh,
                     count(*) filter (where delay_days <= 7) / count(*) as w7 from w group by month),
    cap as (select month, sum(capacity_hours) as ch, count(distinct user_key) as ep from c group by month),
    logging as (select c.month, count(distinct c.user_key) as lp from c
                where exists (select 1 from w where w.month = c.month and w.user_key = c.user_key) group by c.month),
    asg as (select month, sum(assigned_hours) as ah from gold_fact_assignment_month group by month)
    select m.month, h.hl, h.bh, cap.ch, h.bh / cap.ch as util, cap.ep, l.lp, l.lp / cap.ep as cov, h.uh, h.w7,
           lag(h.hl) over (order by m.month) as pm,
           sum(h.hl) over (partition by year(m.month) order by m.month) as ytd, asg.ah,
           cap.ep - l.lp as pnl, lag(h.bh / cap.ch) over (order by m.month) as upm
    from (select distinct month from gold_dim_date) m
    left join hours h using (month) left join cap using (month) left join logging l using (month) left join asg using (month)
    order by m.month
""")}
pairs = dict(zip(MONTH_MEASURES, ["hl", "bh", "ch", "util", "ep", "lp", "cov", "uh", "w7", "pm", "ytd", "ah", "pnl", "upm"], strict=True))
for month, w in want.items():
    for measure, field in pairs.items():
        compare(measure, month, num(got.get(month, {}).get(measure)), w[field])

# ---- by account -----------------------------------------------------------------------------
ACCOUNT_MEASURES = ["Assigned Hours", "Sold Value", "Billable Hours", "Sold Utilization %"]
cols = ", ".join(f'"{m}", [{m}]' for m in ACCOUNT_MEASURES)
got = {r["Account"]: r for r in dax(f"EVALUATE SUMMARIZECOLUMNS ( Account[Account], {cols} )") if r.get("Account")}
for w in duck("""
    with a as (select a.account_id, sum(a.assigned_hours) as ah, sum(a.assigned_hours * d.rate) as sv
               from gold_fact_assignment_month a join gold_dim_deliverable d using (deliverable_id) group by a.account_id),
    b as (select p.account_id, sum(f.billable_hours) as bh from gold_fact_worklog f
          join gold_dim_project p using (project_key) where p.account_id is not null group by p.account_id)
    select acc.account_name, a.ah, a.sv, b.bh, b.bh / a.ah as su
    from gold_dim_account acc left join a using (account_id) left join b using (account_id)
"""):
    g = got.get(w["account_name"], {})
    for measure, field in zip(ACCOUNT_MEASURES, ["ah", "sv", "bh", "su"], strict=True):
        compare(measure, w["account_name"], num(g.get(measure)), w[field])

# ---- totals ---------------------------------------------------------------------------------
TOTALS = ["Median Logging Delay (days)", "Unattributed Share %", "Unused Sold Hours", "Billable Share %", "Average Sold Rate",
          "Worklist Hours", "Worklist Entries", "Days Without Logging"]
cols = ", ".join(f'"{m}", [{m}]' for m in TOTALS)
g = dax(f"EVALUATE ROW ( {cols} )")[0]
w = duck("""
    select median(delay_days) as med,
           sum(hours) filter (where not matched) / sum(hours) as us,
           greatest(0, (select sum(assigned_hours) from gold_fact_assignment_month) - sum(billable_hours)) as unused,
           sum(billable_hours) / sum(hours) as bs,
           (select sum(a.assigned_hours * d.rate) / sum(a.assigned_hours) from gold_fact_assignment_month a
            join gold_dim_deliverable d using (deliverable_id)) as rate,
           (select sum(hours) from gold_audit_unmatched_workers) as wh,
           (select sum(entries) from gold_audit_unmatched_workers) as we,
           (select max(days_since_last_log) from gold_mart_nonloggers) as dw
    from gold_fact_worklog
""")[0]
for measure, field in zip(TOTALS, ["med", "us", "unused", "bs", "rate", "wh", "we", "dw"], strict=True):
    compare(measure, "total", num(g.get(measure)), w[field])

# ---- time intelligence on one month -----------------------------------------------------------
g = dax("""EVALUATE CALCULATETABLE ( ROW ( "mom", [Hours Logged MoM %], "chg", [Utilization Change (pts)] ),
           'Date'[Month] = DATE ( 2026, 9, 1 ) )""")[0]
w = duck("""
    with m as (select date_trunc('month', date)::date as month, sum(hours) as h, sum(billable_hours) as b from gold_fact_worklog group by 1),
    c as (select date_trunc('month', date)::date as month, sum(capacity_hours) as c from gold_fact_capacity group by 1)
    select (s.h - a.h) / a.h as mom, s.b / cs.c - a.b / ca.c as chg
    from m s join m a on a.month = date '2026-08-01' join c cs on cs.month = s.month join c ca on ca.month = a.month
    where s.month = date '2026-09-01'
""")[0]
compare("Hours Logged MoM %", "2026-09", num(g["mom"]), w["mom"])
compare("Utilization Change (pts)", "2026-09", num(g["chg"]), w["chg"])

print(f"checked {checked} values across {len(MONTH_MEASURES) + len(ACCOUNT_MEASURES) + len(TOTALS) + 2} measure views")
if failures:
    print("\n".join(failures))
    sys.exit(1)
print("every DAX measure matches DuckDB")
