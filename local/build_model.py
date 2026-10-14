"""Generate the Power BI semantic model (TMDL) for Fabric: fabric/workspace/wl_model.SemanticModel.

  python local/build_model.py            writes the Direct Lake model (the deployable item)
  python local/build_model.py --import   also writes .lake/model_import/: the same model with Import
                                         partitions over local Parquet exports, for Power BI Desktop

One definition, two storage modes: tables, columns, relationships and DAX measures are identical;
only the partitions differ. Lineage tags are deterministic (uuid5), so regenerating gives the same files.
"""

from __future__ import annotations

import shutil
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ITEM = ROOT / "fabric" / "workspace" / "wl_model.SemanticModel"
NS = uuid.UUID("0c0ffee0-1a2b-4c3d-8e9f-a0b1c2d3e4f5")
SQL_ENDPOINT, SQL_ENDPOINT_ID = "__SQL_ENDPOINT__", "__SQL_ENDPOINT_ID__"  # swapped by parameter.yml

# dataType per Delta type
DT = {"date": "dateTime", "string": "string", "int": "int64", "double": "double", "bool": "boolean", "ts": "dateTime"}

# table name -> (lakehouse table, description, [(column, source column, type, options)])
# options: hidden, key, fmt=<format string>, summarize=<sum|none>, folder=<display folder>
TABLES = {
    "Date": ("gold_dim_date", "Calendar from the first worklog to the run date. Marked as the date table.", [
        ("Date", "date", "date", "key fmt=yyyy-mm-dd"), ("Year", "year", "int", ""), ("Quarter", "quarter", "int", ""),
        ("Month", "month", "date", "fmt=mmm yyyy"), ("Month Name", "month_name", "string", ""),
        ("Week Start", "week_start", "date", "fmt=yyyy-mm-dd"), ("Weekday", "weekday", "int", ""), ("Is Weekend", "is_weekend", "bool", ""),
    ]),
    "User": ("gold_dim_user", "One row per person, current version of the SCD2 history, plus an Unknown member.", [
        ("User Key", "user_key", "string", "hidden key"), ("Person", "display_name", "string", ""), ("Email", "email", "string", ""),
        ("Department", "department", "string", ""), ("Job Title", "job_title", "string", ""), ("Country", "country", "string", ""),
        ("Origin", "origin", "string", ""), ("Manager", "manager_name", "string", ""), ("Employee ID", "employee_id", "string", ""),
        ("Account Enabled", "account_enabled", "bool", ""), ("Timesheet Exempt", "is_timesheet_exempt", "bool", ""),
        ("Expected To Log", "is_expected", "bool", ""), ("Hire Date", "hire_date", "date", "fmt=yyyy-mm-dd"),
        ("Leave Date", "leave_date", "date", "fmt=yyyy-mm-dd"), ("Current Since", "current_since", "date", "hidden"),
    ]),
    "Account": ("gold_dim_account", "CRM accounts, conformed: projects and sold assignments both point here. Internal projects have no account.", [
        ("Account ID", "account_id", "string", "hidden key"), ("Account", "account_name", "string", ""),
        ("Account Owner", "account_owner", "string", ""), ("Tracker Key", "tracker_project_key", "string", ""),
        ("Account State", "account_state", "string", ""),
    ]),
    "Project": ("gold_dim_project", "One row per source project code, with the stable key from the append-only registry.", [
        ("Project Key", "project_key", "int", "hidden key summarize=none"), ("Source", "source", "string", ""),
        ("Project Code", "project_code", "string", ""), ("Project", "project_name", "string", ""),
        ("Account ID", "account_id", "string", "hidden"), ("Billable", "is_billable", "bool", ""), ("First Seen", "first_seen", "date", "fmt=yyyy-mm-dd"),
    ]),
    "Deliverable": ("gold_dim_deliverable", "Sold positions from the CRM: role, period, monthly hours and rate.", [
        ("Deliverable ID", "deliverable_id", "string", "hidden key"), ("Role", "role", "string", ""),
        ("Start Date", "start_date", "date", "fmt=yyyy-mm-dd"), ("End Date", "end_date", "date", "fmt=yyyy-mm-dd"),
        ("Monthly Hours", "monthly_hours", "double", "summarize=none fmt=#,##0"), ("Rate", "rate", "double", "summarize=none fmt=$#,##0.00"),
        ("Approved", "approved", "bool", ""), ("Opportunity", "opportunity_name", "string", ""), ("Account ID", "account_id", "string", "hidden"),
        ("Account Name", "account_name", "string", "hidden"),
    ]),
    "Worklog": ("gold_fact_worklog", "One row per worklog from both time systems, attributed to a person and a project.", [
        ("Date", "date", "date", "hidden"), ("Source", "source", "string", ""), ("User Key", "user_key", "string", "hidden"),
        ("Project Key", "project_key", "int", "hidden summarize=none"), ("Entry ID", "external_id", "string", "hidden"),
        ("Hours", "hours", "double", "hidden"), ("Billable", "billable_hours", "double", "hidden"),
        ("Delay Days", "delay_days", "int", "hidden"), ("Matched", "matched", "bool", ""), ("Match Method", "match_method", "string", ""),
        ("Worker ID", "worker_id", "string", "hidden"), ("Summary", "summary", "string", ""), ("Issue", "issue_key", "string", ""),
        ("Created At", "created_at", "ts", "hidden"), ("Modified On", "modified_on", "ts", "hidden"),
    ]),
    "Capacity": ("gold_fact_capacity", "One row per expected person per working day: the denominator of utilization.", [
        ("Date", "date", "date", "hidden"), ("User Key", "user_key", "string", "hidden"), ("Hours", "capacity_hours", "double", "hidden"),
    ]),
    "Assignment": ("gold_fact_assignment_month", "CRM assignments prorated to person x month: hours sold.", [
        ("Month", "month", "date", "hidden"), ("User Key", "user_key", "string", "hidden"), ("Deliverable ID", "deliverable_id", "string", "hidden"),
        ("Account ID", "account_id", "string", "hidden"), ("Assignment ID", "assignment_id", "string", "hidden"),
        ("Allocation", "allocation", "double", "hidden"), ("Fraction", "fraction", "double", "hidden"),
        ("Hours", "assigned_hours", "double", "hidden"),
    ]),
    "Non-loggers": ("gold_mart_nonloggers", "People expected to log with no worklog in the trailing window. A follow-up list.", [
        ("User Key", "user_key", "string", "hidden"), ("Person", "display_name", "string", ""), ("Email", "email", "string", ""),
        ("Department", "department", "string", ""), ("Manager", "manager_name", "string", ""), ("Employee ID", "employee_id", "string", ""),
        ("Last Log Date", "last_log_date", "date", "fmt=yyyy-mm-dd"), ("Days Since Last Log", "days_since_last_log", "int", "summarize=none"),
    ]),
    "Unmatched Workers": ("gold_audit_unmatched_workers", "Worklog identities no person matched: the mapping-file worklist.", [
        ("Source", "source", "string", ""), ("Worker ID", "worker_id", "string", ""), ("Worker Email", "worker_email", "string", ""),
        ("Worker Name", "worker_name", "string", ""), ("Entries", "entries", "int", "summarize=sum"), ("Hours", "hours", "double", "summarize=sum fmt=#,##0.0"),
        ("First Date", "first_date", "date", "fmt=yyyy-mm-dd"), ("Last Date", "last_date", "date", "fmt=yyyy-mm-dd"),
    ]),
}

# (from table, from column) -> (to table, to column)
RELATIONSHIPS = [
    ("Worklog", "Date", "Date", "Date"), ("Worklog", "User Key", "User", "User Key"), ("Worklog", "Project Key", "Project", "Project Key"),
    ("Project", "Account ID", "Account", "Account ID"),
    ("Capacity", "Date", "Date", "Date"), ("Capacity", "User Key", "User", "User Key"),
    ("Assignment", "Month", "Date", "Date"), ("Assignment", "User Key", "User", "User Key"),
    ("Assignment", "Deliverable ID", "Deliverable", "Deliverable ID"), ("Assignment", "Account ID", "Account", "Account ID"),
]

# table -> [(measure, folder, format, description, DAX)]
MEASURES = {
    "Unmatched Workers": [
        ("Worklist Hours", "Review", "#,##0.0", "Hours waiting on a mapping-file entry.", "SUM ( 'Unmatched Workers'[Hours] )"),
        ("Worklist Entries", "Review", "#,##0", "Worklog entries waiting on a mapping-file entry.", "SUM ( 'Unmatched Workers'[Entries] )"),
    ],
    "Non-loggers": [
        ("Days Without Logging", "Review", "#,##0", "Days since the person last logged (blank: never logged).",
         "MAX ( 'Non-loggers'[Days Since Last Log] )"),
    ],
    "Worklog": [
        ("Hours Logged", "Time", "#,##0", "All hours in worklogs from both systems.", "SUM ( Worklog[Hours] )"),
        ("Billable Hours", "Time", "#,##0", "Hours logged against a CRM account (internal and pending projects excluded).",
         "SUM ( Worklog[Billable] )"),
        ("Billable Share %", "Time", "0.0%", "Billable hours as a share of all hours logged.", "DIVIDE ( [Billable Hours], [Hours Logged] )"),
        ("Unattributed Hours", "Identity", "#,##0", "Hours no person could be matched to (kept in the fact as Unknown).",
         "CALCULATE ( [Hours Logged], Worklog[Matched] = FALSE () )"),
        ("Unattributed Share %", "Identity", "0.00%", "Share of hours waiting on the mapping-file worklist.",
         "DIVIDE ( [Unattributed Hours], [Hours Logged] )"),
        ("Median Logging Delay (days)", "Timeliness", "0.0", "Median days between the work date and when the entry was created.",
         "MEDIAN ( Worklog[Delay Days] )"),
        ("Logged Within 7 Days %", "Timeliness", "0.0%", "Share of entries created within a week of the work date.",
         "DIVIDE (\n    CALCULATE ( COUNTROWS ( Worklog ), Worklog[Delay Days] <= 7 ),\n    COUNTROWS ( Worklog )\n)"),
        ("Hours Logged PM", "Time intelligence", "#,##0", "Hours logged in the previous month (Date is the marked date table).",
         "CALCULATE ( [Hours Logged], DATEADD ( 'Date'[Date], -1, MONTH ) )"),
        ("Hours Logged MoM %", "Time intelligence", "+0.0%;-0.0%;0.0%", "Change in hours logged against the previous month.",
         "VAR prior = [Hours Logged PM]\nRETURN\n    DIVIDE ( [Hours Logged] - prior, prior )"),
        ("Hours Logged YTD", "Time intelligence", "#,##0", "Hours logged from 1 January to the end of the current filter.",
         "TOTALYTD ( [Hours Logged], 'Date'[Date] )"),
    ],
    "Capacity": [
        ("Capacity Hours", "Capacity", "#,##0", "Hours expected people could work: working days employed x hours per day.",
         "SUM ( Capacity[Hours] )"),
        ("Utilization %", "Capacity", "0.0%", "Billable hours over capacity hours.", "DIVIDE ( [Billable Hours], [Capacity Hours] )"),
        ("Utilization % PM", "Time intelligence", "0.0%", "Utilization in the previous month.",
         "CALCULATE ( [Utilization %], DATEADD ( 'Date'[Date], -1, MONTH ) )"),
        ("Utilization Change (pts)", "Time intelligence", "+0.0%;-0.0%;0.0%", "Percentage-point change in utilization against the previous month.",
         "VAR prior = [Utilization % PM]\nRETURN\n    IF ( NOT ISBLANK ( prior ), [Utilization %] - prior )"),
        ("Expected People", "Coverage", "#,##0", "People with capacity in the period: enabled, not exempt, employed.",
         "CALCULATE ( DISTINCTCOUNT ( 'User'[User Key] ), Capacity )"),
        ("Logging People", "Coverage", "#,##0", "Expected people who logged at least one entry in the period.",
         "VAR expected = CALCULATETABLE ( VALUES ( 'User'[User Key] ), Capacity )\n"
         "RETURN\n    COUNTROWS ( FILTER ( expected, CALCULATE ( NOT ISEMPTY ( Worklog ) ) ) )"),
        ("Coverage %", "Coverage", "0.0%", "Logging people over expected people.", "DIVIDE ( [Logging People], [Expected People] )"),
        ("People Not Logging", "Coverage", "#,##0", "Expected people with no worklog in the period.", "[Expected People] - [Logging People]"),
    ],
    "Assignment": [
        ("Assigned Hours", "Sold vs utilized", "#,##0", "Hours sold through CRM assignments, prorated by month and allocation.",
         "SUM ( Assignment[Hours] )"),
        ("Sold Value", "Sold vs utilized", "$#,##0", "Assigned hours priced at each deliverable's rate.",
         "SUMX ( Assignment, Assignment[Hours] * RELATED ( Deliverable[Rate] ) )"),
        ("Average Sold Rate", "Sold vs utilized", "$#,##0.00", "Sold value per assigned hour.", "DIVIDE ( [Sold Value], [Assigned Hours] )"),
        ("Sold Utilization %", "Sold vs utilized", "0.0%", "Billable hours delivered over hours sold.", "DIVIDE ( [Billable Hours], [Assigned Hours] )"),
        ("Unused Sold Hours", "Sold vs utilized", "#,##0", "Sold hours not covered by billable work (never negative).",
         "MAX ( 0, [Assigned Hours] - [Billable Hours] )"),
    ],
}


def tag(*parts):
    return str(uuid.uuid5(NS, "/".join(parts)))


def q(name):
    return f"'{name}'" if not name.replace("_", "").isalnum() else name


def column_tmdl(table, name, source, typ, opts):
    o = dict(x.split("=", 1) if "=" in x else (x, True) for x in opts.split())
    lines = [f"\tcolumn {q(name)}", f"\t\tdataType: {DT[typ]}"]
    if "fmt" in o:
        lines.append(f"\t\tformatString: {o['fmt']}")
    if "key" in o:
        lines.append("\t\tisKey")
    if "hidden" in o:
        lines.append("\t\tisHidden")
    lines.append(f"\t\tlineageTag: {tag(table, 'col', name)}")
    lines.append(f"\t\tsummarizeBy: {o.get('summarize', 'sum' if typ in ('int', 'double') else 'none')}")
    lines.append(f"\t\tsourceColumn: {source}")
    return "\n".join(lines) + "\n"


def measure_tmdl(table, name, folder, fmt, desc, dax):
    body = dax.replace("\n", "\n\t\t\t")
    expr = f" =\n\t\t\t{body}" if "\n" in dax else f" = {dax}"
    return (f"\t/// {desc}\n\tmeasure {q(name)}{expr}\n\t\tformatString: {fmt}\n\t\tdisplayFolder: {folder}\n"
            f"\t\tlineageTag: {tag(table, 'measure', name)}\n")


def partition_tmdl(table, entity, mode, parquet_dir=None):
    if mode == "directLake":
        return (f"\tpartition {q(table)} = entity\n\t\tmode: directLake\n\t\tsource\n"
                f"\t\t\tentityName: {entity}\n\t\t\texpressionSource: DatabaseQuery\n")
    path = str(Path(parquet_dir) / f"{entity}.parquet").replace('"', '""')
    return (f"\tpartition {q(table)} = m\n\t\tmode: import\n\t\tsource =\n"
            f"\t\t\t\tlet\n\t\t\t\t    Source = Parquet.Document ( File.Contents ( \"{path}\" ) )\n\t\t\t\tin\n\t\t\t\t    Source\n")


def write_model(target: Path, mode: str, parquet_dir: str | None = None):
    definition = target / "definition"
    if definition.exists():
        shutil.rmtree(definition)
    (definition / "tables").mkdir(parents=True)
    (definition / "database.tmdl").write_text("database\n\tcompatibilityLevel: 1604\n\n", encoding="utf-8", newline="\n")
    refs = "".join(f"ref table {q(t)}\n" for t in TABLES)
    (definition / "model.tmdl").write_text(
        "model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n\tsourceQueryCulture: en-US\n"
        "\tdataAccessOptions\n\t\tlegacyRedirects\n\t\treturnErrorValuesAsNull\n\n" + refs, encoding="utf-8", newline="\n")
    if mode == "directLake":
        (definition / "expressions.tmdl").write_text(
            "/// The lakehouse's SQL analytics endpoint; parameter.yml swaps in the real values at deploy time.\n"
            f"expression DatabaseQuery =\n\t\tlet\n\t\t    database = Sql.Database ( \"{SQL_ENDPOINT}\", \"{SQL_ENDPOINT_ID}\" )\n"
            f"\t\tin\n\t\t    database\n\tlineageTag: {tag('expr', 'DatabaseQuery')}\n\n"
            "\tannotation PBI_IncludeFutureArtifacts = False\n\n", encoding="utf-8", newline="\n")
    for table, (entity, desc, cols) in TABLES.items():
        out = [f"/// {desc}\ntable {q(table)}\n"]
        if table == "Date":
            out.append("\tdataCategory: Time\n")
        out.append(f"\tlineageTag: {tag(table)}\n\n")
        for name, folder, fmt, mdesc, dax in MEASURES.get(table, []):
            out.append(measure_tmdl(table, name, folder, fmt, mdesc, dax) + "\n")
        for name, source, typ, opts in cols:
            out.append(column_tmdl(table, name, source, typ, opts) + "\n")
        out.append(partition_tmdl(table, entity, mode, parquet_dir))
        (definition / "tables" / f"{table}.tmdl").write_text("".join(out), encoding="utf-8", newline="\n")
    rel = "".join(f"relationship {tag('rel', f, fc)}\n\tfromColumn: {q(f)}.{q(fc)}\n\ttoColumn: {q(t)}.{q(tc)}\n\n"
                  for f, fc, t, tc in RELATIONSHIPS)
    (definition / "relationships.tmdl").write_text(rel, encoding="utf-8", newline="\n")


def export_parquet(lake: Path, out: Path):
    """Each gold table's current Delta version as one Parquet file (Power Query reads Parquet natively)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from deltalake import DeltaTable

    out.mkdir(parents=True, exist_ok=True)
    for entity, _, _ in TABLES.values():
        table = DeltaTable((lake / entity).as_posix()).to_pyarrow_table()
        fields = [f.with_type(pa.timestamp("us")) if pa.types.is_timestamp(f.type) else f for f in table.schema]
        pq.write_table(table.cast(pa.schema(fields)), out / f"{entity}.parquet")


if __name__ == "__main__":
    write_model(ITEM, "directLake")
    print(f"wrote {ITEM}")
    if "--import" in sys.argv:
        local = ROOT / ".lake" / "model_import"
        export_parquet(ROOT / ".lake" / "wl_lakehouse" / "Tables", local / "parquet")
        write_model(local, "import", str((local / "parquet").resolve()))
        print(f"wrote {local} (Import partitions over {local / 'parquet'})")
