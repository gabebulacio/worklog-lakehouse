"""End to end: the real notebooks, a real (local) Delta lakehouse, eight runs."""

import pytest
from conftest import BACKFILL_FROM, DAILY


def test_every_run_passed_the_quality_gate(daily_lake):
    failed = daily_lake.sql("select run_id, check_name from q where not passed and severity = 'error'",
                            q="ops_quality_results")
    assert failed == []
    runs = daily_lake.sql("select count(distinct run_id) from q", q="ops_quality_results")[0][0]
    assert runs >= 1 + len(DAILY)


def silver_rows(lake):
    return set(lake.sql("select source, external_id, worker_id, date, hours from s", s="silver_worklogs"))


def test_incremental_history_equals_one_full_extract_except_deleted_days(daily_lake, full_lake):
    """Daily incrementals + the person-day rule converge on the source's truth. The only
    possible gap: a person-day whose entries were all deleted (an incremental pull can't see it)."""
    daily, full = silver_rows(daily_lake), silver_rows(full_lake)
    assert not full - daily, "an incremental run lost rows the source still has"
    full_days = {(s, w, d) for s, _, w, d, _ in full}
    stale = daily - full
    assert all((s, w, d) not in full_days for s, _, w, d, _ in stale), "stale rows must be whole deleted days"


def test_monthly_full_run_removes_deleted_days(daily_lake, full_lake):
    # rerun a full extract on the same date: now the two histories must be identical
    daily_lake.orchestrate("full", DAILY[-1], date_from=BACKFILL_FROM)
    assert silver_rows(daily_lake) == silver_rows(full_lake)


def test_bronze_ingest_is_idempotent(daily_lake):
    before = daily_lake.version("bronze_timesheet_app_timesheets")
    out = daily_lake.run("nb_bronze_ingest", run_id="rerun")
    assert '"files": 0' in out
    assert daily_lake.version("bronze_timesheet_app_timesheets") == before


def test_project_keys_never_change(daily_lake):
    def rows(version=None):
        return {tuple(r.values()) for r in daily_lake.table("silver_project_registry", version).to_pylist()}

    current = rows()
    for v in range(daily_lake.version("silver_project_registry") + 1):
        assert rows(v) <= current, f"registry version {v}: a key was changed or removed"


def test_scd2_tracks_department_moves_and_reruns_are_noops(daily_lake):
    moved = daily_lake.sql("select count(*) from (select 1 from h group by user_key having count(*) > 1)",
                           h="silver_user_history")[0][0]
    assert moved >= 4, "the synthetic world moves people between departments in early October"
    before = daily_lake.sql("select count(*) from h", h="silver_user_history")[0][0]
    daily_lake.run("nb_silver_users", run_id="rerun")
    after = daily_lake.sql("select count(*) from h", h="silver_user_history")[0][0]
    assert after == before, "re-running on the same snapshot must not close or add versions"


def test_ambiguous_names_are_never_guessed(daily_lake):
    """A swapped login (last.first@) whose owner has a namesake stays unmatched for a human to map."""
    guessed_into_namesake = daily_lake.sql("""
        select count(*) from f join u using (user_key)
        where f.match_method in ('default_domain', 'swapped_name')
          and lower(u.display_name) in (select lower(display_name) from u group by 1 having count(*) > 1)
    """, f="gold_fact_worklog", u="gold_dim_user")[0][0]
    assert guessed_into_namesake == 0
    audit = daily_lake.sql("select source from a", a="gold_audit_unmatched_workers")
    assert ("timesheet_app",) in audit, "the ambiguous login should be on the unmatched worklist"


@pytest.mark.parametrize("table", ["gold_fact_worklog", "gold_dim_user", "gold_dim_project", "gold_dim_date",
                                   "gold_mart_sold_vs_utilized", "gold_mart_coverage_month",
                                   "gold_mart_nonloggers", "gold_audit_unmatched_workers"])
def test_gold_tables_exist_and_are_populated(daily_lake, table):
    assert daily_lake.table(table).num_rows > 0
