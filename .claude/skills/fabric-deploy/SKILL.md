---
name: fabric-deploy
description: Deploy a project's Fabric items (fabric/workspace) to a Microsoft Fabric workspace with fabric-cicd, then backfill, schedule and verify. Use only when the user explicitly asks to deploy or publish to Fabric.
disable-model-invocation: true
---

# Deploy to Microsoft Fabric

This changes a real workspace. Confirm the **workspace GUID and environment** (`dev`/`prod`) with the user before publishing, and never use a workspace that belongs to an employer or client for this portfolio project.

## Prerequisites (ask, don't assume)
- A workspace on a Fabric capacity (trial or F SKU), where the user is Member or Admin.
- `python -m pip install fabric-cicd azure-identity` in the project venv.
- For CI only: a service principal with Contributor on the workspace, and "Service principals can use Fabric APIs" enabled in the tenant. Pass `AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`AZURE_CLIENT_SECRET` as secrets, never in files.

## Pre-flight
```
python run.py test            # must be green: item checks catch what breaks a publish
git status                    # deploy what's committed
```

## Publish
```
python fabric/deploy.py --workspace-id <GUID> --environment dev            # browser sign-in
python fabric/deploy.py --workspace-id <GUID> --environment dev --device-code
```
Items are matched by `logicalId`, so re-running updates in place. `parameter.yml` replaces the pipelines' sentinel notebook and workspace IDs for that environment. `--unpublish-orphans` deletes workspace items that aren't in the repo: confirm first.

## After the first publish
1. **Key Vault** (only when real sources replace the synthetic ones): set `KeyVaultUri` in `wl_settings` and store credentials there. Never put a secret in the variable library.
2. **Backfill:** run `pl_monthly_full` once, or `nb_orchestrator` with `mode=full, date_from=2026-01-01`.
3. **Schedule:** `pl_daily` daily, `pl_monthly_full` monthly (pipeline → Schedule).
4. **Verify** in the lakehouse SQL analytics endpoint:
   `select top 12 run_date, step, status, seconds from ops_run_log order by started_at desc`
   `select check_name, value from ops_quality_results where passed = 0`
5. **Alerting:** have the pipeline's on-failure path send a Teams or Outlook message (the orchestrator raises on any error-level check).

## Report
Workspace (name, not GUID, if the user prefers), environment, items published, the first run's status and quality results, and anything skipped.
