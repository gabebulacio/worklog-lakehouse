"""Publish fabric/workspace/ to a Fabric workspace with Microsoft's fabric-cicd (no Git integration needed).

  python -m pip install fabric-cicd azure-identity
  python fabric/deploy.py --workspace-id <GUID> --environment dev
  python fabric/deploy.py --workspace-id <GUID> --environment prod --unpublish-orphans

Auth: interactive browser sign-in by default; `--device-code` without a browser; in CI set
AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET for a service principal that is a
Contributor (or higher) on the workspace. Items are matched by logicalId (.platform), so
re-running updates in place. parameter.yml swaps the pipelines' sentinel IDs per environment.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ITEM_TYPES = ["Lakehouse", "VariableLibrary", "Notebook", "DataPipeline", "SemanticModel", "Report"]
REPO_DIR = Path(__file__).resolve().parent / "workspace"


def credential(device_code: bool):
    from azure.identity import ClientSecretCredential, DeviceCodeCredential, InteractiveBrowserCredential

    if os.getenv("AZURE_CLIENT_SECRET"):
        return ClientSecretCredential(os.environ["AZURE_TENANT_ID"], os.environ["AZURE_CLIENT_ID"],
                                      os.environ["AZURE_CLIENT_SECRET"])
    return DeviceCodeCredential() if device_code else InteractiveBrowserCredential()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workspace-id", required=True, help="target workspace GUID (from its URL)")
    ap.add_argument("--environment", default="dev", choices=["dev", "prod"], help="parameter.yml environment")
    ap.add_argument("--items", nargs="*", default=ITEM_TYPES, help="item types to publish")
    ap.add_argument("--device-code", action="store_true", help="device-code sign-in instead of a browser")
    ap.add_argument("--unpublish-orphans", action="store_true", help="delete workspace items not in the repo")
    args = ap.parse_args()

    try:
        from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
    except ImportError:
        print("fabric-cicd is not installed: python -m pip install fabric-cicd azure-identity", file=sys.stderr)
        return 1

    workspace = FabricWorkspace(
        workspace_id=args.workspace_id,
        environment=args.environment,
        repository_directory=str(REPO_DIR),
        item_type_in_scope=args.items,
        token_credential=credential(args.device_code),
    )
    publish_all_items(workspace)
    if args.unpublish_orphans:
        unpublish_all_orphan_items(workspace)
    print(f"published {', '.join(args.items)} to workspace {args.workspace_id} ({args.environment})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
