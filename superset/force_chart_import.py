"""Force-apply chart params from the assets export bundle onto the live DB.

`superset import-dashboards` (the bundle import superset-init runs on every
startup) hardcodes overwrite=False for charts/datasets/databases -- only
dashboards get overwrite=True -- so chart edits in superset/assets/export/
are silently skipped on re-import. Confirmed against this build's
superset/commands/dashboard/importers/v1/__init__.py (import_chart returns
the existing chart unchanged when overwrite is falsy).

This script runs inside superset-init right after the standard bundle
import and force-updates every chart in the bundle, matched by uuid:
params, slice_name and description. Charts absent from the DB (fresh
volume) are left alone -- the standard import just created them from this
same bundle, so they already match. Only charts whose stored state
actually differs are written, so the script is idempotent and quiet on
restarts.

Scope note: datasets/databases have the same overwrite gap, but their
bundle content has never needed post-export edits -- charts change every
demo iteration, so this only covers charts. Extend it if that changes.
"""

import glob
import json
import os

import yaml
from superset.app import create_app
from superset.extensions import db

BUNDLE_DIR = os.environ.get("CHART_BUNDLE_DIR", "/app/assets_export/charts")


def main() -> None:
    app = create_app()
    # Slice must be imported AFTER create_app(): importing superset.models
    # at module top builds encrypted Column()s before the app is initialized
    # and raises "App not initialized yet. Please call init_app first".
    from superset.models.slice import Slice

    with app.app_context():
        updated = 0
        for path in sorted(glob.glob(os.path.join(BUNDLE_DIR, "*.yaml"))):
            with open(path) as fh:
                spec = yaml.safe_load(fh)
            chart_uuid = spec.get("uuid")
            if not chart_uuid:
                print(f"SKIP {os.path.basename(path)}: no uuid")
                continue
            slice_ = db.session.query(Slice).filter_by(uuid=chart_uuid).first()
            if slice_ is None:
                # Fresh volume: the standard import just created this chart
                # from the same bundle file, so it already matches.
                print(f"SKIP {os.path.basename(path)}: not in DB (fresh import)")
                continue
            changes = {}
            # Compare parsed JSON, not the stored string -- the DB's params
            # formatting (indentation/key order) doesn't have to match ours,
            # and only real content differences should trigger a write.
            current_params = json.loads(slice_.params) if slice_.params else {}
            if current_params != spec["params"]:
                changes["params"] = True
                slice_.params = json.dumps(spec["params"], indent=2, sort_keys=True)
            if spec.get("slice_name") and slice_.slice_name != spec["slice_name"]:
                changes["slice_name"] = True
                slice_.slice_name = spec["slice_name"]
            if "description" in spec and slice_.description != spec["description"]:
                changes["description"] = True
                slice_.description = spec["description"]
            if changes:
                updated += 1
                print(f"UPDATED {slice_.slice_name!r} (id={slice_.id}): {sorted(changes)}")
        if updated:
            db.session.commit()
        print(f"force_chart_import: {updated} chart(s) updated, rest already matched")


if __name__ == "__main__":
    main()
