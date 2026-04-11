# Semantic Governance Pipeline

Python CLI that cleans supplier A/B AASX packages, enforces stable UUIDs, builds a deduplicated semantic dictionary, and optionally uploads everything to the BaSyx gateway.

This upload pipeline is the recommended preparation step for the split schematic-generation pipeline. The intended standard workflow is:
- start Docker and the BaSyx stack
- upload supplier/runtime data with this script
- run the split pipeline in its default `basyx` mode

Use the split pipeline's `local` mode only as a fallback when the BaSyx runtime cannot be used.

## Prerequisites
- Python 3.10+ on Windows/PowerShell.
- Install dependencies:
  ```powershell
  python -m venv .venv
  .\.venv\Scripts\Activate.ps1
  pip install -r semantic-governance\requirements.txt
  ```

## Default layout
- Input (legacy): `data/supplierA_raw/*.aasx`, `data/supplierB_raw/*.aasx`
- Input (scan mode): any `data/supplier*_raw/*.aasx` (e.g., supplierC_raw, supplierD_raw)
- Persistent ID store: `semantic-governance/id-map-store.json` (auto-created/updated)
- Outputs per run: `out/run_<timestamp>/`
  - `semantic-dictionary.aasx`
  - `supplierA_clean/*.aasx`
  - `supplierB_clean/*.aasx`
  - `supplierC_clean/*.aasx` (if present)
  - `supplierD_clean/*.aasx` (if present)
  - `supplierE_clean/*.aasx` (if present)
  - `id-map.json` (run snapshot)
  - `report.json`

## Deterministic ID policy
- AAS IDs: `urn:uuid:<uuid5>` seeded by `assetInformation.globalAssetId` if present, otherwise `file name + idShort + assetType`.
- Submodel IDs: `urn:uuid:<uuid5>` seeded by the AAS seed plus submodel `idShort` (or original submodel id).
- The persistent `id-map-store.json` retains aliases so IDs stay stable even if upstream metadata changes.
- `semanticId` values are never modified.

## Run (dry run)
```powershell
python semantic-governance/aasx_pipeline.py
```
Outputs land in `out/run_<timestamp>/` without uploading.

## Run (scan all suppliers under data/ by default, upload enabled)
```powershell
python semantic-governance/aasx_pipeline.py
```
This processes every `data/supplier*_raw` directory and uploads results to the gateway.
Use `--suppliers-dir-root <path>` to scan a different root.

## Run (dry run, no upload)
```powershell
python semantic-governance/aasx_pipeline.py --no-upload
```

## Run (legacy A/B only)
```powershell
python semantic-governance/aasx_pipeline.py --no-scan-suppliers
```

## Run with upload (explicit gateway/registry)
```powershell
python semantic-governance/aasx_pipeline.py --gateway http://localhost:8080 --registry http://localhost:8083
```
Uploads in order: semantic dictionary -> each supplier’s cleaned packages. Dictionary `409` conflicts are ignored; component `409` errors abort.

## Recommended Follow-Up
After a successful upload, run the split pipeline from `Github_upload` in the default `basyx` mode, for example:

```powershell
python ..\run_qa_pipeline.py --system-aasx ..\samples\system_aas\test_sys_1.aasx
python ..\run_network_pipeline.py --system-aasx ..\samples\system_aas\test_sys_1.aasx --network-xlsx ..\samples\network_inputs\volume_node_demo.xlsx
```

## Verify-only (no generation, no upload)
Use this to validate existing outputs under `out/<run-id>/`:
```powershell
python semantic-governance/aasx_pipeline.py --verify-only --out-dir out/run_test
```
Produces `out/run_test/verify-report.json` and fails if shells/submodels dropped to zero.

## Inspect AASX contents (payload + counts)
Example PowerShell one-liner to inspect the payload path and counts:
```powershell
@'
import zipfile, pathlib
from xml.etree import ElementTree as ET
path = pathlib.Path("out/run_test/supplierA_clean/A_Tank1.aasx")
with zipfile.ZipFile(path) as zf:
    rels = zf.read("aasx/_rels/aasx-origin.rels")
    root = ET.fromstring(rels)
    target = None
    for rel in root.iter():
        if rel.tag.endswith("Relationship") and rel.attrib.get("Type") == "http://admin-shell.io/aasx/relationships/aas-spec":
            target = rel.attrib.get("Target")
            break
    payload = target.lstrip("/")
    data = zf.read(payload)
root = ET.fromstring(data)
ns = {"aas":"https://admin-shell.io/aas/3/0"}
print("payload:", payload)
print("shells:", len(list(root.find("aas:assetAdministrationShells", ns))))
print("submodels:", len(list(root.find("aas:submodels", ns))))
print("conceptDescriptions:", len(list(root.find("aas:conceptDescriptions", ns))))
'@ | python -
```

## Expected output counts
- Cleaned supplier packages: `conceptDescriptions == 0`, and `shells/submodels` should match the raw input counts.
- Semantic dictionary: `shells == 0`, `submodels == 0`, `conceptDescriptions > 0`.

## Verification snippets (PowerShell/curl)
```powershell
curl http://localhost:8080/supplierA/shells
curl http://localhost:8080/supplierB/shells
curl http://localhost:8080/supplierC/shells
curl http://localhost:8080/supplierD/shells
curl http://localhost:8080/supplierE/shells
curl http://localhost:8080/semantic/shells
curl http://localhost:8083/shell-descriptors
```
After upload, `report.json` also records shell counts, registry route checks, and a semantic sanity check for IRDI-prefixed semanticIds.

## Minimal self-check steps
```powershell
docker compose up -d
curl -sS http://localhost:8080/supplierC/shells
curl -sS http://localhost:8080/supplierD/shells
curl -sS http://localhost:8080/supplierE/shells
curl -sS http://localhost:8083/shell-descriptors
python semantic-governance/aasx_pipeline.py --scan-suppliers --upload
```
Confirm `report.json` or `verify-report.json` includes supplierC/D/E entries and `payloads.status == "ok"`.
