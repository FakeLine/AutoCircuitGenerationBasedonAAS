# Supplier Runtime

This folder contains the BaSyx runtime assets used by the split pipeline.

It is part of the recommended `basyx` workflow:
- start the Docker stack from this folder
- upload supplier AASX data with `semantic-governance/aasx_pipeline.py`
- run the main split pipeline in its default `basyx` mode

Use the main pipeline's `local` mode only as a fallback when Docker or the BaSyx runtime is unavailable.

**Prerequisites**
- Python 3.10+
- Docker Desktop or Docker Engine with Compose support
- `pip install -r ..\requirements.txt`

**Contents**
- `docker-compose.yml`: BaSyx runtime stack
- `nginx/`: gateway routing
- `semantic/`: semantic repository configuration
- `supplierA` to `supplierE`: supplier environment configuration
- `data/`: supplier AASX source packages
- `semantic-governance/`: upload and validation pipeline
- `technical_properties_summary.json`: summary index used by the main pipeline
- `technical_properties_summary.csv`: companion export of the same summary data

**Quickstart**
```powershell
cd D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\supplier_runtime
docker compose up -d
python semantic-governance/aasx_pipeline.py --scan-suppliers --upload
```

After upload, run one of the two main entries from `Github_upload`:

```powershell
python ..\run_qa_pipeline.py --system-aasx ..\samples\system_aas\test_sys_1.aasx
python ..\run_network_pipeline.py --system-aasx ..\samples\system_aas\test_sys_1.aasx --network-xlsx ..\samples\network_inputs\volume_node_demo.xlsx
```

**Notes**
- Docker is not bundled with the repository. Users must install Docker themselves for the recommended BaSyx workflow.
- Removing old clone-tool documentation does not affect the main split pipeline.
