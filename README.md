# AutoCircuitGenerationBasedonAAS

This folder contains the GitHub-ready split version of the hydraulic circuit generation pipeline.

**Structure**
- `Github_upload/pipeline_01_nlp_processing.py`: NLP processing module.
- `Github_upload/pipeline_02_component_selection.py`: component retrieval and selection module.
- `Github_upload/pipeline_03_automatic_diagram_generation.py`: automatic drawing and SVG generation module.
- `Github_upload/pipeline_04_aas_integration_adapter.py`: shared adapter/orchestration layer for AAS access and write-back.
- `Github_upload/run_qa_pipeline.py`: QA entry.
- `Github_upload/run_network_pipeline.py`: network entry.
- `Github_upload/prompts`: stage-1 and stage-2 prompt templates.
- `Github_upload/skeleton`: skeleton library and QA tree configuration.
- `Github_upload/references`: `EClassIRDI.xlsx`, `SymbolHashing.xlsx`, and the network mapping workbook.
- `Github_upload/templates`: blank A4 drawing template.
- `Github_upload/symbols`: SVG symbol library.
- `Github_upload/supplier_runtime`: BaSyx runtime configuration, upload pipeline, and supplier AASX source data.
- `Github_upload/samples/system_aas`: sample System AASX inputs.
- `Github_upload/samples/network_inputs`: sample volume-node network inputs.

**Thesis Mapping**
The split follows the thesis module boundary:
- `NLP Processing Module`
- `Component Selection Module`
- `Automatic Diagram Generation Module`
- `AAS Integration Adapter`

**Environment**
- Python 3.10+
- Node.js, then run `npm install` inside `Github_upload` for `elkjs`
- Ollama for the QA entry
- Docker Desktop or Docker Engine with Compose support for the recommended BaSyx workflow

Environment meaning:
- Python: required for every mode
- Node.js + `npm install`: required for every mode, because the drawing/layout step uses `elkjs`
- Ollama: required only for the QA entry
- Docker: required only for the recommended `basyx` workflow

**Default Mode**
Default mode is `basyx`.

- `basyx` is the standard and recommended workflow.
- Start the BaSyx stack from `Github_upload/supplier_runtime`.
- Upload supplier AASX packages before running the split pipeline.
- Use `local` only as a fallback mode when Docker or the BaSyx runtime is unavailable.

**Install By Scenario**

1. Minimum installation for `network` entry in fallback `local` mode:

```powershell
cd D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload
npm install
```

You need:
- Python
- Node.js

You do not need:
- Ollama
- Docker

2. Installation for `qa` entry in fallback `local` mode:

```powershell
cd D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload
npm install
```

You need:
- Python
- Node.js
- Ollama running locally

You do not need:
- Docker

3. Installation for the recommended `basyx` workflow:

```powershell
cd D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload
pip install -r requirements.txt
npm install
```

You also need:
- Docker Desktop or Docker Engine with Compose support
- Ollama only if you want the QA entry

What the dependency files mean:
- `requirements.txt`: Python third-party dependencies. In this repository it is mainly needed by the BaSyx upload/validation workflow under `Github_upload/supplier_runtime/semantic-governance`.
- `package.json`: Node-side dependencies. It installs `elkjs`, which the drawing/layout module needs in both `qa` and `network` entries.

Short rule:
- `network + local`: install `npm` dependencies
- `qa + local`: install `npm` dependencies and run Ollama
- `basyx`: install both Python and Node dependencies, then install/start Docker

**Entries**
Use these two entry files:
- [run_qa_pipeline.py](/D:/RWTH/MasterThesis/MyThesisProject/Thesis/AutoCircuitGenerationBasedonAAS/Github_upload/run_qa_pipeline.py)
- [run_network_pipeline.py](/D:/RWTH/MasterThesis/MyThesisProject/Thesis/AutoCircuitGenerationBasedonAAS/Github_upload/run_network_pipeline.py)

Do not treat `pipeline_04_aas_integration_adapter.py` as the main user entry. It is the shared adapter/orchestration layer underneath the two entries.

**Recommended Workflow: BaSyx**
1. Start the runtime stack and upload supplier data:

```powershell
cd D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\supplier_runtime
docker compose up -d
python semantic-governance/aasx_pipeline.py --scan-suppliers --upload
```

2. Run the QA entry:

```powershell
python D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\run_qa_pipeline.py --system-aasx D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\samples\system_aas\test_sys_1.aasx
```

3. Run the network entry:

```powershell
python D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\run_network_pipeline.py --system-aasx D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\samples\system_aas\test_sys_1.aasx --network-xlsx D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\samples\network_inputs\volume_node_demo.xlsx
```

**Fallback Workflow: Local**
Use this only when BaSyx cannot be used.

QA fallback:

```powershell
python D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\run_qa_pipeline.py --system-aasx D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\samples\system_aas\test_sys_1.aasx --source local
```

Network fallback:

```powershell
python D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\run_network_pipeline.py --system-aasx D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\samples\system_aas\test_sys_1.aasx --network-xlsx D:\RWTH\MasterThesis\MyThesisProject\Thesis\AutoCircuitGenerationBasedonAAS\Github_upload\samples\network_inputs\volume_node_demo.xlsx --source local
```

**Network Input**
The network entry is Excel-first.

Supported Excel columns:
- `VolumeNodeId`
- `ComponentSlot`
- `PortKey` or `Port_Id`
- `ComponentLabel` or `Component_Type` as an optional override

Legacy JSON input is kept only as a compatibility fallback.

**Supplier Runtime**
The BaSyx runtime preparation pipeline is under:
- `Github_upload/supplier_runtime/docker-compose.yml`
- `Github_upload/supplier_runtime/semantic-governance/aasx_pipeline.py`

Docker is not bundled with the repository. Users must install Docker themselves if they want the recommended BaSyx workflow.

**Output**
Each run writes to `Github_upload/output/run_<timestamp>/`:
- `audit/nlp_audit_<timestamp>.json`
- `diagrams/circuit_diagram_<timestamp>.svg`
- `exports/<SystemAAS>_updated_<timestamp>.aasx`

**Notes**
- All runtime paths inside the split modules were changed to relative paths under `Github_upload`.
- `test_sys_1.aasx` supports audit file write-back through the `Audit_file` element under `SystemRequirements`.
- Code default for `--source` is `basyx`. Use `--source local` only when you intentionally want the fallback mode.
