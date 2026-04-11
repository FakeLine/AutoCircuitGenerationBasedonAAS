This folder stores the supplier AASX source packages used by the `supplier_runtime` workflow.

- Recommended usage: upload these packages into the BaSyx stack with `semantic-governance/aasx_pipeline.py`, then run the main split pipeline in `basyx` mode.
- Fallback usage only: the main split pipeline can read these files directly in `local` mode when Docker/BaSyx is unavailable.
