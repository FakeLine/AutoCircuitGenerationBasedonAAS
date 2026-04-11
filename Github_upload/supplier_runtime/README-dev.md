# Dev workflow (basyx-mock-suppliers)

This is the primary runtime workflow for the split pipeline. The recommended end-to-end mode is `basyx`, not `local`. Local mode should be treated as a fallback/degraded mode only.

## Findings (current structure)
- docker-compose.yml
  - Services: aas-registry, submodel-registry, supplierA-env..supplierE-env, semantic-env, gateway (nginx).
  - Images: eclipsebasyx/aas-registry-log-mem:2.0.0-SNAPSHOT, eclipsebasyx/submodel-registry-log-mem:2.0.0-SNAPSHOT, eclipsebasyx/aas-environment:2.0.0-SNAPSHOT, nginx:alpine.
  - Ports: 8083:8080 (AAS registry), 8084:8080 (submodel registry), 8090-8095:8081 (semantic/suppliers), 8080:80 (gateway).
  - Volumes: supplier*/application.properties -> /application/application.properties; semantic/application.properties -> /application/application.properties; nginx/default.conf -> /etc/nginx/conf.d/default.conf (ro).
  - depends_on: gateway depends on supplier*-env and semantic-env.
  - No custom networks or healthcheck definitions.

- nginx/default.conf
  - Locations: /supplierA/, /supplierB/, /supplierC/, /supplierD/, /supplierE/, /semantic/.
  - proxy_pass targets: supplier*-env:8081 and semantic-env:8081.
  - No rewrite rule; /supplierA/shells maps to /shells on the backend.

- supplier*/application.properties and semantic/application.properties
  - server.port=8081, basyx.backend=InMemory.
  - Registry integration: basyx.aasrepository.feature.registryintegration=http://aas-registry:8080 and basyx.submodelrepository.feature.registryintegration=http://submodel-registry:8080.
  - basyx.externalurl=http://localhost:8080/{supplier} (registry endpointAddress includes /{supplier}/).
  - All suppliers use the same image with per-supplier properties.

- semantic-governance/aasx_pipeline.py
  - Default mode uses direct host ports (809x); gateway mode is enabled by passing --gateway.
  - Endpoints (direct): /shells, /submodels, and /upload (root API by default).
  - Endpoints (gateway): /{supplier}/shells, /{supplier}/submodels, /{supplier}/upload; semantic uses /semantic/...
  - IPv6 fallback: if an IPv4 localhost request returns 404 with Server: Embedthis, it retries via [::1].
  - 409 behavior: semantic dictionary conflicts are allowed; supplier conflicts abort and write conflicts.json.

## Ports and URLs

| Service         | Host Port | Container Port |
|----------------|-----------|----------------|
| semantic-env   | 8090      | 8081           |
| supplierA-env  | 8091      | 8081           |
| supplierB-env  | 8092      | 8081           |
| supplierC-env  | 8093      | 8081           |
| supplierD-env  | 8094      | 8081           |
| supplierE-env  | 8095      | 8081           |

Direct URLs (preferred for dev/debug):
- supplierA: http://localhost:8091/...
- supplierB: http://localhost:8092/...
- supplierC: http://localhost:8093/...
- supplierD: http://localhost:8094/...
- supplierE: http://localhost:8095/...
- semantic:  http://localhost:8090/...

Gateway URLs (optional, if enabled):
- http://localhost:8080/supplierA/...
- http://localhost:8080/supplierB/...
- http://localhost:8080/supplierC/...
- http://localhost:8080/supplierD/...
- http://localhost:8080/supplierE/...
- http://localhost:8080/semantic/...

## Quickstart

Start services:
```powershell
docker compose up -d
```

Check that ports are published:
```powershell
docker ps
```
Expected: 0.0.0.0:809x->8081/tcp for semantic/suppliers and 0.0.0.0:8080->80/tcp for gateway.

### Verify repositories (PowerShell)
```powershell
Invoke-RestMethod http://localhost:8091/shells
Invoke-RestMethod http://localhost:8091/submodels
```

### Verify repositories (curl)
```bash
curl -sS http://localhost:8091/shells
curl -sS http://localhost:8091/submodels
```

### Pipeline runs (clean + upload)
First run (cleans before upload by default):
```powershell
python semantic-governance/aasx_pipeline.py --scan-suppliers --upload
```

Second run (repeats clean + upload, should avoid 409 conflicts):
```powershell
python semantic-governance/aasx_pipeline.py --scan-suppliers --upload
```

Optional: use gateway routing instead of direct ports:
```powershell
python semantic-governance/aasx_pipeline.py --scan-suppliers --upload --gateway http://localhost:8080
```

Optional: disable cleanup before upload:
```powershell
python semantic-governance/aasx_pipeline.py --scan-suppliers --upload --no-clean-before-upload
```

Notes:
- Direct port mapping can be overridden with --direct-port-map supplierA=8091 semantic=8090.
- Direct base host and repo path can be overridden with --direct-base-host and --aas-repository-path.
- Default repository path is root (empty). Use --aas-repository-path /aas-repository if your deployment requires that prefix.
- If a conflict occurs, the pipeline writes out/<run-id>/conflicts.json with the conflicting urn:uuid and type.
- After this upload step, the split pipeline under `Github_upload` should normally be run with the default `--source basyx`.
