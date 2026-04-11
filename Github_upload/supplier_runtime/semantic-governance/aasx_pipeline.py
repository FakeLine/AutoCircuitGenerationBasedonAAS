#!/usr/bin/env python
"""
Semantic governance and upload pipeline for BaSyx AASX packages.

"""
from __future__ import annotations

import argparse
import base64
import copy
import datetime as dt
import json
import re
import posixpath
import sys
import tempfile
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests
from xml.etree import ElementTree as ET

AAS_NS = "https://admin-shell.io/aas/3/0"
NSMAP = {"aas": AAS_NS}
ET.register_namespace("", AAS_NS)

UUID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "basyx-mock-suppliers-semantic-governance")
REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
REL_TYPE_AAS_SPEC = "http://admin-shell.io/aasx/relationships/aas-spec"
AASX_ORIGIN_PATH = "aasx/aasx-origin"
AASX_ORIGIN_RELS = "aasx/_rels/aasx-origin.rels"

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SUPPLIERS_ROOT = REPO_ROOT / "data"
DEFAULT_SUPPLIER_A = DEFAULT_SUPPLIERS_ROOT / "supplierA_raw"
DEFAULT_SUPPLIER_B = DEFAULT_SUPPLIERS_ROOT / "supplierB_raw"
SUPPLIER_RAW_GLOB = "supplier*_raw"
DEFAULT_OUT = REPO_ROOT / "out" / f"run_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
DEFAULT_DIRECT_HOST = "http://localhost"
DEFAULT_DIRECT_PORTS = {
    "semantic": 8090,
    "supplierA": 8091,
    "supplierB": 8092,
    "supplierC": 8093,
    "supplierD": 8094,
    "supplierE": 8095,
}
AAS_REPOSITORY_PATH = ""
HTTP_TIMEOUT = 10

STORE_PATH = Path(__file__).resolve().parent / "id-map-store.json"


# ----------------------------
# Data classes for run mapping
# ----------------------------
@dataclass
class SubmodelMapping:
    id_short: Optional[str]
    seed: str
    stable_id: str
    old_id: Optional[str] = None


@dataclass
class AASMapping:
    file: str
    id_short: Optional[str]
    seed: str
    stable_id: str
    global_asset_id: Optional[str]
    submodels: List[SubmodelMapping] = field(default_factory=list)


@dataclass
class RepoTarget:
    name: str
    base_url: str
    repo_path: str
    shells_url: str
    submodels_url: str
    upload_url: str


class UploadConflictError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        url: str,
        file_path: Path,
        body: str,
        conflict_id: Optional[str],
        conflict_type: Optional[str],
    ) -> None:
        super().__init__(message)
        self.url = url
        self.file_path = file_path
        self.body = body
        self.conflict_id = conflict_id
        self.conflict_type = conflict_type


# ----------------------------
# Store utilities
# ----------------------------
def load_store(store_path: Path) -> Dict:
    if store_path.exists():
        content = store_path.read_text(encoding="utf-8")
        if not content.strip():
            data = {}
        else:
            try:
                data = json.loads(content)
            except json.JSONDecodeError as exc:
                print(f"[WARN] Invalid JSON in {store_path}, starting fresh: {exc}")
                data = {}
    else:
        data = {}
    # normalize
    data.setdefault("aas", {})
    for _, record in data["aas"].items():
        record.setdefault("aliases", [])
        record.setdefault("submodels", {})
        record.setdefault("seed", "")
        for sub_key, sub_val in list(record["submodels"].items()):
            if isinstance(sub_val, str):
                record["submodels"][sub_key] = {"id": sub_val, "aliases": [], "seed": ""}
            else:
                sub_val.setdefault("aliases", [])
                sub_val.setdefault("seed", "")
    return data


def save_store(store_path: Path, data: Dict) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    with store_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ----------------------------
# Supplier discovery utilities
# ----------------------------
def supplier_name_from_raw_dir(path: Path) -> Optional[str]:
    name = path.name
    if name.startswith("supplier") and name.endswith("_raw"):
        return name[: -len("_raw")]
    return None


def scan_suppliers(root: Path) -> Dict[str, Path]:
    suppliers: Dict[str, Path] = {}
    if not root.exists():
        return suppliers
    for path in sorted(root.glob(SUPPLIER_RAW_GLOB)):
        if not path.is_dir():
            continue
        name = supplier_name_from_raw_dir(path)
        if name:
            suppliers[name] = path
    return suppliers


def resolve_suppliers(
    scan: bool, suppliers_root: Path, supplier_a: Path, supplier_b: Path
) -> Dict[str, Path]:
    suppliers: Dict[str, Path] = {}
    if scan:
        suppliers.update(scan_suppliers(suppliers_root))
        # Allow explicit A/B overrides if provided and exist.
        if supplier_a.exists():
            suppliers["supplierA"] = supplier_a
        if supplier_b.exists():
            suppliers["supplierB"] = supplier_b
        if not suppliers:
            suppliers = {"supplierA": supplier_a, "supplierB": supplier_b}
    else:
        suppliers = {"supplierA": supplier_a, "supplierB": supplier_b}
    return dict(sorted(suppliers.items()))


def parse_port_map(entries: Optional[List[str]]) -> Dict[str, int]:
    port_map: Dict[str, int] = {}
    if not entries:
        return port_map
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Invalid port map entry '{entry}', expected name=port.")
        name, port_str = entry.split("=", 1)
        name = name.strip()
        if not name:
            raise ValueError(f"Invalid port map entry '{entry}', name is empty.")
        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError(f"Invalid port for '{name}': {port_str}") from exc
        if port <= 0:
            raise ValueError(f"Invalid port for '{name}': {port}")
        port_map[name] = port
    return port_map


def normalize_repo_path(path: str) -> str:
    if not path:
        return ""
    trimmed = path.strip()
    if not trimmed or trimmed == "/":
        return ""
    if not trimmed.startswith("/"):
        trimmed = "/" + trimmed
    return trimmed.rstrip("/")


def normalize_base_host(base_host: str) -> str:
    if not base_host:
        raise ValueError("direct-base-host is empty.")
    candidate = base_host.strip()
    if "://" not in candidate:
        candidate = f"http://{candidate}"
    parsed = urlparse(candidate)
    if parsed.port is not None:
        raise ValueError("direct-base-host must not include a port; use --direct-port-map.")
    if parsed.path not in ("", "/"):
        raise ValueError("direct-base-host must not include a path.")
    host = parsed.netloc or parsed.path
    if not host:
        raise ValueError("direct-base-host is invalid.")
    return f"{parsed.scheme}://{host}"


def join_url(base: str, *parts: str) -> str:
    url = base.rstrip("/")
    for part in parts:
        if part:
            url = f"{url}/{part.strip('/')}"
    return url


def build_gateway_targets(gateway: str, suppliers: Iterable[str]) -> Tuple[Dict[str, RepoTarget], RepoTarget]:
    base = gateway.rstrip("/")
    supplier_targets: Dict[str, RepoTarget] = {}
    for name in suppliers:
        root = join_url(base, name)
        supplier_targets[name] = RepoTarget(
            name=name,
            base_url=root,
            repo_path="",
            shells_url=join_url(root, "shells"),
            submodels_url=join_url(root, "submodels"),
            upload_url=join_url(root, "upload"),
        )
    semantic_root = join_url(base, "semantic")
    semantic_target = RepoTarget(
        name="semantic",
        base_url=semantic_root,
        repo_path="",
        shells_url=join_url(semantic_root, "shells"),
        submodels_url=join_url(semantic_root, "submodels"),
        upload_url=join_url(semantic_root, "upload"),
    )
    return supplier_targets, semantic_target


def build_direct_targets(
    suppliers: Iterable[str],
    base_host: str,
    port_map: Dict[str, int],
    repo_path: str,
) -> Tuple[Dict[str, RepoTarget], RepoTarget]:
    normalized_host = normalize_base_host(base_host)
    repo_segment = normalize_repo_path(repo_path).strip("/")
    normalized_repo_path = normalize_repo_path(repo_path)

    def repo_url(base: str, *segments: str) -> str:
        if repo_segment:
            return join_url(base, repo_segment, *segments)
        return join_url(base, *segments)

    missing = [name for name in suppliers if name not in port_map]
    if missing:
        missing_list = ", ".join(missing)
        raise RuntimeError(f"Missing direct port mapping for: {missing_list}. Use --direct-port-map.")
    if "semantic" not in port_map:
        raise RuntimeError("Missing direct port mapping for semantic. Use --direct-port-map semantic=PORT.")

    supplier_targets: Dict[str, RepoTarget] = {}
    for name in suppliers:
        port = port_map[name]
        base = f"{normalized_host}:{port}"
        supplier_targets[name] = RepoTarget(
            name=name,
            base_url=base,
            repo_path=normalized_repo_path,
            shells_url=repo_url(base, "shells"),
            submodels_url=repo_url(base, "submodels"),
            upload_url=join_url(base, "upload"),
        )

    semantic_port = port_map["semantic"]
    semantic_base = f"{normalized_host}:{semantic_port}"
    semantic_target = RepoTarget(
        name="semantic",
        base_url=semantic_base,
        repo_path=normalized_repo_path,
        shells_url=repo_url(semantic_base, "shells"),
        submodels_url=repo_url(semantic_base, "submodels"),
        upload_url=join_url(semantic_base, "upload"),
    )
    return supplier_targets, semantic_target

# ----------------------------
# XML helpers
# ----------------------------
def get_text(elem: Optional[ET.Element], xpath: str) -> Optional[str]:
    if elem is None:
        return None
    child = elem.find(xpath, NSMAP)
    if child is not None and child.text:
        return child.text.strip()
    return None


def set_text(elem: ET.Element, xpath: str, value: str) -> None:
    target = elem.find(xpath, NSMAP)
    if target is None:
        return
    target.text = value


def normalize_payload_path(target: str, base_dir: str = "") -> str:
    normalized = target.strip().lstrip("\ufeff").replace("\\", "/")
    if normalized.startswith("/"):
        return normalized.lstrip("/")
    if base_dir:
        return posixpath.normpath(posixpath.join(base_dir, normalized))
    return posixpath.normpath(normalized)


def resolve_payload_path(zf: zipfile.ZipFile) -> str:
    names = set(zf.namelist())
    target = None
    if AASX_ORIGIN_RELS in names:
        data = zf.read(AASX_ORIGIN_RELS)
        root = ET.fromstring(data)
        for rel in root.iter():
            if rel.tag.endswith("Relationship") and rel.attrib.get("Type") == REL_TYPE_AAS_SPEC:
                target = rel.attrib.get("Target")
                if target:
                    candidate = normalize_payload_path(target, posixpath.dirname(AASX_ORIGIN_RELS))
                    if candidate in names:
                        return candidate
                    target = candidate
                break

    if not target and AASX_ORIGIN_PATH in names:
        text = zf.read(AASX_ORIGIN_PATH).decode("utf-8", errors="replace")
        text = text.strip().lstrip("\ufeff")
        if text and text.lower() not in ("intentionally empty",):
            candidate = normalize_payload_path(text)
            if candidate in names:
                return candidate
            target = candidate

    if target and target in names:
        return target

    candidates = [
        n
        for n in names
        if n.lower().endswith((".aas.xml", "aasenv.xml", ".json", "aasenv.json"))
    ]
    if not candidates:
        raise RuntimeError("No environment payload found in AASX package.")
    return sorted(candidates)[0]


def parse_payload_bytes(payload: bytes, name_hint: str) -> Tuple[str, Any]:
    lower = name_hint.lower()
    if lower.endswith(".json") or lower.endswith("aasenv.json"):
        return "json", json.loads(payload.decode("utf-8-sig"))
    try:
        root = ET.fromstring(payload)
        return "xml", root
    except ET.ParseError:
        return "json", json.loads(payload.decode("utf-8-sig"))


def load_payload_from_path(payload_path: Path) -> Tuple[str, Any]:
    return parse_payload_bytes(payload_path.read_bytes(), payload_path.name)


def save_payload_to_path(payload_path: Path, fmt: str, payload: Any) -> None:
    if fmt == "xml":
        payload_path.write_bytes(ET.tostring(payload, encoding="utf-8", xml_declaration=True))
        return
    if fmt == "json":
        with payload_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        return
    raise ValueError(f"Unsupported payload format: {fmt}")


def read_payload_from_aasx(aasx_path: Path) -> Tuple[str, Any, str]:
    with zipfile.ZipFile(aasx_path, "r") as zf:
        payload_rel = resolve_payload_path(zf)
        payload = zf.read(payload_rel)
    fmt, env = parse_payload_bytes(payload, payload_rel)
    return fmt, env, payload_rel


# ----------------------------
# UUID + mapping logic
# ----------------------------
def canonicalize_keys(keys: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for k in keys:
        if k and k not in seen:
            ordered.append(k)
            seen.add(k)
    return ordered


def find_aas_record(store: Dict, candidate_keys: List[str]) -> Tuple[Optional[str], Optional[Dict]]:
    for key, record in store["aas"].items():
        if key in candidate_keys or any(alias in candidate_keys for alias in record.get("aliases", [])):
            return key, record
    return None, None


def group_aas_match_keys(candidate_keys: List[str]) -> List[List[str]]:
    gaid_keys = [k for k in candidate_keys if k.startswith("gaid::")]
    file_keys = [k for k in candidate_keys if k.startswith("file::")]
    orig_keys = [k for k in candidate_keys if k.startswith("orig::")]
    idshort_keys = [k for k in candidate_keys if k.startswith("idShort::")]
    known = set(gaid_keys + file_keys + orig_keys + idshort_keys)
    other_keys = [k for k in candidate_keys if k not in known]

    groups: List[List[str]] = []
    if gaid_keys:
        groups.append(gaid_keys)
    else:
        groups.extend([file_keys, orig_keys, idshort_keys])
    if other_keys:
        groups.append(other_keys)
    return [group for group in groups if group]


def ensure_aas_record(store: Dict, seed: str, candidate_keys: List[str]) -> Tuple[str, Dict]:
    candidate_keys = canonicalize_keys(candidate_keys)
    primary = None
    record = None
    for match_keys in group_aas_match_keys(candidate_keys):
        primary, record = find_aas_record(store, match_keys)
        if record is not None and primary is not None:
            break
    if record is not None and primary is not None:
        # augment aliases with any new candidates
        aliases = set(record.get("aliases", []))
        for key in candidate_keys:
            if key != primary:
                aliases.add(key)
        record["aliases"] = sorted(aliases)
        if not record.get("seed"):
            record["seed"] = seed
        return primary, record

    primary = candidate_keys[0]
    stable_uuid = f"urn:uuid:{uuid.uuid5(UUID_NAMESPACE, seed)}"
    record = {
        "aas_id": stable_uuid,
        "aliases": candidate_keys[1:],
        "seed": seed,
        "submodels": {},
    }
    store["aas"][primary] = record
    return primary, record


def find_submodel_record(aas_record: Dict, candidate_keys: List[str]) -> Tuple[Optional[str], Optional[Dict]]:
    submodels = aas_record.get("submodels", {})
    for key, record in submodels.items():
        if key in candidate_keys or any(alias in candidate_keys for alias in record.get("aliases", [])):
            return key, record
    return None, None


def ensure_submodel_record(aas_record: Dict, seed: str, candidate_keys: List[str]) -> Tuple[str, Dict]:
    candidate_keys = canonicalize_keys(candidate_keys)
    primary, record = find_submodel_record(aas_record, candidate_keys)
    if record is not None and primary is not None:
        aliases = set(record.get("aliases", []))
        for key in candidate_keys:
            if key != primary:
                aliases.add(key)
        record["aliases"] = sorted(aliases)
        if not record.get("seed"):
            record["seed"] = seed
        return primary, record

    primary = candidate_keys[0]
    stable_uuid = f"urn:uuid:{uuid.uuid5(UUID_NAMESPACE, seed)}"
    record = {"id": stable_uuid, "aliases": candidate_keys[1:], "seed": seed}
    aas_record.setdefault("submodels", {})[primary] = record
    return primary, record


# ----------------------------
# Core processing
# ----------------------------
def generate_aas_keys(
    id_short: Optional[str],
    global_asset_id: Optional[str],
    asset_type: Optional[str],
    file_name: str,
    original_id: Optional[str],
) -> List[str]:
    keys: List[str] = []
    if global_asset_id:
        keys.append(f"gaid::{global_asset_id}")
    fallback = f"file::{file_name}|aas::{id_short or original_id or ''}|type::{asset_type or ''}"
    keys.append(fallback)
    if id_short:
        keys.append(f"idShort::{id_short}|type::{asset_type or ''}")
        keys.append(f"idShort::{id_short}")
    if original_id:
        keys.append(f"orig::{original_id}")
    return keys


def generate_submodel_keys(
    aas_store_key: str,
    submodel_id_short: Optional[str],
    old_submodel_id: Optional[str],
    file_name: str,
) -> List[str]:
    keys: List[str] = []
    if submodel_id_short:
        keys.append(f"{aas_store_key}::submodel::{submodel_id_short}")
    if old_submodel_id:
        keys.append(f"{aas_store_key}::submodel::{old_submodel_id}")
    keys.append(f"{aas_store_key}::submodel::{file_name}::{submodel_id_short or old_submodel_id or ''}")
    return keys


def update_identifiers_xml(
    root: ET.Element, file_name: str, store: Dict
) -> Tuple[ET.Element, List[AASMapping]]:
    aas_list = root.find("aas:assetAdministrationShells", NSMAP)
    submodel_list = root.find("aas:submodels", NSMAP)
    if aas_list is None or submodel_list is None:
        return root, []

    submodel_lookup = {}
    for sm in submodel_list:
        sid = get_text(sm, "aas:id")
        if sid:
            submodel_lookup[sid] = sm

    submodel_id_map: Dict[str, Tuple[str, Dict]] = {}
    mappings: List[AASMapping] = []

    for aas in aas_list:
        id_short = get_text(aas, "aas:idShort")
        original_id = get_text(aas, "aas:id")
        global_asset_id = get_text(aas, "aas:assetInformation/aas:globalAssetId")
        asset_type = get_text(aas, "aas:assetInformation/aas:assetType")

        aas_keys = generate_aas_keys(id_short, global_asset_id, asset_type, file_name, original_id)
        aas_seed = aas_keys[0]
        aas_store_key, aas_record = ensure_aas_record(store, aas_seed, aas_keys)
        stable_aas_id = aas_record["aas_id"]
        set_text(aas, "aas:id", stable_aas_id)

        aas_mapping = AASMapping(
            file=file_name,
            id_short=id_short,
            seed=aas_record.get("seed", aas_seed),
            stable_id=stable_aas_id,
            global_asset_id=global_asset_id,
        )

        submodel_refs = aas.find("aas:submodels", NSMAP)
        if submodel_refs is not None:
            for ref in submodel_refs:
                key_elem = ref.find("aas:keys/aas:key", NSMAP)
                if key_elem is None:
                    continue
                old_value = get_text(key_elem, "aas:value")
                if not old_value:
                    continue

                if old_value in submodel_id_map:
                    new_sub_id, sub_record = submodel_id_map[old_value]
                else:
                    sm_elem = submodel_lookup.get(old_value)
                    sub_id_short = get_text(sm_elem, "aas:idShort") if sm_elem is not None else None
                    sub_keys = generate_submodel_keys(aas_store_key, sub_id_short, old_value, file_name)
                    sm_seed = f"{aas_record.get('seed', aas_seed)}::submodel::{sub_id_short or old_value}"
                    sub_store_key, sub_record = ensure_submodel_record(aas_record, sm_seed, sub_keys)
                    new_sub_id = sub_record["id"]
                    submodel_id_map[old_value] = (new_sub_id, sub_record)
                    if sm_elem is not None:
                        set_text(sm_elem, "aas:id", new_sub_id)
                    aas_record["submodels"][sub_store_key] = sub_record

                set_text(key_elem, "aas:value", new_sub_id)

                aas_mapping.submodels.append(
                    SubmodelMapping(
                        id_short=get_text(submodel_lookup.get(old_value), "aas:idShort"),
                        seed=sub_record.get("seed", ""),
                        stable_id=new_sub_id,
                        old_id=old_value,
                    )
                )

        mappings.append(aas_mapping)

    return root, mappings


def unwrap_env_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    if "environment" in payload and isinstance(payload["environment"], dict):
        return payload["environment"]
    return payload


def update_identifiers_json(
    payload: Dict[str, Any], file_name: str, store: Dict
) -> Tuple[Dict[str, Any], List[AASMapping]]:
    env = unwrap_env_json(payload)
    aas_list = env.get("assetAdministrationShells") or []
    submodel_list = env.get("submodels") or []
    if not isinstance(aas_list, list) or not isinstance(submodel_list, list):
        return payload, []

    submodel_lookup = {
        sm.get("id"): sm for sm in submodel_list if isinstance(sm, dict) and sm.get("id")
    }
    submodel_id_map: Dict[str, Tuple[str, Dict]] = {}
    mappings: List[AASMapping] = []

    for aas in aas_list:
        if not isinstance(aas, dict):
            continue
        id_short = aas.get("idShort")
        original_id = aas.get("id")
        asset_info = aas.get("assetInformation") or {}
        global_asset_id = asset_info.get("globalAssetId") if isinstance(asset_info, dict) else None
        asset_type = asset_info.get("assetType") if isinstance(asset_info, dict) else None

        aas_keys = generate_aas_keys(id_short, global_asset_id, asset_type, file_name, original_id)
        aas_seed = aas_keys[0]
        aas_store_key, aas_record = ensure_aas_record(store, aas_seed, aas_keys)
        stable_aas_id = aas_record["aas_id"]
        aas["id"] = stable_aas_id

        aas_mapping = AASMapping(
            file=file_name,
            id_short=id_short,
            seed=aas_record.get("seed", aas_seed),
            stable_id=stable_aas_id,
            global_asset_id=global_asset_id,
        )

        submodel_refs = aas.get("submodels") or []
        if isinstance(submodel_refs, list):
            for ref in submodel_refs:
                if not isinstance(ref, dict):
                    continue
                key_entry = None
                keys = ref.get("keys")
                if isinstance(keys, list):
                    for entry in keys:
                        if isinstance(entry, dict) and "value" in entry:
                            key_entry = entry
                            break
                if key_entry is None:
                    continue
                old_value = key_entry.get("value")
                if not old_value:
                    continue

                sm_elem = submodel_lookup.get(old_value)
                sub_id_short = sm_elem.get("idShort") if isinstance(sm_elem, dict) else None
                if old_value in submodel_id_map:
                    new_sub_id, sub_record = submodel_id_map[old_value]
                else:
                    sub_keys = generate_submodel_keys(aas_store_key, sub_id_short, old_value, file_name)
                    sm_seed = f"{aas_record.get('seed', aas_seed)}::submodel::{sub_id_short or old_value}"
                    sub_store_key, sub_record = ensure_submodel_record(aas_record, sm_seed, sub_keys)
                    new_sub_id = sub_record["id"]
                    submodel_id_map[old_value] = (new_sub_id, sub_record)
                    if isinstance(sm_elem, dict):
                        sm_elem["id"] = new_sub_id
                    aas_record["submodels"][sub_store_key] = sub_record

                key_entry["value"] = new_sub_id
                aas_mapping.submodels.append(
                    SubmodelMapping(
                        id_short=sub_id_short,
                        seed=sub_record.get("seed", ""),
                        stable_id=new_sub_id,
                        old_id=old_value,
                    )
                )

        mappings.append(aas_mapping)

    return payload, mappings


def extract_concept_descriptions_xml(root: ET.Element) -> List[ET.Element]:
    cds_parent = root.find("aas:conceptDescriptions", NSMAP)
    if cds_parent is None:
        return []
    return [copy.deepcopy(cd) for cd in cds_parent]


def extract_concept_descriptions_json(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    env = unwrap_env_json(payload)
    cds = env.get("conceptDescriptions") or []
    if not isinstance(cds, list):
        return []
    return [copy.deepcopy(cd) for cd in cds if isinstance(cd, dict)]


def strip_concept_descriptions_xml(root: ET.Element) -> int:
    cds_parent = root.find("aas:conceptDescriptions", NSMAP)
    if cds_parent is None:
        return 0
    count = len(list(cds_parent))
    for child in list(cds_parent):
        cds_parent.remove(child)
    return count


def strip_concept_descriptions_json(payload: Dict[str, Any]) -> int:
    env = unwrap_env_json(payload)
    cds = env.get("conceptDescriptions") or []
    if not isinstance(cds, list):
        return 0
    count = len(cds)
    env["conceptDescriptions"] = []
    return count


def repack_aasx_from_dir(source_dir: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for path in sorted(source_dir.rglob("*")):
            if path.is_file():
                rel = path.relative_to(source_dir).as_posix()
                zout.write(path, rel)


def build_dictionary_aasx_xml(concepts: Dict[str, ET.Element], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    env = ET.Element(ET.QName(AAS_NS, "environment"))
    ET.SubElement(env, ET.QName(AAS_NS, "assetAdministrationShells"))
    ET.SubElement(env, ET.QName(AAS_NS, "submodels"))
    cds_el = ET.SubElement(env, ET.QName(AAS_NS, "conceptDescriptions"))
    for cd in concepts.values():
        cds_el.append(copy.deepcopy(cd))

    env_bytes = ET.tostring(env, encoding="utf-8", xml_declaration=True)

    with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        # minimal OPC structure
        zout.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="utf-8"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="xml" ContentType="text/xml" />'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml" />'
            '<Override PartName="/aasx/aasx-origin" ContentType="text/plain" />'
            "</Types>",
        )
        zout.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="utf-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Type="http://admin-shell.io/aasx/relationships/aasx-origin" '
            'Target="/aasx/aasx-origin" Id="R-semantic-origin" />'
            "</Relationships>",
        )
        zout.writestr("aasx/aasx-origin", "Intentionally empty")
        zout.writestr(
            "aasx/_rels/aasx-origin.rels",
            '<?xml version="1.0" encoding="utf-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Type="http://admin-shell.io/aasx/relationships/aas-spec" '
            'Target="/aasx/dictionary/semantic.aas.xml" Id="R-semantic-spec" />'
            "</Relationships>",
        )
        zout.writestr("aasx/dictionary/semantic.aas.xml", env_bytes)


def build_dictionary_aasx_json(concepts: Dict[str, Dict[str, Any]], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    env = {
        "assetAdministrationShells": [],
        "submodels": [],
        "conceptDescriptions": list(concepts.values()),
    }
    env_bytes = json.dumps(env, indent=2).encode("utf-8")

    with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        zout.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="utf-8"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="xml" ContentType="text/xml" />'
            '<Default Extension="json" ContentType="application/json" />'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml" />'
            '<Override PartName="/aasx/aasx-origin" ContentType="text/plain" />'
            "</Types>",
        )
        zout.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="utf-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Type="http://admin-shell.io/aasx/relationships/aasx-origin" '
            'Target="/aasx/aasx-origin" Id="R-semantic-origin" />'
            "</Relationships>",
        )
        zout.writestr("aasx/aasx-origin", "Intentionally empty")
        zout.writestr(
            "aasx/_rels/aasx-origin.rels",
            '<?xml version="1.0" encoding="utf-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Type="http://admin-shell.io/aasx/relationships/aas-spec" '
            'Target="/aasx/dictionary/semantic.aas.json" Id="R-semantic-spec" />'
            "</Relationships>",
        )
        zout.writestr("aasx/dictionary/semantic.aas.json", env_bytes)


# ----------------------------
# HTTP helpers
# ----------------------------
CONFLICT_ID_RE = re.compile(r"urn:uuid:[0-9a-fA-F-]+")


def ipv6_localhost_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if parsed.hostname not in ("localhost", "127.0.0.1"):
        return None
    netloc = "[::1]"
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


def should_retry_ipv6(resp: requests.Response, url: str) -> bool:
    server = (resp.headers.get("Server") or "").lower()
    if resp.status_code != 404:
        return False
    if "embedthis" not in server:
        return False
    return ipv6_localhost_url(url) is not None


def request_with_ipv6(method: str, url: str, **kwargs: Any) -> requests.Response:
    resp = requests.request(method, url, **kwargs)
    if should_retry_ipv6(resp, url):
        alt = ipv6_localhost_url(url)
        if alt:
            print(f"[WARN] {url} returned 404 from Embedthis; retrying via {alt}")
            resp = requests.request(method, alt, **kwargs)
    return resp


def parse_conflict_info(body: str) -> Tuple[Optional[str], Optional[str]]:
    text = body or ""
    conflict_id = None
    match = CONFLICT_ID_RE.search(text)
    if match:
        conflict_id = match.group(0)
    lower = text.lower()
    conflict_type = None
    if "submodel" in lower:
        conflict_type = "submodel"
    elif "shell" in lower or "aas" in lower or "assetadministrationshell" in lower:
        conflict_type = "aas"
    return conflict_id, conflict_type


def upload_aasx_to_basyx(upload_base_url: str, aasx_path: Path) -> Dict[str, Any]:
    upload_url = join_url(upload_base_url, "upload")
    print(f"[SEMANTIC] Uploading dictionary: {aasx_path}")
    print(f"[SEMANTIC] Upload URL: {upload_url}")
    with aasx_path.open("rb") as f:
        resp = request_with_ipv6(
            "POST",
            upload_url,
            files={"file": (aasx_path.name, f, "application/octet-stream")},
            timeout=HTTP_TIMEOUT,
        )
    print(f"[SEMANTIC] Upload status: {resp.status_code}")
    if not resp.ok:
        raise RuntimeError(
            f"Semantic dictionary upload failed at {upload_url}: {resp.status_code} {resp.text}"
        )

    shells_url = join_url(upload_base_url, "shells")
    shells_status = None
    shells_error = None
    try:
        shells_resp = request_with_ipv6("GET", shells_url, timeout=HTTP_TIMEOUT)
        shells_status = shells_resp.status_code
        print(f"[SEMANTIC] Shells check status: {shells_status}")
    except Exception as exc:  # noqa: BLE001
        shells_error = str(exc)
        print(f"[WARN] Semantic shells check failed at {shells_url}: {exc}")

    return {
        "status_code": resp.status_code,
        "shells_status": shells_status,
        "shells_error": shells_error,
    }


def post_aasx(url: str, file_path: Path, allow_exists: bool) -> Dict:
    with file_path.open("rb") as f:
        resp = requests.post(url, files={"file": (file_path.name, f, "application/octet-stream")})
    if should_retry_ipv6(resp, url):
        alt = ipv6_localhost_url(url)
        if alt:
            print(f"[WARN] {url} returned 404 from Embedthis; retrying via {alt}")
            with file_path.open("rb") as f:
                resp = requests.post(alt, files={"file": (file_path.name, f, "application/octet-stream")})
    if resp.status_code == 409:
        if allow_exists:
            return {"status": "exists", "code": resp.status_code, "body": resp.text}
        conflict_id, conflict_type = parse_conflict_info(resp.text)
        raise UploadConflictError(
            f"Upload conflict at {url}: {resp.text}",
            url=url,
            file_path=file_path,
            body=resp.text,
            conflict_id=conflict_id,
            conflict_type=conflict_type,
        )
    if not resp.ok:
        raise RuntimeError(f"Upload failed at {url}: {resp.status_code} {resp.text}")
    return {"status": "uploaded", "code": resp.status_code, "body": resp.text}


def fetch_json(url: str) -> Tuple[Optional[object], Optional[str]]:
    try:
        resp = request_with_ipv6("GET", url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json(), None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def ensure_semantic_prefix(obj: object, prefix: str = "0173-") -> bool:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "semanticId" and isinstance(value, dict):
                keys = value.get("keys") or []
                for entry in keys:
                    if isinstance(entry, dict):
                        val = entry.get("value", "")
                        if isinstance(val, str) and prefix in val:
                            return True
            if ensure_semantic_prefix(value, prefix):
                return True
    elif isinstance(obj, list):
        return any(ensure_semantic_prefix(item, prefix) for item in obj)
    elif isinstance(obj, str):
        return prefix in obj
    return False


def normalize_item_list(data: object) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        result = data.get("result")
        if isinstance(result, list):
            return [item for item in result if isinstance(item, dict)]
    return []


def extract_item_id(item: Dict[str, Any]) -> Optional[str]:
    for key in ("id", "identifier", "identification"):
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def encode_base64url_id(raw_id: str) -> str:
    encoded = base64.b64encode(raw_id.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=").replace("+", "-").replace("/", "_")


def probe_target(target: RepoTarget) -> None:
    primary_url = target.shells_url
    fallback_url = join_url(target.base_url, "shells")

    def raise_probe_failure(message: str) -> None:
        raise RuntimeError(
            f"Probe failed for {target.name}. {message} Expected shells at {primary_url}"
            f"{' or ' + fallback_url if target.repo_path else ''}."
        )

    try:
        resp = request_with_ipv6("GET", primary_url, timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        if target.repo_path:
            try:
                fallback_resp = request_with_ipv6("GET", fallback_url, timeout=HTTP_TIMEOUT)
            except requests.RequestException as fallback_exc:
                raise_probe_failure(f"Connection error: {exc}; fallback error: {fallback_exc}.")
            if fallback_resp.ok:
                return
            raise_probe_failure(
                f"Fallback {fallback_url} returned {fallback_resp.status_code} {fallback_resp.text}."
            )
        raise_probe_failure(f"Connection error: {exc}.")

    if resp.ok:
        return
    if target.repo_path and resp.status_code == 404:
        fallback_resp = request_with_ipv6("GET", fallback_url, timeout=HTTP_TIMEOUT)
        if fallback_resp.ok:
            return
        raise_probe_failure(
            f"Fallback {fallback_url} returned {fallback_resp.status_code} {fallback_resp.text}."
        )
    raise_probe_failure(f"{primary_url} returned {resp.status_code} {resp.text}.")


def delete_items(list_url: str, items: List[Dict[str, Any]], kind: str) -> int:
    deleted = 0
    for item in items:
        item_id = extract_item_id(item)
        if not item_id:
            raise RuntimeError(f"Missing {kind} id in listing from {list_url}")
        encoded_id = encode_base64url_id(item_id)
        delete_url = f"{list_url.rstrip('/')}/{encoded_id}"
        resp = request_with_ipv6("DELETE", delete_url, timeout=HTTP_TIMEOUT)
        if not resp.ok:
            raise RuntimeError(
                f"Failed to delete {kind} {item_id} at {delete_url}: {resp.status_code} {resp.text}"
            )
        deleted += 1
    return deleted


def cleanup_repository(target: RepoTarget) -> Dict[str, int]:
    shells_data, shells_err = fetch_json(target.shells_url)
    if shells_err:
        raise RuntimeError(f"Failed to list shells at {target.shells_url}: {shells_err}")
    shells = normalize_item_list(shells_data)
    deleted_shells = delete_items(target.shells_url, shells, "shell")
    shells_after_data, shells_after_err = fetch_json(target.shells_url)
    if shells_after_err:
        raise RuntimeError(f"Failed to verify shells at {target.shells_url}: {shells_after_err}")
    shells_after = normalize_item_list(shells_after_data)
    if shells_after:
        raise RuntimeError(f"Shell cleanup incomplete at {target.shells_url} ({len(shells_after)} remaining).")

    submodels_data, submodels_err = fetch_json(target.submodels_url)
    if submodels_err:
        raise RuntimeError(f"Failed to list submodels at {target.submodels_url}: {submodels_err}")
    submodels = normalize_item_list(submodels_data)
    deleted_submodels = delete_items(target.submodels_url, submodels, "submodel")
    submodels_after_data, submodels_after_err = fetch_json(target.submodels_url)
    if submodels_after_err:
        raise RuntimeError(f"Failed to verify submodels at {target.submodels_url}: {submodels_after_err}")
    submodels_after = normalize_item_list(submodels_after_data)
    if submodels_after:
        raise RuntimeError(
            f"Submodel cleanup incomplete at {target.submodels_url} ({len(submodels_after)} remaining)."
        )

    return {
        "shells_found": len(shells),
        "shells_deleted": deleted_shells,
        "submodels_found": len(submodels),
        "submodels_deleted": deleted_submodels,
    }


def verify_endpoints(
    supplier_targets: Dict[str, RepoTarget],
    semantic_target: RepoTarget,
    registry: str,
) -> Dict:
    result: Dict[str, object] = {"shell_counts": {}, "registry_routes": {}, "semantic_check": {}}

    shells_data: Dict[str, List] = {}
    for name, target in supplier_targets.items():
        data, err = fetch_json(target.shells_url)
        if err:
            result["shell_counts"][name] = {"error": err}
            continue
        try:
            shells_data[name] = list(data) if isinstance(data, list) else []
            result["shell_counts"][name] = {"count": len(shells_data[name])}
        except Exception as exc:  # noqa: BLE001
            result["shell_counts"][name] = {"error": str(exc)}

    semantic_data, semantic_err = fetch_json(semantic_target.shells_url)
    if semantic_err:
        result["shell_counts"]["semantic"] = {"error": semantic_err}
    else:
        semantic_list = list(semantic_data) if isinstance(semantic_data, list) else []
        result["shell_counts"]["semantic"] = {"count": len(semantic_list)}

    reg_data, reg_err = fetch_json(f"{registry.rstrip('/')}/shell-descriptors")
    if reg_err:
        result["registry_routes"] = {"error": reg_err}
    else:
        hits = {name: False for name in supplier_targets}
        descriptors = reg_data if isinstance(reg_data, list) else []
        for desc in descriptors:
            for ep in desc.get("endpoints", []):
                addr = ep.get("protocolInformation", {}).get("endpointAddress") or ep.get("address")
                if isinstance(addr, str):
                    for name in hits:
                        if f"/{name}/" in addr:
                            hits[name] = True
        result["registry_routes"] = hits

    # Semantic sanity: grab one submodel and look for IRDI prefix
    semantic_flag = False
    sample_error: Optional[str] = None
    sample_supplier = None
    for name in supplier_targets:
        if shells_data.get(name):
            sample_supplier = name
            break
    if sample_supplier and shells_data.get(sample_supplier):
        shell_id = shells_data[sample_supplier][0].get("id") or shells_data[sample_supplier][0].get("identifier")
        if isinstance(shell_id, str):
            shells_url = supplier_targets[sample_supplier].shells_url
            encoded_shell_id = encode_base64url_id(shell_id)
            sub_list_url = f"{shells_url.rstrip('/')}/{encoded_shell_id}/submodels"
            sub_list, err = fetch_json(sub_list_url)
            if err:
                sample_error = f"submodel list error: {err}"
            elif sub_list:
                sub_id = sub_list[0].get("id") or sub_list[0].get("identifier")
                if isinstance(sub_id, str):
                    sub_url = (
                        f"{shells_url.rstrip('/')}/{encoded_shell_id}/submodels/"
                        f"{encode_base64url_id(sub_id)}/submodel"
                    )
                    sub_data, sub_err = fetch_json(sub_url)
                    if sub_err:
                        sample_error = f"submodel fetch error: {sub_err}"
                    else:
                        semantic_flag = ensure_semantic_prefix(sub_data, "0173-")
        else:
            sample_error = "shell id missing for semantic check"
    else:
        sample_error = "no shells available for semantic check"

    result["semantic_check"] = {"has_irdi_prefix": semantic_flag, "sample_supplier": sample_supplier}
    if sample_error:
        result["semantic_check"]["note"] = sample_error
    return result


# ----------------------------
# Payload verification helpers
# ----------------------------
def count_payload_xml(root: ET.Element) -> Dict[str, int]:
    shells_el = root.find("aas:assetAdministrationShells", NSMAP)
    submodels_el = root.find("aas:submodels", NSMAP)
    cds_el = root.find("aas:conceptDescriptions", NSMAP)
    shells = len(list(shells_el)) if shells_el is not None else 0
    submodels = len(list(submodels_el)) if submodels_el is not None else 0
    cds = len(list(cds_el)) if cds_el is not None else 0
    return {"shells": shells, "submodels": submodels, "conceptDescriptions": cds}


def count_payload_json(payload: Dict[str, Any]) -> Dict[str, int]:
    env = unwrap_env_json(payload)
    shells = env.get("assetAdministrationShells") or []
    submodels = env.get("submodels") or []
    cds = env.get("conceptDescriptions") or []
    shells_count = len(shells) if isinstance(shells, list) else 0
    submodels_count = len(submodels) if isinstance(submodels, list) else 0
    cds_count = len(cds) if isinstance(cds, list) else 0
    return {"shells": shells_count, "submodels": submodels_count, "conceptDescriptions": cds_count}


def count_payload(fmt: str, payload: Any) -> Dict[str, int]:
    if fmt == "xml":
        return count_payload_xml(payload)
    if fmt == "json":
        return count_payload_json(payload)
    raise ValueError(f"Unsupported payload format: {fmt}")


def inspect_aasx_payload(aasx_path: Path) -> Dict[str, Any]:
    fmt, payload, payload_rel = read_payload_from_aasx(aasx_path)
    counts = count_payload(fmt, payload)
    return {"format": fmt, "payload": payload_rel, **counts}


def verify_outputs(suppliers: Dict[str, Path], out_dir: Path) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "status": "ok",
        "suppliers": {},
        "failures": [],
        "warnings": [],
    }

    for supplier_name, raw_dir in suppliers.items():
        report["suppliers"][supplier_name] = []
        if not raw_dir.exists():
            report["warnings"].append(f"{supplier_name}: raw directory missing ({raw_dir})")
            continue
        raw_files = sorted(raw_dir.glob("*.aasx"))
        if not raw_files:
            report["warnings"].append(f"{supplier_name}: no .aasx files found in {raw_dir}")
            continue

        out_clean = out_dir / f"{supplier_name}_clean"
        for raw_path in raw_files:
            out_path = out_clean / raw_path.name
            entry: Dict[str, Any] = {"file": raw_path.name}
            if not out_path.exists():
                entry["error"] = f"missing output: {out_path}"
                report["failures"].append(f"{supplier_name}:{raw_path.name} output missing")
                report["suppliers"][supplier_name].append(entry)
                continue

            raw_counts = inspect_aasx_payload(raw_path)
            out_counts = inspect_aasx_payload(out_path)
            entry["raw"] = raw_counts
            entry["output"] = out_counts
            report["suppliers"][supplier_name].append(entry)

            if raw_counts["shells"] > 0 and out_counts["shells"] == 0:
                report["failures"].append(f"{supplier_name}:{raw_path.name} shells dropped to 0")
            if raw_counts["submodels"] > 0 and out_counts["submodels"] == 0:
                report["failures"].append(f"{supplier_name}:{raw_path.name} submodels dropped to 0")

    if report["failures"]:
        report["status"] = "failed"
    return report


# ----------------------------
# Pipeline orchestration
# ----------------------------
def process_directory(
    supplier_name: str,
    source_dir: Path,
    output_dir: Path,
    store: Dict,
    concept_accumulator_xml: Dict[str, ET.Element],
    concept_accumulator_json: Dict[str, Dict[str, Any]],
) -> List[AASMapping]:
    output_dir.mkdir(parents=True, exist_ok=True)
    mappings: List[AASMapping] = []

    for aasx_path in sorted(source_dir.glob("*.aasx")):
        payload_fmt, payload_obj, payload_rel = read_payload_from_aasx(aasx_path)
        if payload_fmt == "xml":
            for cd in extract_concept_descriptions_xml(payload_obj):
                cid = get_text(cd, "aas:id")
                if cid and cid not in concept_accumulator_xml:
                    concept_accumulator_xml[cid] = cd
            payload_obj, file_mappings = update_identifiers_xml(payload_obj, aasx_path.name, store)
            strip_concept_descriptions_xml(payload_obj)
        else:
            for cd in extract_concept_descriptions_json(payload_obj):
                cid = cd.get("id") if isinstance(cd, dict) else None
                if cid and cid not in concept_accumulator_json:
                    concept_accumulator_json[cid] = cd
            payload_obj, file_mappings = update_identifiers_json(payload_obj, aasx_path.name, store)
            strip_concept_descriptions_json(payload_obj)

        dest_path = output_dir / aasx_path.name
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            with zipfile.ZipFile(aasx_path, "r") as zf:
                zf.extractall(tmp_path)
            payload_path = tmp_path / payload_rel
            if not payload_path.exists():
                raise RuntimeError(f"Payload path missing after extraction: {payload_rel}")
            save_payload_to_path(payload_path, payload_fmt, payload_obj)
            repack_aasx_from_dir(tmp_path, dest_path)

        mappings.extend(file_mappings)
        print(f"[{supplier_name}] cleaned {aasx_path.name} -> {dest_path}")

    return mappings


def run_pipeline(
    suppliers: Dict[str, Path],
    out_dir: Path,
    registry: str,
    upload: bool,
    verify_only: bool,
    runtime_targets: Dict[str, RepoTarget],
    semantic_target: RepoTarget,
    semantic_url: str,
    upload_semantic: bool,
    clean_before_upload: bool,
    runtime_mode: str,
) -> None:
    store = load_store(STORE_PATH)
    if verify_only:
        verify_report = verify_outputs(suppliers, out_dir)
        report_path = out_dir / "verify-report.json"
        out_dir.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8") as f:
            json.dump(verify_report, f, indent=2)
        print(f"Verification report written to: {report_path}")
        if verify_report.get("status") == "failed":
            raise RuntimeError("Verification failed; see verify-report.json for details.")
        save_store(STORE_PATH, store)
        return

    concept_accumulator_xml: Dict[str, ET.Element] = {}
    concept_accumulator_json: Dict[str, Dict[str, Any]] = {}
    run_mappings: List[AASMapping] = []
    print(f"Suppliers: {', '.join(suppliers.keys())}")

    cleaned_outputs: Dict[str, List[str]] = {}

    for supplier_name, source_dir in suppliers.items():
        out_dir_supplier = out_dir / f"{supplier_name}_clean"
        cleaned_outputs[supplier_name] = []
        if not source_dir.exists():
            print(f"[WARN] {supplier_name}: raw directory missing ({source_dir}), skipping.")
            continue
        if not any(source_dir.glob("*.aasx")):
            print(f"[WARN] {supplier_name}: no .aasx files found in {source_dir}, skipping.")
            continue

        run_mappings.extend(
            process_directory(
                supplier_name,
                source_dir,
                out_dir_supplier,
                store,
                concept_accumulator_xml,
                concept_accumulator_json,
            )
        )
        cleaned_outputs[supplier_name] = [str(p) for p in out_dir_supplier.glob("*.aasx")]

    if concept_accumulator_xml and concept_accumulator_json:
        raise RuntimeError("Mixed XML/JSON concept descriptions are not supported in a single run.")

    dictionary_path = out_dir / "semantic-dictionary.aasx"
    if concept_accumulator_json:
        build_dictionary_aasx_json(concept_accumulator_json, dictionary_path)
    else:
        build_dictionary_aasx_xml(concept_accumulator_xml, dictionary_path)

    id_map_payload = {
        "run_id": out_dir.name,
        "generated_at": dt.datetime.now().isoformat(),
        "items": [
            {
                "file": m.file,
                "idShort": m.id_short,
                "seed": m.seed,
                "stableId": m.stable_id,
                "globalAssetId": m.global_asset_id,
                "submodels": [
                    {
                        "idShort": sm.id_short,
                        "seed": sm.seed,
                        "stableId": sm.stable_id,
                        "oldId": sm.old_id,
                    }
                    for sm in m.submodels
                ],
            }
            for m in run_mappings
        ],
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    id_map_path = out_dir / "id-map.json"
    with id_map_path.open("w", encoding="utf-8") as f:
        json.dump(id_map_payload, f, indent=2)

    save_store(STORE_PATH, store)

    report: Dict[str, object] = {
        "run_id": out_dir.name,
        "outputs": {
            "semantic_dictionary": str(dictionary_path),
            "id_map": str(id_map_path),
            "cleaned": cleaned_outputs,
        },
        "runtime": {
            "mode": runtime_mode,
            "suppliers": {name: target.base_url for name, target in runtime_targets.items()},
            "semantic": semantic_target.base_url,
        },
        "upload": {},
        "verification": {},
    }

    verification = verify_outputs(suppliers, out_dir)
    report["verification"]["payloads"] = verification

    semantic_upload_report: Dict[str, object] = {"enabled": bool(upload_semantic)}
    semantic_upload_error: Optional[Exception] = None
    if upload_semantic:
        semantic_upload_report["semanticUrl"] = semantic_url
        semantic_upload_report["file"] = str(dictionary_path)
        try:
            semantic_result = upload_aasx_to_basyx(semantic_url, dictionary_path)
            semantic_upload_report["status"] = "success"
            semantic_upload_report["httpStatus"] = semantic_result.get("status_code")
            if semantic_result.get("shells_status") is not None:
                semantic_upload_report["shellsStatus"] = semantic_result.get("shells_status")
            if semantic_result.get("shells_error"):
                semantic_upload_report["shellsError"] = semantic_result.get("shells_error")
        except Exception as exc:  # noqa: BLE001
            semantic_upload_report["status"] = "failed"
            semantic_upload_report["error"] = str(exc)
            semantic_upload_error = exc
    report["semanticUpload"] = semantic_upload_report

    upload_error: Optional[Exception] = None
    if semantic_upload_error:
        report["upload"]["message"] = "Skipped due to semantic upload failure."
        upload_error = semantic_upload_error
    elif verification.get("status") == "failed":
        report["upload"]["message"] = "Skipped due to verification failure."
    elif upload:
        report["upload"]["suppliers"] = {}
        current_supplier: Optional[str] = None
        current_file: Optional[Path] = None
        try:
            suppliers_with_files = {
                name: runtime_targets[name] for name, paths in cleaned_outputs.items() if paths
            }

            probe_target(semantic_target)
            for target in suppliers_with_files.values():
                probe_target(target)

            if clean_before_upload:
                cleanup_summary = {"semantic": {}, "suppliers": {}}
                semantic_counts = cleanup_repository(semantic_target)
                cleanup_summary["semantic"] = semantic_counts
                print(
                    f"[CLEAN] semantic: shells {semantic_counts['shells_found']} deleted "
                    f"{semantic_counts['shells_deleted']}, submodels {semantic_counts['submodels_found']} deleted "
                    f"{semantic_counts['submodels_deleted']}"
                )
                for name, target in suppliers_with_files.items():
                    counts = cleanup_repository(target)
                    cleanup_summary["suppliers"][name] = counts
                    print(
                        f"[CLEAN] {name}: shells {counts['shells_found']} deleted {counts['shells_deleted']}, "
                        f"submodels {counts['submodels_found']} deleted {counts['submodels_deleted']}"
                    )
                report["upload"]["cleanup"] = cleanup_summary
            else:
                report["upload"]["cleanup"] = {"skipped": True}

            for supplier_name in suppliers:
                supplier_out_dir = out_dir / f"{supplier_name}_clean"
                files = sorted(supplier_out_dir.glob("*.aasx"))
                if not files:
                    report["upload"]["suppliers"][supplier_name] = {"skipped": "no cleaned packages"}
                    print(f"[WARN] {supplier_name}: no cleaned packages to upload, skipping.")
                    continue
                results = []
                for path in files:
                    current_supplier = supplier_name
                    current_file = path
                    results.append(post_aasx(runtime_targets[supplier_name].upload_url, path, allow_exists=False))
                report["upload"]["suppliers"][supplier_name] = results
                print(f"[UPLOAD] {supplier_name}: uploaded {len(files)} package(s).")
            report["verification"]["runtime"] = verify_endpoints(runtime_targets, semantic_target, registry)
        except UploadConflictError as exc:
            conflict_id = exc.conflict_id or "unknown"
            conflict_type = exc.conflict_type or "unknown"
            supplier_label = current_supplier or "unknown"
            file_label = current_file.name if current_file else exc.file_path.name
            conflict_record = {
                "supplier": supplier_label,
                "file": file_label,
                "url": exc.url,
                "conflict_id": exc.conflict_id,
                "conflict_type": exc.conflict_type,
                "body": exc.body,
            }
            conflicts_payload = {
                "run_id": out_dir.name,
                "generated_at": dt.datetime.now().isoformat(),
                "conflicts": [conflict_record],
            }
            conflicts_path = out_dir / "conflicts.json"
            with conflicts_path.open("w", encoding="utf-8") as f:
                json.dump(conflicts_payload, f, indent=2)
            report["upload"]["conflicts"] = conflicts_payload
            report["upload"]["error"] = str(exc)
            print(f"[CONFLICT] {supplier_label} {file_label}: {conflict_type} {conflict_id}")
            upload_error = exc
        except Exception as exc:  # noqa: BLE001
            report["upload"]["error"] = str(exc)
            upload_error = exc
    else:
        report["upload"]["message"] = "Skipped (dry run)."

    report_path = out_dir / "report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Run outputs saved under: {out_dir}")
    for supplier_name in cleaned_outputs:
        print(f"- Cleaned {supplier_name}: {out_dir / f'{supplier_name}_clean'}")
    print(f"- Semantic dictionary: {dictionary_path}")
    print(f"- ID map: {id_map_path}")
    print(f"- Report: {report_path}")

    if report.get("verification", {}).get("payloads", {}).get("status") == "failed":
        raise RuntimeError("Verification failed; see report.json for details.")
    if upload_error:
        raise RuntimeError(str(upload_error))


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Semantic governance pipeline for AASX packages.")
    parser.add_argument("--supplierA-dir", type=Path, default=DEFAULT_SUPPLIER_A, help="Input directory for supplier A AASX.")
    parser.add_argument("--supplierB-dir", type=Path, default=DEFAULT_SUPPLIER_B, help="Input directory for supplier B AASX.")
    parser.add_argument(
        "--scan-suppliers",
        action="store_true",
        default=True,
        help="Scan data/ for supplier*_raw directories and process all matches (default).",
    )
    parser.add_argument(
        "--no-scan-suppliers",
        action="store_true",
        help="Disable supplier directory scanning and use only supplierA/B dirs.",
    )
    parser.add_argument(
        "--suppliers-dir-root",
        type=Path,
        default=DEFAULT_SUPPLIERS_ROOT,
        help="Root directory for supplier*_raw scanning (default: data/).",
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT, help="Output directory (default: out/run_<timestamp>).")
    parser.add_argument(
        "--gateway",
        type=str,
        default=None,
        help="Gateway base URL (set to use gateway routing instead of direct host ports).",
    )
    parser.add_argument(
        "--direct-base-host",
        type=str,
        default=DEFAULT_DIRECT_HOST,
        help="Direct base host for supplier/semantic ports (default: http://localhost).",
    )
    parser.add_argument(
        "--direct-port-map",
        nargs="*",
        help="Override direct port mapping, e.g. supplierA=8091 supplierB=8092 semantic=8090.",
    )
    parser.add_argument(
        "--aas-repository-path",
        type=str,
        default=AAS_REPOSITORY_PATH,
        help="Repository base path for shells/submodels in direct mode (default: root, empty).",
    )
    parser.add_argument("--registry", type=str, default="http://localhost:8083", help="AAS registry base URL.")
    parser.add_argument(
        "--semantic-url",
        type=str,
        default="http://localhost:8090",
        help="Semantic repository base URL for dictionary upload (default: http://localhost:8090).",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        default=True,
        help="Upload cleaned supplier packages to runtime targets (default).",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Disable upload and run in dry mode.",
    )
    parser.add_argument(
        "--no-upload-semantic",
        action="store_true",
        help="Disable semantic dictionary upload.",
    )
    parser.add_argument(
        "--clean-before-upload",
        action="store_true",
        default=True,
        help="Clean repositories before upload (default).",
    )
    parser.add_argument(
        "--no-clean-before-upload",
        action="store_true",
        help="Disable repository cleanup before upload.",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Verify existing output AASX payloads without generating or uploading.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    if args.no_upload and args.upload:
        # argparse sets upload default True; explicit --no-upload disables it
        args.upload = False
    if args.no_clean_before_upload and args.clean_before_upload:
        args.clean_before_upload = False
    args.upload_semantic = not args.no_upload_semantic
    if args.verify_only:
        args.upload = False
        args.upload_semantic = False
    suppliers = resolve_suppliers(
        scan=not args.no_scan_suppliers,
        suppliers_root=args.suppliers_dir_root,
        supplier_a=args.supplierA_dir,
        supplier_b=args.supplierB_dir,
    )
    if not suppliers:
        raise RuntimeError("No suppliers found. Check --scan-suppliers or supplierA/B directories.")

    runtime_targets: Dict[str, RepoTarget] = {}
    semantic_target = RepoTarget(
        name="semantic",
        base_url="",
        repo_path="",
        shells_url="",
        submodels_url="",
        upload_url="",
    )
    runtime_mode = "verify-only"
    if not args.verify_only:
        direct_ports = DEFAULT_DIRECT_PORTS.copy()
        direct_ports.update(parse_port_map(args.direct_port_map))
        if args.gateway:
            runtime_mode = "gateway"
            runtime_targets, semantic_target = build_gateway_targets(args.gateway, suppliers.keys())
        else:
            runtime_mode = "direct"
            runtime_targets, semantic_target = build_direct_targets(
                suppliers.keys(),
                args.direct_base_host,
                direct_ports,
                args.aas_repository_path,
            )
    run_pipeline(
        suppliers=suppliers,
        out_dir=args.out_dir,
        registry=args.registry,
        upload=args.upload,
        verify_only=args.verify_only,
        runtime_targets=runtime_targets,
        semantic_target=semantic_target,
        semantic_url=args.semantic_url,
        upload_semantic=args.upload_semantic,
        clean_before_upload=args.clean_before_upload,
        runtime_mode=runtime_mode,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
