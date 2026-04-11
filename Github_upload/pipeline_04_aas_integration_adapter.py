#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

sys.modules.setdefault('pipeline_04_aas_integration_adapter', sys.modules[__name__])

AAS_NAMESPACE_OPTIONS = {
    'aas2': 'http://www.admin-shell.io/aas/2/0',
    'aas3': 'https://admin-shell.io/aas/3/0',
}
DEFAULT_AAS_NAMESPACE = 'aas3'
AAS_NS = AAS_NAMESPACE_OPTIONS[DEFAULT_AAS_NAMESPACE]
XLINK_NS = 'http://www.w3.org/1999/xlink'
OPC_CONTENT_TYPES_NS = 'http://schemas.openxmlformats.org/package/2006/content-types'
UTF8_BOM = b"\xef\xbb\xbf"
NS = {'aas': AAS_NS}
ET.register_namespace('', AAS_NS)
ET.register_namespace('xlink', XLINK_NS)
PROJECT_ROOT = Path(__file__).resolve().parent
PHI_SYMBOL = "\u03a6"

CONCEPT_KEYWORDS = {
    'maxOperatingPressure': ['max operating pressure', 'maximum operating pressure', 'max outlet pressure'],
    'ratedFlowRate': ['rated flow rate', 'nominal flow rate', 'required flow rate', 'max flow rate'],
    'hydraulicFluid': ['hydraulic fluid', 'fluid grade', 'fluid type', 'hlp'],
    'cylinderLoad': ['cylinder load', 'load', 'force', 'mass', 'advancing force'],
    'cylinderSpeed': ['cylinder speed', 'linear speed'],
    'cylinderStroke': ['stroke', 'stroke length'],
    'motorTorque': ['torque'],
    'motorSpeed': ['rotational speed', 'rpm', 'speed of the drive'],
    'tankLevelMax': ['tank level max', 'maximum tank level'],
    'tankLevelMin': ['tank level min', 'minimum tank level'],
    'tankNominalVolume': ['tank volume', 'nominal volume', 'required volume'],
    'prvSetpoint': ['pressure relief', 'relief setpoint', 'pressure protection', 'cracking pressure'],
    'accNominalVolume': ['accumulator volume', 'bladder accumulator volume', 'nominal volume'],
    'accPreChargePressure': ['pre charge pressure', 'precharge pressure', 'prechargepressure'],
    'accBARequirements': ['accumulator', 'bladder accumulator', 'accumulator volume', 'pre charge pressure', 'precharge pressure'],
}

DEFAULT_BASYX_SUPPLIERS = {
    'SupplierA': 'http://localhost:8091',
    'SupplierB': 'http://localhost:8092',
    'SupplierC': 'http://localhost:8093',
    'SupplierD': 'http://localhost:8094',
    'SupplierE': 'http://localhost:8095',
}

PORT_SUPPLIER_NAMES = {
    8091: 'SupplierA',
    8092: 'SupplierB',
    8093: 'SupplierC',
    8094: 'SupplierD',
    8095: 'SupplierE',
}

CYL_FORCE_IRDI = '0173-1#02-AAZ980#003'
CYL_STROKE_IRDI = '0173-1#02-ABC413#003'
TANK_VOLUME_IRDI = '0173-1#02-AAR733#005'
TANK_PRESSURE_IRDI = '0173-1#02-ABC510#003'
PUMP_FLOW_IRDI = '0173-1#02-ABC551#003'
PUMP_PRESSURE_IRDI = '0173-1#02-ABC510#003'
CYL_PRESSURE_IRDI = '0173-1#02-AAZ943#003'
DCV_FLOW_IRDI = '0173-1#02-ABC551#003'
DCV_PRESSURE_IRDI = '0173-1#02-AAZ943#003'
PRV_CRACKING_IRDI = '0173-1#02-ABC527#003'
CHECK_VALVE_CRACKING_IRDI = '0173-1#02-ABC527#003'
ACC_PRECHARGE_SEM = 'urn:sdf:cd:hydraulic:PreChargePressure:1.1'
GENERIC_PRESSURE_IRDI = '0173-1#02-AAZ943#003'
GENERIC_FLOW_IRDI = '0173-1#02-ABC551#003'
ROD_DIAMETER_IRDI = '0173-1#02-BAA151#006'
SYMBOL_KEY_SEMANTIC_ID = 'urn:sdf:hydraulic:SchematicSymbolId:1.0'
TOPOLOGY_MODES = ('qa_skeleton', 'volume_node_demo')

MAPPING_COMPONENT_TYPE_MAP = {
    'Verstell_Pumpe': 'VariablePump',
    'Differential_Zylinder': 'Double-ActingCylinder',
    'Druckbegrenzungsventil': 'PressureReliefValve',
    'Druckspeicher': 'BladderAccumulator',
}

MAPPING_PORT_KEY_MAP = {
    'Verstell_Pumpe': {'VK_ein': 'S', 'VK_aus': 'P'},
    'Differential_Zylinder': {'VK_a': 'A', 'VK_b': 'B'},
    'Druckbegrenzungsventil': {'VK_ein': 'P', 'VK_aus': 'T'},
    'Druckspeicher': {'VK_p': 'P'},
}


@dataclass
class Constraint:
    semantic_id: str
    property_label: Optional[str]
    operator: str
    value: Optional[float] = None
    unit: Optional[str] = None
    confidence: float = 0.0
    evidence: Optional[str] = None
    value_text: Optional[str] = None
    concept: Optional[str] = None
    defaulted_operator: bool = False

@dataclass
class SemanticRecord:
    semantic_id: str
    raw_value: str
    numeric_value: Optional[float]
    raw_unit: Optional[str]
    unit: Optional[str]
    submodel_id_short: str
    path: str

@dataclass
class ComponentAAS:
    aas_id: str
    asset_type: str
    component_type: str
    aas_file: str
    supplier: Optional[str] = None
    id_short: Optional[str] = None
    submodels_raw: List[Dict[str, Any]] = field(default_factory=list)
    aasx_xml_root: Optional[ET.Element] = None
    technical_properties: Dict[str, float] = field(default_factory=dict)
    raw_properties: Dict[str, str] = field(default_factory=dict)
    interface_specs: Dict[str, str] = field(default_factory=dict)
    port_role_specs: Dict[str, str] = field(default_factory=dict)
    semantic_records: List[SemanticRecord] = field(default_factory=list)
    semantic_index: Dict[str, List[SemanticRecord]] = field(default_factory=dict)
    symbol_id: Optional[str] = None

@dataclass
class IRDIRegistry:
    semantic_to_label: Dict[str, str] = field(default_factory=dict)
    semantic_to_concept: Dict[str, str] = field(default_factory=dict)
    semantic_to_description: Dict[str, str] = field(default_factory=dict)

IRDI_REGISTRY: Optional[IRDIRegistry] = None
IRDI_WARNED: set[str] = set()
KNOWN_AAS_SEMANTIC_IDS: set[str] = set()

def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def load_prompt_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")

def render_prompt(template: str, **kwargs: str) -> str:
    result = template
    for key, value in kwargs.items():
        result = result.replace(f"{{{{{key}}}}}", value)
    return result

def _xlsx_col_to_index(col_name: str) -> int:
    value = 0
    for ch in col_name:
        if "A" <= ch <= "Z":
            value = value * 26 + (ord(ch) - ord("A") + 1)
    return value

def read_mapping_xlsx_rows(path: Path, sheet_name: Optional[str] = None) -> Tuple[str, List[Dict[str, str]]]:
    ns_main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    ns_doc_rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    ns_pkg_rel = "http://schemas.openxmlformats.org/package/2006/relationships"

    if not path.exists():
        raise FileNotFoundError(f"Mapping XLSX not found: {path}")

    with zipfile.ZipFile(path) as archive:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for si in shared_root.findall(f"{{{ns_main}}}si"):
                text = "".join(node.text or "" for node in si.findall(f".//{{{ns_main}}}t"))
                shared_strings.append(text)

        workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
        rel_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rid_to_target = {
            rel.attrib.get("Id", ""): rel.attrib.get("Target", "")
            for rel in rel_root.findall(f"{{{ns_pkg_rel}}}Relationship")
        }

        sheet_defs: List[Tuple[str, str]] = []
        for sheet in workbook_root.findall(f".//{{{ns_main}}}sheet"):
            name = sheet.attrib.get("name", "")
            rid = sheet.attrib.get(f"{{{ns_doc_rel}}}id", "")
            target = rid_to_target.get(rid, "")
            if target and not target.startswith("xl/"):
                target = f"xl/{target}"
            sheet_defs.append((name, target))
        if not sheet_defs:
            raise RuntimeError(f"No sheets found in mapping file: {path}")

        if sheet_name:
            selected_name, selected_target = next(
                ((name, target) for name, target in sheet_defs if name == sheet_name),
                ("", ""),
            )
            if not selected_target:
                raise RuntimeError(f"Sheet '{sheet_name}' not found in {path}.")
        else:
            selected_name, selected_target = sheet_defs[0]
        if not selected_target:
            raise RuntimeError(f"Sheet target missing for '{selected_name}' in {path}.")

        sheet_root = ET.fromstring(archive.read(selected_target))
        row_map: Dict[int, Dict[int, str]] = {}
        for cell in sheet_root.findall(f".//{{{ns_main}}}sheetData/{{{ns_main}}}row/{{{ns_main}}}c"):
            ref = cell.attrib.get("r", "")
            match = re.match(r"([A-Z]+)(\d+)", ref)
            if not match:
                continue
            col_idx = _xlsx_col_to_index(match.group(1))
            row_idx = int(match.group(2))

            cell_type = cell.attrib.get("t", "")
            value = ""
            if cell_type == "s":
                raw = cell.find(f"{{{ns_main}}}v")
                if raw is not None and raw.text:
                    ss_idx = int(raw.text)
                    if 0 <= ss_idx < len(shared_strings):
                        value = shared_strings[ss_idx]
            elif cell_type == "inlineStr":
                value = "".join(node.text or "" for node in cell.findall(f".//{{{ns_main}}}t"))
            else:
                raw = cell.find(f"{{{ns_main}}}v")
                if raw is not None and raw.text is not None:
                    value = raw.text
            row_map.setdefault(row_idx, {})[col_idx] = value.strip()

    if not row_map:
        return selected_name, []

    header_row_idx = min(row_map.keys())
    headers = row_map.get(header_row_idx, {})
    col_indexes = sorted(headers.keys())
    header_names = [headers[idx].strip() for idx in col_indexes]
    rows: List[Dict[str, str]] = []
    for row_idx in sorted(row_map.keys()):
        if row_idx == header_row_idx:
            continue
        row_cells = row_map[row_idx]
        row: Dict[str, str] = {}
        for col_idx, header in zip(col_indexes, header_names):
            if not header:
                continue
            row[header] = row_cells.get(col_idx, "").strip()
        if any(value for value in row.values()):
            rows.append(row)
    return selected_name, rows

def validate_stage1_v2_1(qa_tree: Dict[str, Any]) -> None:
    stage1 = qa_tree.get("stage1", {})
    nodes = stage1.get("nodes", [])
    node_map = {node.get("id"): node for node in nodes}
    required_ids = [
        "S1_CONTROL_PRINCIPLE",
        "S2V_PRESSURE_CONCEPT",
        "S2P_CIRCUIT_TYPE",
        "S3_CONTROL_STRATEGY",
        "S0_CYLINDER_TYPE",
    ]
    for node_id in required_ids:
        if node_id not in node_map:
            raise RuntimeError(f"Stage1 missing required node id: {node_id}")
    if not nodes or nodes[0].get("id") != "S1_CONTROL_PRINCIPLE":
        raise RuntimeError("Stage1 must start with S1_CONTROL_PRINCIPLE.")

    def check_next(node_id: str, expected_next: str) -> None:
        node = node_map[node_id]
        for route in node.get("routing", []):
            nxt = route.get("next")
            if nxt != expected_next:
                raise RuntimeError(
                    f"Stage1 node {node_id} routing must go to {expected_next}, got {nxt}."
                )

    s1 = node_map["S1_CONTROL_PRINCIPLE"]
    s1_nexts = {route.get("when", {}).get("control_principle"): route.get("next") for route in s1.get("routing", [])}
    if s1_nexts.get("valve_controlled") != "S2V_PRESSURE_CONCEPT":
        raise RuntimeError("Stage1 valve_controlled must route to S2V_PRESSURE_CONCEPT.")
    if s1_nexts.get("pump_controlled") != "S2P_CIRCUIT_TYPE":
        raise RuntimeError("Stage1 pump_controlled must route to S2P_CIRCUIT_TYPE.")

    check_next("S2V_PRESSURE_CONCEPT", "S0_CYLINDER_TYPE")
    check_next("S2P_CIRCUIT_TYPE", "S3_CONTROL_STRATEGY")
    check_next("S3_CONTROL_STRATEGY", "S0_CYLINDER_TYPE")

    s0 = node_map["S0_CYLINDER_TYPE"]
    select_routes = [route for route in s0.get("routing", []) if route.get("select")]
    if len(select_routes) != 32:
        raise RuntimeError(f"Stage1 leaf count must be 32; got {len(select_routes)}.")

def validate_config_consistency(
    qa_tree: Dict[str, Any],
    skeleton_library: Dict[str, Any],
    qa_path: Path,
    library_path: Path,
) -> None:
    skeleton_ids = {s.get("skeletonId") for s in skeleton_library.get("skeletons", [])}
    index_ids = set(qa_tree.get("skeletonIndex", {}).keys())
    missing = sorted(sid for sid in index_ids if sid not in skeleton_ids)
    if missing:
        raise RuntimeError(f"QA tree references unknown skeletonIds: {missing}")
    for node in qa_tree.get("stage1", {}).get("nodes", []):
        for route in node.get("routing", []):
            select_id = route.get("select")
            if select_id and select_id not in skeleton_ids:
                raise RuntimeError(f"Stage1 routing selects unknown skeletonId: {select_id}")
    library_ref = qa_tree.get("libraryRef")
    if library_ref and Path(library_ref).name != library_path.name:
        print(
            f"[WARN] QA tree libraryRef={library_ref} does not match loaded file {library_path.name}."
        )
    validate_stage1_v2_1(qa_tree)

def normalize_symbol_key(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None

def set_aas_namespace(choice: str) -> None:
    if choice not in AAS_NAMESPACE_OPTIONS:
        raise ValueError(f"Unknown AAS namespace choice: {choice}")
    global AAS_NS, NS
    AAS_NS = AAS_NAMESPACE_OPTIONS[choice]
    NS = {"aas": AAS_NS}
    ET.register_namespace("", AAS_NS)

def read_xlsx_rows(xlsx_path: Path) -> List[List[Optional[str]]]:
    rows: List[List[Optional[str]]] = []
    if not zipfile.is_zipfile(xlsx_path):
        try:
            import xlrd  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                f"Excel reader required for {xlsx_path}. Install xlrd to read .xls files."
            ) from exc
        workbook = xlrd.open_workbook(xlsx_path)
        sheet = workbook.sheet_by_index(0)
        for r in range(sheet.nrows):
            row_values: List[Optional[str]] = []
            for c in range(sheet.ncols):
                value = sheet.cell_value(r, c)
                row_values.append(str(value) if value is not None else None)
            rows.append(row_values)
        return rows
    with zipfile.ZipFile(xlsx_path, "r") as zf:
        sheet_names = [
            name for name in zf.namelist() if name.startswith("xl/worksheets/") and name.endswith(".xml")
        ]
        if not sheet_names:
            return rows
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            shared_xml = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            shared_strings = [
                node.text or ""
                for node in shared_xml.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
            ]
        sheet_xml = ET.fromstring(zf.read(sheet_names[0]))
        ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        for row in sheet_xml.findall(".//s:row", ns):
            values: List[Optional[str]] = []
            for cell in row.findall("s:c", ns):
                cell_type = cell.get("t")
                value_node = cell.find("s:v", ns)
                if value_node is None:
                    values.append(None)
                    continue
                value_text = value_node.text or ""
                if cell_type == "s":
                    index = int(value_text)
                    values.append(shared_strings[index] if index < len(shared_strings) else "")
                else:
                    values.append(value_text)
            rows.append(values)
    return rows

def load_irdi_registry(path: Path) -> IRDIRegistry:
    rows = read_xlsx_rows(path)
    if not rows:
        raise RuntimeError(f"IRDI registry is empty: {path}")
    header = [str(cell or "").strip().lower() for cell in rows[0]]
    idx_irdi = None
    idx_label = None
    idx_concept = None
    idx_description = None
    for idx, name in enumerate(header):
        if name in {"irdi", "semanticid", "semantic id", "semantic_id"}:
            idx_irdi = idx
        if name in {"name", "label", "canonlabel", "canonical label", "canonical_label"}:
            idx_label = idx
        if name in {"concept", "conceptid", "concept id"}:
            idx_concept = idx
        if name in {"description", "desc"}:
            idx_description = idx

    registry = IRDIRegistry()
    for row in rows[1:]:
        if idx_irdi is None or idx_irdi >= len(row):
            continue
        semantic_id = str(row[idx_irdi] or "").strip()
        if not semantic_id:
            continue
        label = ""
        concept = ""
        description = ""
        if idx_label is not None and idx_label < len(row):
            label = str(row[idx_label] or "").strip()
        if idx_concept is not None and idx_concept < len(row):
            concept = str(row[idx_concept] or "").strip()
        if idx_description is not None and idx_description < len(row):
            description = str(row[idx_description] or "").strip()
        if semantic_id not in registry.semantic_to_label:
            registry.semantic_to_label[semantic_id] = label
            registry.semantic_to_concept[semantic_id] = concept
            registry.semantic_to_description[semantic_id] = description
    return registry

def resolve_irdi_registry_path(root_dir: Path) -> Path:
    xlsx_path = root_dir / "EClassIRDI.xlsx"
    if xlsx_path.exists():
        return xlsx_path
    raise FileNotFoundError(f"EClassIRDI.xlsx not found under {root_dir}")

def validate_semantic_id(semantic_id: str, context: str) -> None:
    if not semantic_id:
        return
    if semantic_id in KNOWN_AAS_SEMANTIC_IDS:
        return
    registry = IRDI_REGISTRY
    if registry is None:
        return
    if semantic_id in registry.semantic_to_label:
        return
    if semantic_id in IRDI_WARNED:
        return
    IRDI_WARNED.add(semantic_id)
    print(f"[IRDI] UNKNOWN semanticId={semantic_id} used in {context}")


def register_known_semantic_ids(semantic_ids: List[str]) -> None:
    for semantic_id in semantic_ids:
        text = str(semantic_id or "").strip()
        if text:
            KNOWN_AAS_SEMANTIC_IDS.add(text)

def fetch_json(url: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            data = response.read().decode("utf-8", errors="replace")
        return json.loads(data)
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
        print(f"[WARN] Failed to fetch JSON from {url}: {exc}")
        return None

def basyx_encode_id(identifier: str) -> str:
    return base64.urlsafe_b64encode(identifier.encode("utf-8")).decode("ascii").rstrip("=")

def extract_result_list(payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "result" in payload:
        result = payload.get("result")
        return result if isinstance(result, list) else []
    return []

def parse_supplier_list(value: str) -> Dict[str, str]:
    entries = [item.strip() for item in value.split(",") if item.strip()]
    suppliers: Dict[str, str] = {}
    for index, entry in enumerate(entries):
        if "=" in entry:
            name, url = entry.split("=", 1)
            suppliers[name.strip()] = url.strip()
            continue
        url = entry
        parsed = urllib.parse.urlparse(url)
        name = PORT_SUPPLIER_NAMES.get(parsed.port, f"Supplier{index + 1}")
        suppliers[name] = url
    return suppliers

def parse_submodel_reference_ids(shell: Dict[str, Any]) -> List[str]:
    refs = shell.get("submodels") or shell.get("submodelRefs") or []
    ids: List[str] = []
    for ref in refs:
        keys = ref.get("keys", [])
        if not keys:
            continue
        last_key = keys[-1]
        if isinstance(last_key, dict):
            submodel_id = last_key.get("value")
        elif isinstance(last_key, str):
            submodel_id = parse_semantic_id_value(last_key)
        else:
            submodel_id = None
        if submodel_id:
            ids.append(submodel_id)
    return ids

def extract_semantic_id(element: Dict[str, Any]) -> str:
    semantic = element.get("semanticId")
    return extract_semantic_id_from_ref(semantic)

def parse_semantic_id_value(value: str) -> str:
    text = value.strip()
    match = re.search(r"value=([^;}]*)", text)
    if match:
        return match.group(1).strip()
    return text

def extract_semantic_id_from_ref(ref: Any) -> str:
    if ref is None:
        return ""
    if isinstance(ref, str):
        return parse_semantic_id_value(ref)
    if isinstance(ref, dict):
        keys = ref.get("keys") or []
        for key in keys:
            if isinstance(key, dict):
                value = key.get("value")
                if isinstance(value, str) and value.strip():
                    return value.strip()
            elif isinstance(key, str) and key.strip():
                parsed = parse_semantic_id_value(key)
                if parsed:
                    return parsed
        value = ref.get("value")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""

def walk_submodel_elements(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    if not isinstance(elements, list):
        return collected
    for element in elements:
        if not isinstance(element, dict):
            continue
        collected.append(element)
        model_type = get_model_type_name(element)
        if model_type in {"SubmodelElementCollection", "SubmodelElementList"}:
            child_elements = element.get("value")
            if isinstance(child_elements, list):
                collected.extend(walk_submodel_elements(child_elements))
    return collected

def collect_elements_with_paths(elements: Any, base_path: str) -> List[Tuple[Dict[str, Any], str]]:
    collected: List[Tuple[Dict[str, Any], str]] = []
    if not isinstance(elements, list):
        return collected
    for index, element in enumerate(elements):
        if not isinstance(element, dict):
            continue
        id_short = str(element.get("idShort") or f"element_{index}")
        path = f"{base_path}/{id_short}" if base_path else id_short
        collected.append((element, path))
        model_type = get_model_type_name(element)
        if model_type in {"SubmodelElementCollection", "SubmodelElementList"}:
            child_elements = element.get("value")
            if isinstance(child_elements, list):
                collected.extend(collect_elements_with_paths(child_elements, path))
    return collected

def extract_value_from_json_element(element: Dict[str, Any]) -> str:
    value = element.get("value")
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value).strip()
    if isinstance(value, dict):
        for key in ("value", "text", "rawValue", "valueText"):
            nested = value.get(key)
            if isinstance(nested, (str, int, float, bool)):
                text = str(nested).strip()
                if text:
                    return text
        lang_items = value.get("langStringTextType")
        if isinstance(lang_items, list):
            for item in lang_items:
                if isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str) and text.strip():
                        return text.strip()
    return ""

def extract_semantic_id_from_xml(element: ET.Element) -> str:
    return element.findtext(
        "aas:semanticId/aas:keys/aas:key/aas:value", default="", namespaces=NS
    ).strip()

def extract_value_from_xml_element(element: ET.Element) -> str:
    value_elem = element.find("aas:value", NS)
    if value_elem is None:
        return ""
    if list(value_elem):
        text_elem = value_elem.find(".//aas:text", NS)
        if text_elem is not None and text_elem.text:
            return text_elem.text.strip()
        return ""
    return (value_elem.text or "").strip()

def iter_xml_submodel_elements(
    elements: List[ET.Element],
    base_path: str,
) -> List[Tuple[ET.Element, str]]:
    collected: List[Tuple[ET.Element, str]] = []
    for element in elements:
        if not isinstance(element.tag, str):
            continue
        id_short = element.findtext("aas:idShort", default="", namespaces=NS)
        tag_name = element.tag.split("}")[-1]
        elem_path = id_short or tag_name
        path = f"{base_path}/{elem_path}" if base_path else elem_path
        collected.append((element, path))
        value_elem = element.find("aas:value", NS)
        if value_elem is not None:
            children = [child for child in list(value_elem) if isinstance(child.tag, str)]
            if children:
                collected.extend(iter_xml_submodel_elements(children, path))
    return collected

def get_property_value_by_semantic_id(
    component_aas: ComponentAAS,
    target_semantic_id: str,
) -> Optional[str]:
    validate_semantic_id(target_semantic_id, "symbol_lookup")
    matches: List[str] = []
    if component_aas.submodels_raw:
        for submodel in component_aas.submodels_raw:
            elements = submodel.get("submodelElements") or []
            submodel_id_short = submodel.get("idShort", "")
            for element, _path in collect_elements_with_paths(elements, submodel_id_short):
                if extract_semantic_id(element) == target_semantic_id:
                    value = extract_value_from_json_element(element)
                    matches.append(value)
    if component_aas.aasx_xml_root is not None:
        root = component_aas.aasx_xml_root
        for submodel in root.findall(".//aas:submodels/aas:submodel", NS):
            submodel_id_short = submodel.findtext("aas:idShort", default="", namespaces=NS)
            elements = submodel.find("aas:submodelElements", NS)
            if elements is None:
                continue
            for element, _path in iter_xml_submodel_elements(list(elements), submodel_id_short or ""):
                if extract_semantic_id_from_xml(element) == target_semantic_id:
                    value = extract_value_from_xml_element(element)
                    matches.append(value)
    if len(matches) > 1:
        component_label = component_aas.id_short or component_aas.aas_id
        print(
            f"[SYMBOL] DUPLICATE semanticId={target_semantic_id} component={component_label} count={len(matches)}"
        )
    non_empty = [normalize_symbol_key(value) for value in matches]
    for value in non_empty:
        if value:
            return value
    return None

def find_element_by_id_short(elements: List[Dict[str, Any]], id_short: str) -> Optional[Dict[str, Any]]:
    for element in elements:
        if element.get("idShort") == id_short:
            return element
    return None

def get_model_type_name(element: Dict[str, Any]) -> str:
    model_type = element.get("modelType")
    if isinstance(model_type, dict):
        return str(model_type.get("name") or "")
    if isinstance(model_type, str):
        return model_type
    return ""

def build_semantic_index(
    records: List[SemanticRecord],
) -> Tuple[Dict[str, float], Dict[str, str], Dict[str, List[SemanticRecord]]]:
    numeric_props: Dict[str, float] = {}
    raw_props: Dict[str, str] = {}
    index: Dict[str, List[SemanticRecord]] = {}
    for record in records:
        if record.semantic_id not in raw_props:
            raw_props[record.semantic_id] = record.raw_value
        if record.numeric_value is not None and record.semantic_id not in numeric_props:
            numeric_props[record.semantic_id] = record.numeric_value
        index.setdefault(record.semantic_id, []).append(record)
    return numeric_props, raw_props, index

def extract_semantic_records_from_submodel_json(submodel: Dict[str, Any]) -> List[SemanticRecord]:
    submodel_id_short = str(submodel.get("idShort") or "")
    records: List[SemanticRecord] = []
    elements = submodel.get("submodelElements") or []
    for element, path in collect_elements_with_paths(elements, submodel_id_short):
        if get_model_type_name(element) != "Property":
            continue
        semantic_id = extract_semantic_id(element)
        if not semantic_id:
            continue
        raw_value = element.get("value")
        raw_text = "" if raw_value is None else str(raw_value)
        raw_unit = extract_unit_from_qualifiers(element.get("qualifiers"))
        if raw_unit is None:
            raw_unit = detect_unit_from_text(raw_text)
        numeric_value = parse_float(raw_text)
        normalized_value = None
        normalized_unit = raw_unit
        if numeric_value is not None:
            normalized_value, normalized_unit = normalize_value(numeric_value, raw_unit)
        records.append(
            SemanticRecord(
                semantic_id=semantic_id,
                raw_value=raw_text,
                numeric_value=normalized_value,
                raw_unit=raw_unit,
                unit=normalized_unit,
                submodel_id_short=submodel_id_short,
                path=path,
            )
        )
    return records

def extract_properties_from_submodel_json(
    submodel: Dict[str, Any],
) -> Tuple[List[SemanticRecord], Dict[str, float], Dict[str, str], Dict[str, List[SemanticRecord]]]:
    numeric_props: Dict[str, float] = {}
    raw_props: Dict[str, str] = {}
    records = extract_semantic_records_from_submodel_json(submodel)
    numeric_props, raw_props, index = build_semantic_index(records)
    return records, numeric_props, raw_props, index

def extract_symbol_id_from_submodel_json(submodel: Dict[str, Any]) -> Optional[str]:
    validate_semantic_id(SYMBOL_KEY_SEMANTIC_ID, "symbol_lookup")
    elements = submodel.get("submodelElements") or []
    for element, _path in collect_elements_with_paths(elements, submodel.get("idShort", "")):
        if extract_semantic_id(element) != SYMBOL_KEY_SEMANTIC_ID:
            continue
        value = extract_value_from_json_element(element)
        if value:
            return value
    return None

def extract_port_role_id_from_port_elements(elements: List[Dict[str, Any]]) -> str:
    role = find_element_by_id_short(elements, "PortRole")
    if not role:
        return ""
    value_id = role.get("valueId")
    role_id = extract_semantic_id_from_ref(value_id)
    if role_id:
        return role_id
    role_id = extract_semantic_id(role)
    if role_id:
        return role_id
    raw_value = role.get("value")
    if isinstance(raw_value, str) and raw_value.strip().startswith("urn:"):
        return raw_value.strip()
    return ""

def extract_interface_specs_from_submodel_json(
    submodel: Dict[str, Any],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    specs: Dict[str, str] = {}
    role_specs: Dict[str, str] = {}
    elements = submodel.get("submodelElements") or []
    all_ports = find_element_by_id_short(elements, "AllPorts")
    if not all_ports:
        return specs, role_specs
    ports = all_ports.get("value") or []
    if not isinstance(ports, list):
        return specs, role_specs
    for port in ports:
        if not isinstance(port, dict):
            continue
        if get_model_type_name(port) != "SubmodelElementCollection":
            continue
        port_elements = port.get("value") or []
        if not isinstance(port_elements, list):
            continue
        role_id = extract_port_role_id_from_port_elements(port_elements)
        svg_port = find_element_by_id_short(port_elements, "SVGPortID")
        port_key = str(svg_port.get("value", "")).strip() if svg_port else ""
        physical = find_element_by_id_short(port_elements, "PhysicalInterface")
        interface_spec = None
        if physical:
            physical_value = physical.get("value")
            if isinstance(physical_value, list):
                interface_spec = find_element_by_id_short(physical_value, "InterfaceSpec")
        spec_value = str(interface_spec.get("value", "")).strip() if interface_spec else ""
        if port_key and spec_value:
            specs[port_key] = spec_value
        if role_id and spec_value:
            role_specs[role_id] = spec_value
    return specs, role_specs

def fetch_submodel(base_url: str, submodel_id: str) -> Optional[Dict[str, Any]]:
    encoded = basyx_encode_id(submodel_id)
    url = f"{base_url}/submodels/{encoded}"
    return fetch_json(url)

def fetch_shells(base_url: str) -> List[Dict[str, Any]]:
    return extract_result_list(fetch_json(f"{base_url}/shells"))

def fetch_submodels(base_url: str) -> List[Dict[str, Any]]:
    return extract_result_list(fetch_json(f"{base_url}/submodels"))

def extract_properties_from_aasx(aasx_path: Path) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    xml_root, _ = read_aasx_xml(aasx_path)
    submodel = xml_root.find(
        ".//aas:submodels/aas:submodel[aas:idShort='TechnicalData']",
        NS,
    )
    if submodel is None:
        return results
    for prop in submodel.findall(".//aas:property", NS):
        semantic_id = prop.findtext(
            "aas:semanticId/aas:keys/aas:key/aas:value", default="", namespaces=NS
        )
        label = prop.findtext("aas:idShort", default="", namespaces=NS)
        if semantic_id and label:
            results.append(
                {
                    "semanticId": semantic_id,
                    "label": label,
                    "componentType": "",
                }
            )
    return results

def find_payload_xml_name(zf: zipfile.ZipFile) -> str:
    candidates = [name for name in zf.namelist() if name.endswith(".aas.xml")]
    if not candidates:
        raise RuntimeError("No .aas.xml payload found in AASX package.")
    return candidates[0]

def read_aasx_xml(aasx_path: Path) -> Tuple[ET.Element, str]:
    with zipfile.ZipFile(aasx_path, "r") as zf:
        xml_name = find_payload_xml_name(zf)
        xml_bytes = zf.read(xml_name)
    root = ET.fromstring(xml_bytes)
    return root, xml_name

def write_aasx_with_updates(
    source_aasx: Path,
    dest_aasx: Path,
    xml_name: str,
    xml_root: ET.Element,
    extra_files: Dict[str, bytes],
) -> None:
    with zipfile.ZipFile(source_aasx, "r") as zin:
        source_xml_bytes = zin.read(xml_name)
        source_has_bom = source_xml_bytes.startswith(UTF8_BOM)
        source_xml_raw = source_xml_bytes[len(UTF8_BOM) :] if source_has_bom else source_xml_bytes
        source_has_decl = source_xml_raw.lstrip().startswith(b"<?xml")
        content_types_name = "[Content_Types].xml"
        updated_content_types: Optional[bytes] = None
        if content_types_name in zin.namelist():
            updated_content_types = update_content_types_xml(
                zin.read(content_types_name),
                extra_files,
            )
        with zipfile.ZipFile(dest_aasx, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if item.filename == xml_name:
                    continue
                if item.filename in extra_files:
                    continue
                if updated_content_types is not None and item.filename == content_types_name:
                    continue
                zout.writestr(item, zin.read(item.filename))
            ET.register_namespace("", AAS_NS)
            ET.register_namespace("xlink", XLINK_NS)
            xml_payload = ET.tostring(xml_root, encoding="utf-8", xml_declaration=source_has_decl)
            if source_has_bom:
                xml_payload = UTF8_BOM + xml_payload
            zout.writestr(xml_name, xml_payload)
            if updated_content_types is not None:
                zout.writestr(content_types_name, updated_content_types)
            for path, payload in extra_files.items():
                zout.writestr(path, payload)

def update_content_types_xml(content_types_bytes: bytes, extra_files: Dict[str, bytes]) -> bytes:
    has_bom = content_types_bytes.startswith(UTF8_BOM)
    raw = content_types_bytes[len(UTF8_BOM) :] if has_bom else content_types_bytes
    has_decl = raw.lstrip().startswith(b"<?xml")
    root = ET.fromstring(raw)
    default_tag = f"{{{OPC_CONTENT_TYPES_NS}}}Default"
    existing_exts = {
        str(elem.get("Extension", "")).strip().lower()
        for elem in root.findall(default_tag)
    }
    ext_to_content_type = {
        "svg": "image/svg+xml",
        "json": "application/json",
    }
    changed = False
    for file_path in extra_files.keys():
        extension = Path(file_path).suffix.strip(".").lower()
        content_type = ext_to_content_type.get(extension)
        if not extension or content_type is None or extension in existing_exts:
            continue
        ET.SubElement(
            root,
            default_tag,
            {"Extension": extension, "ContentType": content_type},
        )
        existing_exts.add(extension)
        changed = True
    if not changed:
        return content_types_bytes
    ET.register_namespace("", OPC_CONTENT_TYPES_NS)
    payload = ET.tostring(root, encoding="utf-8", xml_declaration=has_decl)
    ET.register_namespace("", AAS_NS)
    ET.register_namespace("xlink", XLINK_NS)
    if has_bom:
        payload = UTF8_BOM + payload
    return payload

def validate_aasx_package(aasx_path: Path) -> None:
    with zipfile.ZipFile(aasx_path, "r") as zf:
        names = zf.namelist()
        aas_xmls = [name for name in names if name.endswith(".aas.xml")]
        xml_name = ""
        if aas_xmls:
            system_xml = next(
                (name for name in aas_xmls if Path(name).name.lower().startswith("system")),
                "",
            )
            xml_name = system_xml or aas_xmls[0]
            xml_root = ET.fromstring(zf.read(xml_name))
        else:
            xml_root = None
    key_files: List[str] = []
    if "[Content_Types].xml" in names:
        key_files.append("[Content_Types].xml")
    if aas_xmls:
        key_files.append(xml_name or aas_xmls[0])
    if any(name.startswith("aasx/") for name in names):
        key_files.append("aasx/")
    if "_rels/.rels" in names:
        key_files.append("_rels/.rels")
    if "aasx/_rels/.rels" in names:
        key_files.append("aasx/_rels/.rels")
    print(f"[AASX] Package contents: {', '.join(key_files)}")

    if "[Content_Types].xml" not in names:
        raise RuntimeError("AASX validation failed: missing [Content_Types].xml")
    if not aas_xmls:
        raise RuntimeError("AASX validation failed: missing .aas.xml payload")
    if not any(name.startswith("aasx/") for name in names):
        raise RuntimeError("AASX validation failed: missing aasx/ folder structure")
    rels_files = [name for name in names if name.endswith(".rels")]
    if rels_files and "_rels/.rels" not in names and "aasx/_rels/.rels" not in names:
        raise RuntimeError("AASX validation failed: missing _rels/.rels")

    if xml_root is None:
        raise RuntimeError("AASX validation failed: could not parse .aas.xml payload")
    shells = len(xml_root.findall(".//aas:assetAdministrationShell", NS))
    submodels = len(xml_root.findall(".//aas:submodel", NS))
    cds = len(xml_root.findall(".//aas:conceptDescription", NS))
    print(f"[AASX] Parsed OK: shells={shells}, submodels={submodels}, CDs={cds}")

def get_text(elem: Optional[ET.Element]) -> str:
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()

def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None

def extract_unit_from_qualifiers(qualifiers: Any) -> Optional[str]:
    if not isinstance(qualifiers, list):
        return None
    for qualifier in qualifiers:
        if not isinstance(qualifier, dict):
            continue
        q_type = str(qualifier.get("type", "")).strip().lower()
        if q_type in {"unit", "units"}:
            return str(qualifier.get("value", "")).strip() or None
    return None

def extract_unit_from_qualifiers_xml(element: ET.Element) -> Optional[str]:
    qualifiers = element.find("aas:qualifiers", NS)
    if qualifiers is None:
        return None
    for qualifier in qualifiers.findall("aas:qualifier", NS):
        q_type = qualifier.findtext("aas:type", default="", namespaces=NS).strip().lower()
        if q_type in {"unit", "units"}:
            value = qualifier.findtext("aas:value", default="", namespaces=NS).strip()
            return value or None
    return None

def detect_unit_from_text(text: str) -> Optional[str]:
    lowered = text.lower()
    if "mpa" in lowered:
        return "MPa"
    if "bar" in lowered:
        return "bar"
    if "l/min" in lowered or "lpm" in lowered:
        return "L/min"
    if "m3/h" in lowered or "m^3/h" in lowered or "m³/h" in lowered:
        return "m3/h"
    if "m3" in lowered or "m^3" in lowered or "m³" in lowered:
        return "m3"
    if "mm" in lowered:
        return "mm"
    if "cm" in lowered:
        return "cm"
    if re.search(r"\bm\b", lowered):
        return "m"
    if re.search(r"\bkn\b", lowered):
        return "kN"
    if re.search(r"\bn\b", lowered) or "newton" in lowered:
        return "N"
    if re.search(r"\bkg\b", lowered):
        return "kg"
    if re.search(r"\btonne\b", lowered) or re.search(r"\bton\b", lowered) or re.search(r"\bt\b", lowered):
        return "t"
    if re.search(r"\bl\b", lowered) or "liter" in lowered or "litre" in lowered:
        return "L"
    return None

def normalize_value(value: float, unit: Optional[str]) -> Tuple[float, Optional[str]]:
    if unit is None:
        return value, None
    unit_clean = unit.strip()
    unit_key = unit_clean.lower().replace(" ", "")
    if unit_key in {"mpa"}:
        return value * 10.0, "bar"
    if unit_key in {"kpa"}:
        return value / 100.0, "bar"
    if unit_key in {"bar", "barg"}:
        return value, "bar"
    if unit_key in {"l", "liter", "litre"}:
        return value, "L"
    if unit_key in {"m3", "m^3", "m³"}:
        return value * 1000.0, "L"
    if unit_key in {"l/min", "lpm", "l/minute"}:
        return value, "L/min"
    if unit_key in {"m3/h", "m^3/h", "m³/h"}:
        return value * 1000.0 / 60.0, "L/min"
    if unit_key in {"kn"}:
        return value * 1000.0, "N"
    if unit_key in {"n"}:
        return value, "N"
    if unit_key in {"kg"}:
        return value * 9.81, "N"
    if unit_key in {"t", "ton", "tonne"}:
        return value * 1000.0 * 9.81, "N"
    if unit_key in {"mm"}:
        return value, "mm"
    if unit_key in {"cm"}:
        return value * 10.0, "mm"
    if unit_key in {"m"}:
        return value * 1000.0, "mm"
    return value, unit_clean

def extract_symbol_id(xml_root: ET.Element) -> Optional[str]:
    validate_semantic_id(SYMBOL_KEY_SEMANTIC_ID, "symbol_lookup")
    for submodel in xml_root.findall(".//aas:submodels/aas:submodel", NS):
        submodel_id_short = submodel.findtext("aas:idShort", default="", namespaces=NS)
        elements = submodel.find("aas:submodelElements", NS)
        if elements is None:
            continue
        for element, _path in iter_xml_submodel_elements(list(elements), submodel_id_short):
            if extract_semantic_id_from_xml(element) != SYMBOL_KEY_SEMANTIC_ID:
                continue
            value = extract_value_from_xml_element(element)
            if value:
                return value
    return None

def extract_interface_specs(xml_root: ET.Element) -> Tuple[Dict[str, str], Dict[str, str]]:
    specs: Dict[str, str] = {}
    role_specs: Dict[str, str] = {}
    ports = xml_root.findall(
        ".//aas:submodels/aas:submodel[aas:idShort='HydraulicInterfaces']"
        "/aas:submodelElements/aas:submodelElementCollection[aas:idShort='AllPorts']"
        "/aas:value/aas:submodelElementCollection",
        NS,
    )
    for port in ports:
        svg_port = port.find(
            "aas:value/aas:property[aas:idShort='SVGPortID']/aas:value", NS
        )
        port_key = get_text(svg_port)
        interface_spec = port.find(
            "aas:value/aas:submodelElementCollection[aas:idShort='PhysicalInterface']"
            "/aas:value/aas:property[aas:idShort='InterfaceSpec']/aas:value",
            NS,
        )
        spec_value = get_text(interface_spec)
        role_value_id = port.find(
            "aas:value/aas:property[aas:idShort='PortRole']/aas:valueId/aas:keys/aas:key/aas:value",
            NS,
        )
        role_id = get_text(role_value_id)
        if port_key:
            if spec_value:
                specs[port_key] = spec_value
        if role_id and spec_value:
            role_specs[role_id] = spec_value
    return specs, role_specs

def extract_technical_properties(
    xml_root: ET.Element,
) -> Tuple[List[SemanticRecord], Dict[str, float], Dict[str, str], Dict[str, List[SemanticRecord]]]:
    submodel = xml_root.find(
        ".//aas:submodels/aas:submodel[aas:idShort='TechnicalData']",
        NS,
    )
    if submodel is None:
        return [], {}, {}, {}
    records: List[SemanticRecord] = []
    for prop in submodel.findall(".//aas:property", NS):
        semantic_id = prop.findtext(
            "aas:semanticId/aas:keys/aas:key/aas:value", default="", namespaces=NS
        )
        if not semantic_id:
            continue
        id_short = prop.findtext("aas:idShort", default="", namespaces=NS)
        value_text = prop.findtext("aas:value", default="", namespaces=NS)
        raw_unit = extract_unit_from_qualifiers_xml(prop)
        if raw_unit is None:
            raw_unit = detect_unit_from_text(value_text or "")
        value_num = parse_float(value_text)
        normalized_value = None
        normalized_unit = raw_unit
        if value_num is not None:
            normalized_value, normalized_unit = normalize_value(value_num, raw_unit)
        path = f"TechnicalData/{id_short or 'property'}"
        records.append(
            SemanticRecord(
                semantic_id=semantic_id,
                raw_value=value_text or "",
                numeric_value=normalized_value,
                raw_unit=raw_unit,
                unit=normalized_unit,
                submodel_id_short="TechnicalData",
                path=path,
            )
        )
    numeric_props, raw_props, index = build_semantic_index(records)
    return records, numeric_props, raw_props, index

def parse_component_aas(
    aasx_path: Path, asset_type_map: Dict[str, str]
) -> Optional[ComponentAAS]:
    xml_root, _ = read_aasx_xml(aasx_path)
    shell = xml_root.find(".//aas:assetAdministrationShells/aas:assetAdministrationShell", NS)
    if shell is None:
        return None
    aas_id = shell.findtext("aas:id", default="", namespaces=NS)
    id_short = shell.findtext("aas:idShort", default="", namespaces=NS)
    asset_info = shell.find("aas:assetInformation", NS)
    if asset_info is None:
        return None
    asset_type = asset_info.findtext("aas:assetType", default="", namespaces=NS)
    component_type = asset_type_map.get(asset_type)
    if not component_type:
        return None
    records, tech_props, raw_props, semantic_index = extract_technical_properties(xml_root)
    interfaces, role_specs = extract_interface_specs(xml_root)
    supplier = aasx_path.parent.name
    return ComponentAAS(
        aas_id=aas_id,
        asset_type=asset_type,
        component_type=component_type,
        aas_file=aasx_path.name,
        supplier=supplier,
        id_short=id_short,
        submodels_raw=[],
        aasx_xml_root=xml_root,
        technical_properties=tech_props,
        raw_properties=raw_props,
        interface_specs=interfaces,
        port_role_specs=role_specs,
        semantic_records=records,
        semantic_index=semantic_index,
        symbol_id=None,
    )

def load_components_local(
    data_root: Path, asset_type_map: Dict[str, str]
) -> Dict[str, List[ComponentAAS]]:
    cache: Dict[str, List[ComponentAAS]] = {}
    for aasx_path in sorted(data_root.rglob("*.aasx")):
        component = parse_component_aas(aasx_path, asset_type_map)
        if component is None:
            continue
        cache.setdefault(component.asset_type, []).append(component)
    for components in cache.values():
        components.sort(key=lambda comp: comp.aas_id)
    return cache

def load_components_basyx(
    suppliers: Dict[str, str], asset_type_map: Dict[str, str]
) -> Dict[str, List[ComponentAAS]]:
    cache: Dict[str, List[ComponentAAS]] = {}
    for supplier_name, base_url in suppliers.items():
        shells = fetch_shells(base_url)
        if not shells:
            print(f"[WARN] No shells found from {supplier_name} at {base_url}")
            continue
        submodel_cache: Dict[str, Dict[str, Any]] = {}
        all_submodels: Optional[List[Dict[str, Any]]] = None
        for shell in shells:
            asset_info = shell.get("assetInformation") or {}
            asset_type = asset_info.get("assetType") or ""
            if asset_type not in asset_type_map:
                continue
            component_type = asset_type_map[asset_type]
            aas_id = shell.get("id") or ""
            id_short = shell.get("idShort") or ""
            submodel_ids = parse_submodel_reference_ids(shell)
            submodels_by_idshort: Dict[str, Dict[str, Any]] = {}
            for submodel_id in submodel_ids:
                submodel = submodel_cache.get(submodel_id)
                if submodel is None:
                    submodel = fetch_submodel(base_url, submodel_id)
                    if submodel:
                        submodel_cache[submodel_id] = submodel
                if submodel:
                    submodels_by_idshort[submodel.get("idShort", "")] = submodel

            if not submodels_by_idshort and all_submodels is None:
                all_submodels = fetch_submodels(base_url)
            if all_submodels:
                for submodel in all_submodels:
                    id_short_sm = submodel.get("idShort")
                    if id_short_sm and id_short_sm not in submodels_by_idshort:
                        submodels_by_idshort[id_short_sm] = submodel

            technical = submodels_by_idshort.get("TechnicalData")
            interfaces = submodels_by_idshort.get("HydraulicInterfaces")
            symbol = submodels_by_idshort.get("SymbolKey")
            if not technical and not interfaces and not symbol:
                continue

            records: List[SemanticRecord] = []
            tech_props: Dict[str, float] = {}
            raw_props: Dict[str, str] = {}
            semantic_index: Dict[str, List[SemanticRecord]] = {}
            if technical:
                records, tech_props, raw_props, semantic_index = extract_properties_from_submodel_json(technical)
            interface_specs: Dict[str, str] = {}
            role_specs: Dict[str, str] = {}
            if interfaces:
                interface_specs, role_specs = extract_interface_specs_from_submodel_json(interfaces)
            component = ComponentAAS(
                aas_id=aas_id,
                asset_type=asset_type,
                component_type=component_type,
                aas_file=id_short or aas_id,
                supplier=supplier_name,
                id_short=id_short,
                submodels_raw=list(submodels_by_idshort.values()),
                aasx_xml_root=None,
                technical_properties=tech_props,
                raw_properties=raw_props,
                interface_specs=interface_specs,
                port_role_specs=role_specs,
                semantic_records=records,
                semantic_index=semantic_index,
                symbol_id=None,
            )
            cache.setdefault(asset_type, []).append(component)

    for components in cache.values():
        components.sort(key=lambda comp: comp.aas_id)
    return cache

def build_port_role_map(skeleton: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    role_map: Dict[str, Dict[str, str]] = {}
    for slot in skeleton.get("componentSlots", []):
        slot_id = slot.get("slotId")
        ports = slot.get("ports", [])
        if not slot_id or not isinstance(ports, list):
            continue
        port_roles: Dict[str, str] = {}
        for port in ports:
            if not isinstance(port, dict):
                continue
            port_key = port.get("portKey")
            role_id = port.get("roleId")
            if port_key and role_id:
                port_roles[port_key] = role_id
        if port_roles:
            role_map[slot_id] = port_roles
    return role_map

def get_port_spec(candidate: ComponentAAS, port_key: str, role_id: Optional[str]) -> str:
    if role_id:
        spec = candidate.port_role_specs.get(role_id)
        if spec:
            return spec
    return candidate.interface_specs.get(port_key, "")

def read_schematic_title_block_values(xml_root: ET.Element) -> Dict[str, str]:
    values: Dict[str, str] = {}
    submodel = find_submodel(xml_root, "SchematicLayout")
    if submodel is None:
        return values
    elements = find_submodel_elements(submodel)
    title_block = find_child_by_id_short(elements, "submodelElementCollection", "TitleBlock")
    if title_block is None:
        return values
    container = title_block.find("aas:value", NS)
    if container is None:
        return values

    def read_prop(id_shorts: List[str]) -> str:
        for id_short in id_shorts:
            prop = find_child_by_id_short(container, "property", id_short)
            if prop is None:
                continue
            val = prop.findtext("aas:value", default="", namespaces=NS).strip()
            if val:
                return val
        return ""

    title = read_prop(["Title"])
    drawing_number = read_prop(["DrawingNumber"])
    creator = read_prop(["Creater", "Creator", "CreatedBy"])
    approver = read_prop(["Approver", "ApprovedBy"])

    if title:
        values["Title"] = title
    if drawing_number:
        values["DrawingNumber"] = drawing_number
    if creator:
        values["created_by_name"] = creator
    if approver:
        values["approved_by_name"] = approver
    return values

def find_submodel(xml_root: ET.Element, id_short: str) -> Optional[ET.Element]:
    return xml_root.find(f".//aas:submodels/aas:submodel[aas:idShort='{id_short}']", NS)

def find_submodel_elements(submodel: ET.Element) -> ET.Element:
    elements = submodel.find("aas:submodelElements", NS)
    if elements is None:
        elements = ET.SubElement(submodel, f"{{{AAS_NS}}}submodelElements")
    return elements

def find_child_by_id_short(parent: ET.Element, tag: str, id_short: str) -> Optional[ET.Element]:
    return parent.find(f"aas:{tag}[aas:idShort='{id_short}']", NS)

def ensure_property(
    parent: ET.Element,
    id_short: str,
    value: str,
    value_type: str = "xs:string",
) -> ET.Element:
    prop = find_child_by_id_short(parent, "property", id_short)
    if prop is None:
        prop = ET.SubElement(parent, f"{{{AAS_NS}}}property")
        id_elem = ET.SubElement(prop, f"{{{AAS_NS}}}idShort")
        id_elem.text = id_short
        value_type_elem = ET.SubElement(prop, f"{{{AAS_NS}}}valueType")
        value_type_elem.text = value_type
        value_elem = ET.SubElement(prop, f"{{{AAS_NS}}}value")
        value_elem.text = value
    else:
        value_elem = prop.find("aas:value", NS)
        if value_elem is None:
            value_elem = ET.SubElement(prop, f"{{{AAS_NS}}}value")
        value_elem.text = value
    return prop

def find_property_by_semantic_id(parent: ET.Element, semantic_id: str) -> Optional[ET.Element]:
    for prop in parent.findall("aas:property", NS):
        value = prop.findtext("aas:semanticId/aas:keys/aas:key/aas:value", default="", namespaces=NS)
        if value == semantic_id:
            return prop
    return None

def ensure_property_with_semantic(
    parent: ET.Element,
    id_short: str,
    semantic_id: str,
    value: str,
    value_type: str = "xs:string",
) -> ET.Element:
    validate_semantic_id(semantic_id, "aas_writeback")
    prop = find_property_by_semantic_id(parent, semantic_id)
    if prop is None:
        prop = find_child_by_id_short(parent, "property", id_short)
    if prop is None:
        prop = ET.SubElement(parent, f"{{{AAS_NS}}}property")
        id_elem = ET.SubElement(prop, f"{{{AAS_NS}}}idShort")
        id_elem.text = id_short
        value_type_elem = ET.SubElement(prop, f"{{{AAS_NS}}}valueType")
        value_type_elem.text = value_type
        sem_elem = ET.SubElement(prop, f"{{{AAS_NS}}}semanticId")
        keys_elem = ET.SubElement(sem_elem, f"{{{AAS_NS}}}keys")
        key_elem = ET.SubElement(keys_elem, f"{{{AAS_NS}}}key")
        key_type = ET.SubElement(key_elem, f"{{{AAS_NS}}}type")
        key_type.text = "GlobalReference"
        key_val = ET.SubElement(key_elem, f"{{{AAS_NS}}}value")
        key_val.text = semantic_id
    else:
        sem_val = prop.find("aas:semanticId/aas:keys/aas:key/aas:value", NS)
        if sem_val is None:
            sem_elem = prop.find("aas:semanticId", NS)
            if sem_elem is None:
                sem_elem = ET.SubElement(prop, f"{{{AAS_NS}}}semanticId")
            keys_elem = sem_elem.find("aas:keys", NS)
            if keys_elem is None:
                keys_elem = ET.SubElement(sem_elem, f"{{{AAS_NS}}}keys")
            key_elem = ET.SubElement(keys_elem, f"{{{AAS_NS}}}key")
            key_type = ET.SubElement(key_elem, f"{{{AAS_NS}}}type")
            key_type.text = "GlobalReference"
            key_val = ET.SubElement(key_elem, f"{{{AAS_NS}}}value")
            key_val.text = semantic_id
    value_elem = prop.find("aas:value", NS)
    if value_elem is None:
        value_elem = ET.SubElement(prop, f"{{{AAS_NS}}}value")
    value_elem.text = value
    return prop

def ensure_file_element(
    parent: ET.Element,
    id_short: str,
    value: str,
    content_type: str,
) -> ET.Element:
    file_elem = find_child_by_id_short(parent, "file", id_short)
    if file_elem is None:
        file_elem = ET.SubElement(parent, f"{{{AAS_NS}}}file")
        id_elem = ET.SubElement(file_elem, f"{{{AAS_NS}}}idShort")
        id_elem.text = id_short
    value_elem = file_elem.find("aas:value", NS)
    if value_elem is None:
        value_elem = ET.SubElement(file_elem, f"{{{AAS_NS}}}value")
    value_elem.text = value
    content_elem = file_elem.find("aas:contentType", NS)
    if content_elem is None:
        content_elem = ET.SubElement(file_elem, f"{{{AAS_NS}}}contentType")
    content_elem.text = content_type
    return file_elem

def update_global_constraints(xml_root: ET.Element, constraints: Dict[str, Any]) -> None:
    submodel = find_submodel(xml_root, "SystemRequirements")
    if submodel is None:
        raise RuntimeError("SystemRequirements submodel not found in System AAS.")
    elements = find_submodel_elements(submodel)
    collection = find_child_by_id_short(elements, "submodelElementCollection", "GlobalConstraints")
    if collection is None:
        print("[WARN] GlobalConstraints collection missing; skip updating constraints.")
        return
    value_elem = collection.find("aas:value", NS)
    if value_elem is None:
        print("[WARN] GlobalConstraints value container missing; skip updating constraints.")
        return

    def update_existing_property(id_shorts: List[str], value: str) -> bool:
        for id_short in id_shorts:
            prop = find_child_by_id_short(value_elem, "property", id_short)
            if prop is None:
                continue
            val_elem = prop.find("aas:value", NS)
            if val_elem is None:
                val_elem = ET.SubElement(prop, f"{{{AAS_NS}}}value")
            val_elem.text = value
            return True
        return False

    if "maxOperatingPressure" in constraints:
        value = constraints["maxOperatingPressure"]
        update_existing_property(["MaxOperatingPressure"], f"{value}")
    if "ratedFlowRate" in constraints:
        value = constraints["ratedFlowRate"]
        update_existing_property(["RatedFlowRate", "MinFlowRate"], f"{value}")
    if "hydraulicFluid" in constraints:
        update_existing_property(["HydraulicFluid"], constraints["hydraulicFluid"])
    if "tankNominalVolume" in constraints:
        value = constraints["tankNominalVolume"]
        update_existing_property(["TankNominalVolume"], f"{value}")
    if "prvSetpoint" in constraints:
        value = constraints["prvSetpoint"]
        update_existing_property(["PressureProtectionSetpoint"], f"{value}")

def update_nlp_result_file(xml_root: ET.Element, file_path: str) -> None:
    submodel = find_submodel(xml_root, "SystemRequirements")
    if submodel is None:
        raise RuntimeError("SystemRequirements submodel not found in System AAS.")
    elements = find_submodel_elements(submodel)

    def normalized(value: str) -> str:
        return re.sub(r"[\s_-]+", "", value or "").strip().lower()

    candidate_names = {"outputfile", "auditfile"}
    file_elem: Optional[ET.Element] = None
    for elem in elements.iter():
        if elem.tag.split("}")[-1] != "file":
            continue
        id_short = elem.findtext("aas:idShort", default="", namespaces=NS)
        if normalized(id_short) in candidate_names:
            file_elem = elem
            break
    if file_elem is None:
        print("[WARN] No audit/output file element found in SystemRequirements; skip audit file write-back.")
        return

    value_elem_file = file_elem.find("aas:value", NS)
    if value_elem_file is None:
        value_elem_file = ET.SubElement(file_elem, f"{{{AAS_NS}}}value")
    value_elem_file.text = file_path
    content_elem = file_elem.find("aas:contentType", NS)
    if content_elem is None:
        content_elem = ET.SubElement(file_elem, f"{{{AAS_NS}}}contentType")
    content_elem.text = "application/json"

def update_schematic_layout(xml_root: ET.Element, file_path: str) -> None:
    submodel = find_submodel(xml_root, "SchematicLayout")
    if submodel is None:
        raise RuntimeError("SchematicLayout submodel not found in System AAS.")
    elements = find_submodel_elements(submodel)
    file_elem = find_child_by_id_short(elements, "file", "Diagram")
    if file_elem is None:
        print("[WARN] Diagram file element missing; skip diagram write-back.")
        return
    value_elem = file_elem.find("aas:value", NS)
    if value_elem is None:
        value_elem = ET.SubElement(file_elem, f"{{{AAS_NS}}}value")
    value_elem.text = file_path
    content_elem = file_elem.find("aas:contentType", NS)
    if content_elem is None:
        content_elem = ET.SubElement(file_elem, f"{{{AAS_NS}}}contentType")
    content_elem.text = "image/svg+xml"

def get_system_aas_id(xml_root: ET.Element) -> str:
    shell = xml_root.find(".//aas:assetAdministrationShells/aas:assetAdministrationShell", NS)
    if shell is None:
        return ""
    return shell.findtext("aas:id", default="", namespaces=NS).strip()

def _set_model_reference_target(ref_elem: ET.Element, target_id: str) -> None:
    type_elem = ref_elem.find("aas:type", NS)
    if type_elem is None:
        type_elem = ET.SubElement(ref_elem, f"{{{AAS_NS}}}type")
    type_elem.text = "ModelReference"

    keys = ref_elem.find("aas:keys", NS)
    if keys is None:
        keys = ET.SubElement(ref_elem, f"{{{AAS_NS}}}keys")
    key = keys.find("aas:key", NS)
    if key is None:
        key = ET.SubElement(keys, f"{{{AAS_NS}}}key")
    key_type = key.find("aas:type", NS)
    if key_type is None:
        key_type = ET.SubElement(key, f"{{{AAS_NS}}}type")
    key_type.text = "GlobalReference"
    key_value = key.find("aas:value", NS)
    if key_value is None:
        key_value = ET.SubElement(key, f"{{{AAS_NS}}}value")
    key_value.text = target_id

def update_haspart_relationships(xml_root: ET.Element, system_aas_id: str, aas_ids: List[str]) -> None:
    submodel = find_submodel(xml_root, "HierarchicalStructures")
    if submodel is None:
        raise RuntimeError("HierarchicalStructures submodel not found in System AAS.")
    if not system_aas_id:
        raise RuntimeError("System AAS id not found; cannot write HasPart.first reference.")
    entry = submodel.find("aas:submodelElements/aas:entity[aas:idShort='EntryNode']", NS)
    if entry is None:
        raise RuntimeError("EntryNode entity not found in HierarchicalStructures.")
    statements = entry.find("aas:statements", NS)
    if statements is None:
        statements = ET.SubElement(entry, f"{{{AAS_NS}}}statements")

    template = statements.find("aas:relationshipElement[aas:idShort='HasPart']", NS)
    if template is None:
        template = ET.SubElement(statements, f"{{{AAS_NS}}}relationshipElement")
        id_elem = ET.SubElement(template, f"{{{AAS_NS}}}idShort")
        id_elem.text = "HasPart"
        ET.SubElement(template, f"{{{AAS_NS}}}first")
        ET.SubElement(template, f"{{{AAS_NS}}}second")

    existing = statements.findall("aas:relationshipElement[aas:idShort='HasPart']", NS)
    while len(existing) < len(aas_ids):
        clone = ET.fromstring(ET.tostring(template, encoding="utf-8"))
        statements.append(clone)
        existing.append(clone)

    for rel in existing:
        first = rel.find("aas:first", NS)
        if first is None:
            first = ET.SubElement(rel, f"{{{AAS_NS}}}first")
        _set_model_reference_target(first, system_aas_id)

    for rel, aas_id in zip(existing, aas_ids):
        second = rel.find("aas:second", NS)
        if second is None:
            second = ET.SubElement(rel, f"{{{AAS_NS}}}second")
        _set_model_reference_target(second, aas_id)


DIRECT_CONSTRAINT_META: Dict[str, Tuple[str, str, str]] = {
    "maxOperatingPressure": (GENERIC_PRESSURE_IRDI, "MaxOperatingPressure", "ge"),
    "ratedFlowRate": (GENERIC_FLOW_IRDI, "RatedFlowRate", "ge"),
    "hydraulicFluid": ("urn:sdf:cd:hydraulic:HydraulicFluid:1.0", "HydraulicFluid", "eq"),
    "tankNominalVolume": (TANK_VOLUME_IRDI, "NominalVolume", "ge"),
    "tankLevelMax": ("urn:sdf:cd:hydraulic:TankLevelMax:1.0", "TankLevelMax", "ge"),
    "tankLevelMin": ("urn:sdf:cd:hydraulic:TankLevelMin:1.0", "TankLevelMin", "ge"),
    "prvSetpoint": (PRV_CRACKING_IRDI, "NominalCrackingPressure", "eq"),
    "accNominalVolume": (TANK_VOLUME_IRDI, "NominalVolume", "ge"),
    "accPreChargePressure": (ACC_PRECHARGE_SEM, "PreChargePressure", "ge"),
    "cylinderLoad": (CYL_FORCE_IRDI, "RatedCylinderAdvancingForce", "ge"),
    "cylinderStroke": (CYL_STROKE_IRDI, "RatedStroke", "ge"),
}


def build_default_paths() -> Dict[str, Path]:
    return {
        "qa_tree": PROJECT_ROOT / "skeleton" / "qa_tree_v2_1.json",
        "skeleton_library": PROJECT_ROOT / "skeleton" / "skeleton_library_v2_1.json",
        "summary": PROJECT_ROOT / "supplier_runtime" / "technical_properties_summary.json",
        "data_root": PROJECT_ROOT / "supplier_runtime" / "data",
        "symbol_map": PROJECT_ROOT / "references" / "SymbolHashing.xlsx",
        "drawing_template": PROJECT_ROOT / "templates" / "A4_drawing.svg",
        "mapping_xlsx": PROJECT_ROOT / "references" / "Mapping_of_components_and_their_ports.xlsx",
        "irdi_registry": PROJECT_ROOT / "references" / "EClassIRDI.xlsx",
        "sample_system_aasx": PROJECT_ROOT / "samples" / "system_aas" / "test_sys_1.aasx",
        "sample_network_xlsx": PROJECT_ROOT / "samples" / "network_inputs" / "volume_node_demo.xlsx",
        "sample_network_json": PROJECT_ROOT / "samples" / "network_inputs" / "volume_node_demo.json",
    }


def load_component_port_semantics_from_mapping(path: Path) -> Dict[str, Dict[str, Any]]:
    sheet_name, rows = read_mapping_xlsx_rows(path)
    semantics: Dict[str, Dict[str, Any]] = {}
    current_component = ""
    for row in rows:
        component_name = (row.get("Component_Type") or "").strip()
        if component_name:
            current_component = component_name
        if not current_component:
            continue
        port_id = (row.get("Port_Id") or "").strip()
        connection_type = (row.get("Connection_Type") or "").strip()
        entry = semantics.setdefault(
            current_component,
            {"componentName": current_component, "sheet": sheet_name, "ports": []},
        )
        if port_id:
            existing = {(p.get("portId"), p.get("connectionType")) for p in entry["ports"]}
            key = (port_id, connection_type)
            if key not in existing:
                entry["ports"].append({"portId": port_id, "connectionType": connection_type})
    return semantics


def make_constraint_from_spec(concept: str, spec: Dict[str, Any]) -> Constraint:
    semantic_id, property_label, default_operator = DIRECT_CONSTRAINT_META[concept]
    if "value_text" in spec:
        return Constraint(
            semantic_id=semantic_id,
            property_label=property_label,
            operator="eq",
            value_text=str(spec["value_text"]),
            confidence=1.0,
            evidence="direct_constraint_input",
            concept=concept,
        )
    return Constraint(
        semantic_id=semantic_id,
        property_label=property_label,
        operator=str(spec.get("operator", default_operator)),
        value=float(spec["value"]),
        unit=spec.get("unit"),
        confidence=1.0,
        evidence="direct_constraint_input",
        concept=concept,
    )


def load_constraints_from_json(
    path: Optional[Path],
) -> Tuple[List[Constraint], Dict[str, Constraint], Dict[str, Optional[float]]]:
    if path is None:
        return [], {}, {}
    payload = load_json(path)
    constraints = [make_constraint_from_spec(concept, spec) for concept, spec in payload.items()]
    global_constraints = {
        constraint.concept: constraint
        for constraint in constraints
        if constraint.concept
        in {
            "maxOperatingPressure",
            "ratedFlowRate",
            "hydraulicFluid",
            "tankNominalVolume",
            "tankLevelMax",
            "tankLevelMin",
            "prvSetpoint",
            "accNominalVolume",
            "accPreChargePressure",
        }
    }
    requirements = {
        concept: constraint.value
        for concept, constraint in ((item.concept, item) for item in constraints)
        if constraint.value is not None
        and concept
        in {
            "maxOperatingPressure",
            "ratedFlowRate",
            "tankNominalVolume",
            "prvSetpoint",
            "accNominalVolume",
            "accPreChargePressure",
            "cylinderLoad",
            "cylinderStroke",
        }
    }
    return constraints, global_constraints, requirements


def build_global_values(global_constraints: Dict[str, Constraint]) -> Dict[str, Any]:
    global_values: Dict[str, Any] = {}
    max_pressure = global_constraints.get("maxOperatingPressure")
    if max_pressure and max_pressure.value is not None:
        global_values["maxOperatingPressure"] = normalize_value(max_pressure.value, max_pressure.unit)[0]
    rated_flow = global_constraints.get("ratedFlowRate")
    if rated_flow and rated_flow.value is not None:
        global_values["ratedFlowRate"] = normalize_value(rated_flow.value, rated_flow.unit)[0]
    hydraulic_fluid = global_constraints.get("hydraulicFluid")
    if hydraulic_fluid and hydraulic_fluid.value_text:
        global_values["hydraulicFluid"] = hydraulic_fluid.value_text
    tank_volume = global_constraints.get("tankNominalVolume")
    if tank_volume and tank_volume.value is not None:
        global_values["tankNominalVolume"] = normalize_value(tank_volume.value, tank_volume.unit)[0]
    prv_setpoint = global_constraints.get("prvSetpoint")
    if prv_setpoint and prv_setpoint.value is not None:
        global_values["prvSetpoint"] = normalize_value(prv_setpoint.value, prv_setpoint.unit)[0]
    return global_values


def build_asset_type_map(library: Dict[str, Any]) -> Dict[str, str]:
    return {entry["assetType"]: entry["componentType"] for entry in library.get("componentCatalog", [])}


def load_runtime_library(
    qa_tree_path: Path,
    skeleton_library_path: Path,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Dict[str, Any]]]:
    qa_tree = load_json(qa_tree_path)
    library = load_json(skeleton_library_path)
    skeletons = {item["skeletonId"]: item for item in library.get("skeletons", [])}
    validate_config_consistency(qa_tree, library, qa_tree_path, skeleton_library_path)
    return qa_tree, library, skeletons


def normalize_binding_port(binding: Dict[str, Any], raw_component_label: str) -> str:
    port_key = str(binding.get("portKey", "")).strip()
    if port_key:
        return port_key
    port_id = str(binding.get("portId", "")).strip()
    if not port_id:
        raise RuntimeError(f"Binding for slot {binding.get('slotId', '')} is missing portKey/portId.")
    raw_map = MAPPING_PORT_KEY_MAP.get(raw_component_label, {})
    normalized = raw_map.get(port_id)
    if not normalized:
        raise RuntimeError(f"Unsupported raw port '{port_id}' for component '{raw_component_label}'.")
    return normalized


def infer_component_label_from_slot_id(slot_id: str) -> str:
    upper = slot_id.strip().upper()
    if upper.startswith("PUMP"):
        return "Verstell_Pumpe"
    if upper.startswith("CYL"):
        return "Differential_Zylinder"
    if upper.startswith("PRV"):
        return "Druckbegrenzungsventil"
    if upper.startswith("ACC"):
        return "Druckspeicher"
    raise RuntimeError(
        f"Cannot infer component label from slot '{slot_id}'. Provide a ComponentLabel/Component_Type column in the network Excel."
    )


def get_row_value(row: Dict[str, str], aliases: List[str]) -> str:
    alias_map = {normalized_key: value for normalized_key, value in ((re.sub(r"[\s_-]+", "", key).lower(), val) for key, val in row.items())}
    for alias in aliases:
        hit = alias_map.get(re.sub(r"[\s_-]+", "", alias).lower(), "")
        if hit:
            return str(hit).strip()
    return ""


def load_network_from_xlsx(path: Path) -> Dict[str, Any]:
    _sheet_name, rows = read_mapping_xlsx_rows(path)
    if not rows:
        raise RuntimeError(f"Network XLSX is empty: {path}")

    slot_labels: Dict[str, str] = {}
    volume_nodes: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        volume_node_id = get_row_value(row, ["VolumeNodeId", "Volume Node", "VN_ID", "NetId"])
        slot_id = get_row_value(row, ["ComponentSlot", "Component Slot", "SlotId", "Node_Id", "NodeId"])
        port_value = get_row_value(row, ["PortKey", "Port_Id", "PortId", "Port"])
        component_label = get_row_value(
            row,
            ["ComponentLabel", "Component_Type", "ComponentType", "RawComponentLabel"],
        )
        if not volume_node_id and not slot_id and not port_value:
            continue
        if not volume_node_id or not slot_id or not port_value:
            raise RuntimeError(
                f"Incomplete network row in {path}: need VolumeNodeId, ComponentSlot, and PortKey/Port_Id."
            )
        if not component_label:
            component_label = infer_component_label_from_slot_id(slot_id)
        slot_labels.setdefault(slot_id, component_label)

        binding: Dict[str, str] = {"slotId": slot_id}
        if len(port_value) <= 3 and port_value.upper() == port_value and "#" not in port_value:
            binding["portKey"] = port_value
        else:
            binding["portId"] = port_value
        volume_nodes.setdefault(volume_node_id, []).append(binding)

    slots = [
        {"slotId": slot_id, "componentLabel": component_label}
        for slot_id, component_label in sorted(slot_labels.items())
    ]
    return {
        "name": path.stem,
        "slots": slots,
        "volumeNodes": [
            {"id": node_id, "bindings": bindings}
            for node_id, bindings in volume_nodes.items()
        ],
    }


def load_network_definition(args: argparse.Namespace) -> Tuple[Dict[str, Any], str]:
    network_xlsx = getattr(args, "network_xlsx", None)
    network_json = getattr(args, "network_json", None)
    if network_xlsx is not None and network_xlsx.exists():
        return load_network_from_xlsx(network_xlsx), str(network_xlsx)
    if network_json is not None and network_json.exists():
        return load_json(network_json), str(network_json)
    raise FileNotFoundError(
        f"No network input found. Expected Excel at {network_xlsx} or JSON at {network_json}."
    )


def build_network_skeleton(
    network_data: Dict[str, Any],
    mapping_semantics: Dict[str, Dict[str, Any]],
    component_catalog: List[Dict[str, Any]],
) -> Dict[str, Any]:
    slots_payload = network_data.get("slots") or network_data.get("componentSlots") or []
    volume_nodes = network_data.get("volumeNodes") or []
    if not slots_payload:
        raise RuntimeError("Network input must contain a non-empty 'slots' or 'componentSlots' list.")
    if not volume_nodes:
        raise RuntimeError("Network input must contain a non-empty 'volumeNodes' list.")

    type_to_asset: Dict[str, str] = {}
    for item in component_catalog:
        component_type = item.get("componentType")
        asset_type = item.get("assetType")
        if component_type and asset_type and component_type not in type_to_asset:
            type_to_asset[component_type] = asset_type

    slot_meta: Dict[str, Dict[str, Any]] = {}
    for slot in slots_payload:
        slot_id = str(slot.get("slotId") or slot.get("nodeId") or "").strip()
        if not slot_id:
            raise RuntimeError("Every network slot requires a slotId or nodeId.")
        input_label = str(slot.get("componentLabel") or slot.get("componentType") or "").strip()
        if not input_label:
            raise RuntimeError(f"Slot '{slot_id}' is missing componentLabel/componentType.")
        raw_label = input_label if input_label in mapping_semantics else ""
        if raw_label:
            normalized_type = MAPPING_COMPONENT_TYPE_MAP.get(raw_label)
            if not normalized_type:
                raise RuntimeError(f"No internal componentType mapping configured for '{raw_label}'.")
        else:
            normalized_type = input_label
            if normalized_type not in type_to_asset:
                raise RuntimeError(
                    f"Unsupported component label '{input_label}'. Use a raw mapping label or a known internal componentType."
                )
        asset_type = type_to_asset.get(normalized_type)
        if not asset_type:
            raise RuntimeError(f"No assetType found for internal componentType '{normalized_type}'.")
        slot_meta[slot_id] = {
            "slotId": slot_id,
            "rawLabel": raw_label,
            "componentType": normalized_type,
            "assetType": asset_type,
            "portKeys": set(),
        }

    standardized_volume_nodes: List[Dict[str, Any]] = []
    for node in volume_nodes:
        node_id = str(node.get("id") or node.get("volumeNodeId") or "").strip()
        bindings = node.get("bindings") or []
        if not node_id or len(bindings) < 2:
            raise RuntimeError(f"Volume node '{node_id or '<missing>'}' needs at least two bindings.")
        std_bindings: List[Dict[str, str]] = []
        for binding in bindings:
            slot_id = str(binding.get("slotId") or binding.get("nodeId") or "").strip()
            if slot_id not in slot_meta:
                raise RuntimeError(f"Binding references unknown slot '{slot_id}'.")
            raw_label = slot_meta[slot_id]["rawLabel"]
            if binding.get("portId"):
                if not raw_label:
                    raise RuntimeError(
                        f"Binding for slot '{slot_id}' uses raw portId without a raw component label mapping."
                    )
                available_ports = {item.get("portId") for item in mapping_semantics[raw_label].get("ports", [])}
                if binding["portId"] not in available_ports:
                    raise RuntimeError(
                        f"Port '{binding['portId']}' is not declared for component '{raw_label}' in the mapping XLSX."
                    )
            port_key = normalize_binding_port(binding, raw_label)
            slot_meta[slot_id]["portKeys"].add(port_key)
            std_bindings.append({"slotId": slot_id, "portKey": port_key})
        standardized_volume_nodes.append({"id": node_id, "bindings": std_bindings})

    component_slots = []
    for slot_id, meta in slot_meta.items():
        component_slots.append(
            {
                "slotId": slot_id,
                "componentType": meta["componentType"],
                "assetType": meta["assetType"],
                "ports": [
                    {
                        "portKey": port_key,
                        "roleId": f"urn:sdf:cv:hydraulic:PortRole:{slot_id}_{port_key}",
                    }
                    for port_key in sorted(meta["portKeys"])
                ],
            }
        )

    connections: List[Dict[str, Any]] = []
    for node in standardized_volume_nodes:
        hub = node["bindings"][0]
        for endpoint in node["bindings"][1:]:
            connections.append(
                {
                    "from": {"slotId": hub["slotId"], "portKey": hub["portKey"]},
                    "to": {"slotId": endpoint["slotId"], "portKey": endpoint["portKey"]},
                    "volumeNodeId": node["id"],
                }
            )

    return {
        "skeletonId": network_data.get("skeletonId") or "external_volume_node_network",
        "name": network_data.get("name") or "External volume-node topology",
        "componentSlots": sorted(component_slots, key=lambda item: item["slotId"]),
        "connections": connections,
        "volumeNodes": standardized_volume_nodes,
    }


def ensure_runtime_state(
    irdi_registry_path: Optional[Path] = None,
    export_namespace: str = DEFAULT_AAS_NAMESPACE,
) -> None:
    global IRDI_REGISTRY, KNOWN_AAS_SEMANTIC_IDS
    set_aas_namespace(export_namespace)
    KNOWN_AAS_SEMANTIC_IDS = set()
    registry_path = irdi_registry_path or build_default_paths()["irdi_registry"]
    IRDI_REGISTRY = load_irdi_registry(registry_path)
    print(f"[IRDI] Loaded {len(IRDI_REGISTRY.semantic_to_label)} semantic entries from {registry_path}")


def parse_common_args(parser: argparse.ArgumentParser) -> None:
    defaults = build_default_paths()
    parser.add_argument("--system-aasx", type=Path, help="Path to the System AASX file.")
    parser.add_argument("--output-dir", type=Path, help="Output directory (default: output/run_<timestamp>).")
    parser.add_argument(
        "--source",
        type=str,
        default="basyx",
        choices=["basyx", "local"],
        help="Component source: basyx (recommended default, HTTP) or local (fallback AASX files).",
    )
    parser.add_argument(
        "--basyx-suppliers",
        type=str,
        default=",".join(DEFAULT_BASYX_SUPPLIERS.values()),
        help="Comma-separated supplier base URLs or name=url entries.",
    )
    parser.add_argument("--skeleton-library", type=Path, default=defaults["skeleton_library"])
    parser.add_argument("--qa-tree", type=Path, default=defaults["qa_tree"])
    parser.add_argument("--summary", type=Path, default=defaults["summary"])
    parser.add_argument("--data-root", type=Path, default=defaults["data_root"])
    parser.add_argument("--symbol-map", type=Path, default=defaults["symbol_map"])
    parser.add_argument("--drawing-template", type=Path, default=defaults["drawing_template"])
    parser.add_argument("--mapping-xlsx", type=Path, default=defaults["mapping_xlsx"])
    parser.add_argument("--wire-stroke-width", type=float, default=1.3)
    parser.add_argument("--layout-engine", type=str, default="elk", choices=["elk"])
    parser.add_argument("--allow-fallback", action="store_true")
    parser.add_argument("--elk-direction", type=str, default="UP", choices=["UP", "RIGHT", "DOWN", "LEFT"])
    parser.add_argument("--elk-timeout", type=int, default=30)
    parser.add_argument("--debug-ports", action="store_true")
    parser.add_argument("--debug-elk-json", action="store_true")
    parser.add_argument("--symbol-render-mode", type=str, default="inline", choices=["inline", "image", "auto"])
    parser.add_argument("--symbol-render-image-keys", type=str, default="")
    parser.add_argument("--symbol-render-inline-keys", type=str, default="")
    parser.add_argument(
        "--export-aas-namespace",
        type=str,
        choices=sorted(AAS_NAMESPACE_OPTIONS.keys()),
        default=DEFAULT_AAS_NAMESPACE,
    )
    parser.add_argument("--random-seed", type=int, default=None)


def resolve_system_aasx(path: Optional[Path]) -> Path:
    if path is None:
        return Path(input("Enter the path to the System AASX file:\n> ").strip())
    return path


def resolve_output_dirs(
    output_dir: Optional[Path],
) -> Tuple[Path, Path, Path, Path, str, str]:
    now = dt.datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    diagram_date = now.date().isoformat()
    base = output_dir or (PROJECT_ROOT / "output" / f"run_{timestamp}")
    audit_dir = base / "audit"
    diagrams_dir = base / "diagrams"
    exports_dir = base / "exports"
    audit_dir.mkdir(parents=True, exist_ok=True)
    diagrams_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    return base, audit_dir, diagrams_dir, exports_dir, timestamp, diagram_date


def load_candidate_components(
    args: argparse.Namespace,
    asset_type_map: Dict[str, str],
) -> Dict[str, List[ComponentAAS]]:
    if args.source == "basyx":
        suppliers = parse_supplier_list(args.basyx_suppliers) or DEFAULT_BASYX_SUPPLIERS
        return load_components_basyx(suppliers, asset_type_map)
    return load_components_local(args.data_root, asset_type_map)


def write_pipeline_outputs(
    *,
    system_aasx: Path,
    selected_skeleton: Dict[str, Any],
    selection: Dict[str, ComponentAAS],
    audit: Dict[str, Any],
    global_constraints: Dict[str, Constraint],
    symbol_mapping: Dict[str, Path],
    title_block_values: Dict[str, str],
    output_dirs: Tuple[Path, Path, Path, Path, str, str],
    args: argparse.Namespace,
) -> Dict[str, str]:
    import pipeline_03_automatic_diagram_generation as drawing

    output_dir, audit_dir, diagrams_dir, exports_dir, timestamp, diagram_date = output_dirs
    xml_root, xml_name = read_aasx_xml(system_aasx)
    system_aas_id = get_system_aas_id(xml_root)
    audit_path = audit_dir / f"nlp_audit_{timestamp}.json"
    write_json(audit_path, audit)

    drawing.WIRE_STROKE_WIDTH = f"{args.wire_stroke_width:.3f}".rstrip("0").rstrip(".")
    diagram_svg_path = diagrams_dir / f"circuit_diagram_{timestamp}.svg"
    drawing.generate_diagram_svg(
        args.drawing_template,
        diagram_svg_path,
        selected_skeleton,
        selection,
        symbol_mapping,
        global_constraints,
        args.layout_engine,
        args.allow_fallback,
        args.elk_direction,
        args.elk_timeout,
        args.debug_ports,
        args.debug_elk_json,
        args.symbol_render_mode,
        drawing.parse_symbol_key_csv(args.symbol_render_image_keys),
        drawing.parse_symbol_key_csv(args.symbol_render_inline_keys),
        diagram_date,
        title_block_values,
    )

    update_global_constraints(xml_root, build_global_values(global_constraints))
    audit_zip_path = f"aasx/audit/{audit_path.name}"
    diagram_zip_path = f"aasx/diagrams/{diagram_svg_path.name}"
    update_nlp_result_file(xml_root, audit_zip_path)
    update_schematic_layout(xml_root, diagram_zip_path)
    update_haspart_relationships(xml_root, system_aas_id, [comp.aas_id for comp in selection.values()])

    extra_files = {
        audit_zip_path: audit_path.read_bytes(),
        diagram_zip_path: diagram_svg_path.read_bytes(),
    }
    export_path = exports_dir / f"{system_aasx.stem}_updated_{timestamp}.aasx"
    write_aasx_with_updates(system_aasx, export_path, xml_name, xml_root, extra_files)
    validate_aasx_package(export_path)
    return {
        "outputDir": str(output_dir),
        "auditPath": str(audit_path),
        "diagramSvgPath": str(diagram_svg_path),
        "exportAasxPath": str(export_path),
    }


def run_qa_mode(args: argparse.Namespace) -> int:
    import pipeline_01_nlp_processing as nlp
    import pipeline_02_component_selection as selection_module
    import pipeline_03_automatic_diagram_generation as drawing

    ensure_runtime_state(export_namespace=args.export_aas_namespace)
    system_aasx = resolve_system_aasx(args.system_aasx)
    if not system_aasx.exists():
        raise FileNotFoundError(f"System AASX not found: {system_aasx}")

    output_dirs = resolve_output_dirs(args.output_dir)
    qa_tree, library, skeletons = load_runtime_library(args.qa_tree, args.skeleton_library)
    client = nlp.OllamaClient(model=args.model, mode=args.ollama_mode)
    stage1_template = load_prompt_template(PROJECT_ROOT / "prompts" / "stage1_prompt.txt")
    stage2_template = load_prompt_template(PROJECT_ROOT / "prompts" / "stage2_prompt.txt")

    skeleton_id, stage1_inputs, stage1_outputs, routing_trace = nlp.select_skeleton(
        qa_tree,
        skeletons,
        stage1_template,
        client,
    )
    selected_skeleton = skeletons[skeleton_id]

    lexicon_entries, semantic_to_label, concept_index, _semantic_to_components = nlp.build_property_lexicon(
        args.summary,
        args.data_root,
    )
    stage2_constraints, stage2_non_numeric, global_constraints, stage2_inputs, stage2_outputs = nlp.run_stage2(
        qa_tree,
        selected_skeleton,
        stage2_template,
        client,
        lexicon_entries,
        concept_index,
    )
    excluded_filter_concepts = {"tankLevelMax", "tankLevelMin"}
    selection_constraints = [
        constraint for constraint in stage2_constraints if constraint.concept not in excluded_filter_concepts
    ]

    asset_type_map = build_asset_type_map(library)
    candidates_by_asset = load_candidate_components(args, asset_type_map)
    requirements = {
        "maxOperatingPressure": selection_module.normalize_constraint_value(global_constraints.get("maxOperatingPressure")),
        "ratedFlowRate": selection_module.normalize_constraint_value(global_constraints.get("ratedFlowRate")),
        "tankNominalVolume": selection_module.normalize_constraint_value(global_constraints.get("tankNominalVolume")),
        "prvSetpoint": selection_module.normalize_constraint_value(global_constraints.get("prvSetpoint")),
        "accNominalVolume": selection_module.normalize_constraint_value(global_constraints.get("accNominalVolume")),
        "accPreChargePressure": selection_module.normalize_constraint_value(global_constraints.get("accPreChargePressure")),
        "cylinderLoad": selection_module.normalize_constraint_value(
            selection_module.get_constraint_by_concept(selection_constraints, "cylinderLoad")
        ),
        "cylinderStroke": selection_module.normalize_constraint_value(
            selection_module.get_constraint_by_concept(selection_constraints, "cylinderStroke")
        ),
    }
    selection, selection_results, backtracking_log = selection_module.select_components(
        selected_skeleton,
        candidates_by_asset,
        requirements,
    )
    audit = {
        "metadata": {
            "timestamp": dt.datetime.now().isoformat(),
            "mode": "qa",
            "model": args.model,
            "ollamaMode": args.ollama_mode,
            "ollamaVersion": nlp.get_ollama_version(),
        },
        "userInputs": stage1_inputs + stage2_inputs,
        "nlpOutputs": stage1_outputs + stage2_outputs,
        "selectedSkeleton": {"skeletonId": skeleton_id, "routingTrace": routing_trace},
        "alignedConstraints": [
            {
                "semanticId": c.semantic_id,
                "propertyLabel": c.property_label,
                "operator": c.operator,
                "value": c.value,
                "unit": c.unit,
                "confidence": c.confidence,
                "evidence": c.evidence,
                "concept": c.concept,
                "operatorDefaulted": c.defaulted_operator,
            }
            for c in stage2_constraints
        ],
        "nonNumericConstraints": [
            {
                "semanticId": c.semantic_id,
                "propertyLabel": c.property_label,
                "valueText": c.value_text,
                "confidence": c.confidence,
                "evidence": c.evidence,
                "concept": c.concept,
            }
            for c in stage2_non_numeric
        ],
        "semanticMapping": [
            {
                "semanticId": c.semantic_id,
                "canonicalLabel": semantic_to_label.get(c.semantic_id, ""),
                "concept": c.concept,
            }
            for c in stage2_constraints
        ],
        "selectionResults": selection_results,
        "selectionBacktracking": backtracking_log,
    }
    symbol_mapping = drawing.load_symbol_mapping(args.symbol_map, PROJECT_ROOT)
    xml_root, _xml_name = read_aasx_xml(system_aasx)
    title_block_values = read_schematic_title_block_values(xml_root)
    result_paths = write_pipeline_outputs(
        system_aasx=system_aasx,
        selected_skeleton=selected_skeleton,
        selection=selection,
        audit=audit,
        global_constraints=global_constraints,
        symbol_mapping=symbol_mapping,
        title_block_values=title_block_values,
        output_dirs=output_dirs,
        args=args,
    )
    print(json.dumps({"mode": "qa", "skeletonId": skeleton_id, **result_paths}, indent=2))
    return 0


def run_network_mode(args: argparse.Namespace) -> int:
    import pipeline_02_component_selection as selection_module
    import pipeline_03_automatic_diagram_generation as drawing

    ensure_runtime_state(export_namespace=args.export_aas_namespace)
    system_aasx = resolve_system_aasx(args.system_aasx)
    if not system_aasx.exists():
        raise FileNotFoundError(f"System AASX not found: {system_aasx}")

    output_dirs = resolve_output_dirs(args.output_dir)
    _qa_tree, library, _skeletons = load_runtime_library(args.qa_tree, args.skeleton_library)
    mapping_semantics = load_component_port_semantics_from_mapping(args.mapping_xlsx)
    network_data, network_input_path = load_network_definition(args)
    selected_skeleton = build_network_skeleton(network_data, mapping_semantics, library.get("componentCatalog", []))

    direct_constraints, global_constraints, requirements = load_constraints_from_json(args.constraints_json)
    asset_type_map = build_asset_type_map(library)
    candidates_by_asset = load_candidate_components(args, asset_type_map)
    if requirements:
        selection, selection_results, backtracking_log = selection_module.select_components(
            selected_skeleton,
            candidates_by_asset,
            requirements,
        )
        selection_mode = "constraint_filtered"
    else:
        selection, selection_results, backtracking_log = selection_module.select_components_random_by_type(
            selected_skeleton,
            candidates_by_asset,
            seed=args.random_seed,
        )
        selection_mode = "random_by_component_type"
    audit = {
        "metadata": {
            "timestamp": dt.datetime.now().isoformat(),
            "mode": "network",
            "selectionMode": selection_mode,
            "networkInput": network_input_path,
        },
        "userInputs": [
            {"stage": "network", "prompt": "network_input", "answer": network_input_path},
            {"stage": "network", "prompt": "mapping_xlsx", "answer": str(args.mapping_xlsx)},
        ],
        "nlpOutputs": [],
        "selectedSkeleton": {
            "skeletonId": selected_skeleton["skeletonId"],
            "routingTrace": [{"mode": "external_volume_node_network"}],
        },
        "alignedConstraints": [
            {
                "semanticId": c.semantic_id,
                "propertyLabel": c.property_label,
                "operator": c.operator,
                "value": c.value,
                "unit": c.unit,
                "confidence": c.confidence,
                "evidence": c.evidence,
                "concept": c.concept,
                "operatorDefaulted": c.defaulted_operator,
            }
            for c in direct_constraints
            if c.value is not None
        ],
        "nonNumericConstraints": [
            {
                "semanticId": c.semantic_id,
                "propertyLabel": c.property_label,
                "valueText": c.value_text,
                "confidence": c.confidence,
                "evidence": c.evidence,
                "concept": c.concept,
            }
            for c in direct_constraints
            if c.value_text
        ],
        "mappingSemantics": {
            "mappingFile": str(args.mapping_xlsx),
            "componentCount": len(mapping_semantics),
        },
        "volumeNodeTopology": selected_skeleton.get("volumeNodes", []),
        "selectionResults": selection_results,
        "selectionBacktracking": backtracking_log,
    }
    symbol_mapping = drawing.load_symbol_mapping(args.symbol_map, PROJECT_ROOT)
    xml_root, _xml_name = read_aasx_xml(system_aasx)
    title_block_values = read_schematic_title_block_values(xml_root)
    result_paths = write_pipeline_outputs(
        system_aasx=system_aasx,
        selected_skeleton=selected_skeleton,
        selection=selection,
        audit=audit,
        global_constraints=global_constraints,
        symbol_mapping=symbol_mapping,
        title_block_values=title_block_values,
        output_dirs=output_dirs,
        args=args,
    )
    print(json.dumps({"mode": "network", "selectionMode": selection_mode, **result_paths}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    defaults = build_default_paths()
    parser = argparse.ArgumentParser(description="Split hydraulic circuit generation pipeline.")
    subparsers = parser.add_subparsers(dest="command")

    qa_parser = subparsers.add_parser("qa", help="Run the original interactive QA-based pipeline.")
    parse_common_args(qa_parser)
    qa_parser.add_argument("--model", type=str, default="llama3.1", help="Ollama model name.")
    qa_parser.add_argument("--ollama-mode", type=str, default="auto", choices=["auto", "http", "cli"])

    network_parser = subparsers.add_parser("network", help="Run the Chapter 8 volume-node/network input mode.")
    parse_common_args(network_parser)
    network_parser.add_argument(
        "--network-xlsx",
        type=Path,
        default=defaults["sample_network_xlsx"],
        help="Path to the external volume-node network Excel file.",
    )
    network_parser.add_argument(
        "--network-json",
        type=Path,
        default=defaults["sample_network_json"],
        help="Legacy fallback JSON network input. Excel is preferred.",
    )
    network_parser.add_argument(
        "--constraints-json",
        type=Path,
        default=None,
        help="Optional direct constraints JSON used before component selection.",
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in {"qa", "network", "-h", "--help"}:
        argv = ["qa", *argv]
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "network":
        return run_network_mode(args)
    return run_qa_mode(args)


if __name__ == "__main__":
    raise SystemExit(main())
