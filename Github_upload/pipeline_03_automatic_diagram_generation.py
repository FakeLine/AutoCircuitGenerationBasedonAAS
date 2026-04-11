#!/usr/bin/env python3
from __future__ import annotations

import base64
import copy
import json
import math
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

from pipeline_04_aas_integration_adapter import (
    CYL_STROKE_IRDI,
    PHI_SYMBOL,
    PUMP_FLOW_IRDI,
    PRV_CRACKING_IRDI,
    ROD_DIAMETER_IRDI,
    SYMBOL_KEY_SEMANTIC_ID,
    XLINK_NS,
    ComponentAAS,
    build_port_role_map,
    extract_interface_specs,
    get_port_spec,
    get_property_value_by_semantic_id,
    normalize_symbol_key,
    parse_float,
    read_xlsx_rows,
    validate_semantic_id,
)

SVG_NS = 'http://www.w3.org/2000/svg'
WIRE_STROKE_WIDTH = '1.3'
JUNCTION_RADIUS_MIN = 1.0
JUNCTION_RADIUS_MAX = 1.0
JUNCTION_DEDUP_TOL = 1.0
JUNCTION_DEGREE_THRESHOLD = 3
JUNCTION_NODE_SIZE = 4.0
DRAW_PIPE_INTERFACE_LABELS = False
COMPONENT_SIZE_MULTIPLIERS = {'BladderAccumulator': 2.0}
INTERNAL_SNAP_EXCLUDED_COMPONENTS: set[str] = set()
PROJECT_ROOT = Path(__file__).resolve().parent
SYMBOL_RENDER_MODES = ('inline', 'image', 'auto')
ET.register_namespace('xlink', XLINK_NS)


@dataclass
@dataclass
class InlineSymbolTemplate:
    min_x: float
    min_y: float
    vb_w: float
    vb_h: float
    defs_children: List[ET.Element]
    content_children: List[ET.Element]


INLINE_SYMBOL_CACHE: Dict[Path, InlineSymbolTemplate] = {}
BASE_H = 56.0
W_MIN = 56.0
W_MAX = 140.0
DEFAULT_W = 72.0
PORT_EPS = 0.8
PORT_ANCHOR_TOL = 1.0
LABEL_CHAR_WIDTH = 0.6
LABEL_HEIGHT_FACTOR = 1.2
LABEL_SOFT_MARGIN = 1.4
LABEL_CANDIDATE_RADII = [0.0, 4.0, 8.0, 12.0, 16.0, 20.0]
COMPONENT_LABEL_CANDIDATE_RADII = [0.0, 4.0, 8.0, 12.0, 16.0, 20.0, 24.0]
LINE_LABEL_CANDIDATE_RADII = [0.0, 3.0, 6.0, 9.0, 12.0, 15.0]


def load_symbol_mapping(xlsx_path: Path, root_dir: Path) -> Dict[str, Path]:
    mapping: Dict[str, Path] = {}
    rows = read_xlsx_rows(xlsx_path)
    if not rows:
        print(f"[SYMBOL] Loaded 0 symbol mappings from {xlsx_path}")
        return mapping
    for row in rows[1:]:
        if len(row) < 2:
            continue
        key = normalize_symbol_key(row[0])
        svg_path = row[1] or ""
        if not key or not svg_path:
            continue
        normalized = str(svg_path).strip().replace("\\", "/")
        normalized = normalized.lstrip("/\\")
        candidate_paths = [(root_dir / normalized).resolve()]
        if normalized.startswith("SymbolDemo/"):
            remapped = normalized.split("/", 1)[1]
            candidate_paths.append((root_dir / "symbols" / remapped).resolve())
        candidate_paths.append((root_dir / "main" / normalized).resolve())
        candidate_paths.append((xlsx_path.parent / normalized).resolve())
        resolved = candidate_paths[-1]
        for candidate in candidate_paths:
            if candidate.exists():
                resolved = candidate
                break
        if not resolved.exists():
            print(f"[SYMBOL] SVG_MISSING key={key} resolved={resolved}")
        mapping[key] = resolved
    print(f"[SYMBOL] Loaded {len(mapping)} symbol mappings from {xlsx_path}")
    return mapping

def parse_svg_size(root: ET.Element) -> Tuple[float, float]:
    view_box = root.get("viewBox")
    if view_box:
        parts = view_box.replace(",", " ").split()
        if len(parts) == 4:
            try:
                return float(parts[2]), float(parts[3])
            except ValueError:
                pass
    width_attr = root.get("width", "")
    height_attr = root.get("height", "")
    width = parse_float(re.findall(r"[0-9.]+", width_attr)[0]) if re.findall(r"[0-9.]+", width_attr) else 1000.0
    height = parse_float(re.findall(r"[0-9.]+", height_attr)[0]) if re.findall(r"[0-9.]+", height_attr) else 700.0
    return width or 1000.0, height or 700.0

def update_template_date(root: ET.Element, date_str: str) -> None:
    if not date_str:
        return
    date_elem = root.find(f".//{{{SVG_NS}}}text[@id='date']")
    if date_elem is None:
        print("[WARN] Date field not found in SVG template.")
        return
    tspan = date_elem.find(f"{{{SVG_NS}}}tspan")
    if tspan is not None:
        tspan.text = date_str
    else:
        date_elem.text = date_str
    date_elem.set("font-size", "3.6")
    print(f"[LAYOUT] Updated template date to {date_str}")

def wrap_text_for_svg(text: str, max_chars: int, max_lines: int) -> List[str]:
    raw = (text or "").strip()
    if not raw:
        return [""]
    if max_chars <= 0 or len(raw) <= max_chars:
        return [raw]

    words = raw.split()
    lines: List[str] = []
    if len(words) > 1:
        current = words[0]
        for word in words[1:]:
            candidate = f"{current} {word}"
            if len(candidate) <= max_chars:
                current = candidate
            else:
                lines.append(current)
                current = word
        lines.append(current)
    else:
        lines = [raw[i : i + max_chars] for i in range(0, len(raw), max_chars)]

    if len(lines) > max_lines:
        lines = lines[:max_lines]
        if not lines[-1].endswith("..."):
            trimmed = lines[-1][: max(1, max_chars - 3)].rstrip()
            lines[-1] = f"{trimmed}..."
    return lines

def wrap_text_by_words_for_svg(text: str, words_per_line: int, max_lines: int) -> List[str]:
    raw = (text or "").strip()
    if not raw:
        return [""]
    if words_per_line <= 0:
        return [raw]
    words = raw.split()
    if len(words) <= words_per_line:
        return [raw]
    lines = [
        " ".join(words[idx : idx + words_per_line])
        for idx in range(0, len(words), words_per_line)
    ]
    if max_lines > 0 and len(lines) > max_lines:
        lines = lines[:max_lines]
        if not lines[-1].endswith("..."):
            lines[-1] = f"{lines[-1].rstrip()}..."
    return lines

def set_svg_text_by_id(
    root: ET.Element,
    text_id: str,
    value: str,
    wrap_chars: Optional[int] = None,
    max_lines: int = 2,
    font_size: Optional[str] = None,
) -> bool:
    text_elem = root.find(f".//{{{SVG_NS}}}text[@id='{text_id}']")
    if text_elem is None:
        return False

    old_tspans = text_elem.findall(f"{{{SVG_NS}}}tspan")
    lines = [value]
    if "\n" in value:
        lines = [line for line in (part.strip() for part in value.splitlines()) if line]
        if not lines:
            lines = [""]
    elif wrap_chars is not None and wrap_chars > 0:
        lines = wrap_text_for_svg(value, wrap_chars, max_lines)

    if len(lines) == 1 and not old_tspans:
        text_elem.text = value
        if font_size is not None:
            text_elem.set("font-size", str(font_size))
        return True

    text_elem.text = None
    x_val = text_elem.get("x")
    y_val = text_elem.get("y")
    if old_tspans:
        x_val = x_val or old_tspans[0].get("x")
        y_val = y_val or old_tspans[0].get("y")
    for child in list(text_elem):
        text_elem.remove(child)

    for idx, line in enumerate(lines):
        attrs: Dict[str, str] = {}
        if x_val is not None:
            attrs["x"] = x_val
        if idx == 0:
            if y_val is not None:
                attrs["y"] = y_val
        else:
            attrs["dy"] = "1.1em"
        tspan = ET.SubElement(text_elem, f"{{{SVG_NS}}}tspan", attrs)
        tspan.text = line
    if font_size is not None:
        text_elem.set("font-size", str(font_size))
    return True

def update_template_title_block(root: ET.Element, title_block_values: Dict[str, str]) -> None:
    if not title_block_values:
        return
    wrap_rules: Dict[str, Tuple[int, int]] = {}
    font_rules: Dict[str, str] = {
        "Title": "7pt",
        "DrawingNumber": "12pt",
        "created_by_name": "9pt",
    }
    for svg_text_id, value in title_block_values.items():
        if not value:
            continue
        wrapped_by_words = wrap_text_by_words_for_svg(value, words_per_line=2, max_lines=4)
        final_value = "\n".join(wrapped_by_words)
        wrap = wrap_rules.get(svg_text_id)
        font_size = font_rules.get(svg_text_id)
        if wrap:
            ok = set_svg_text_by_id(
                root,
                svg_text_id,
                final_value,
                wrap_chars=wrap[0],
                max_lines=wrap[1],
                font_size=font_size,
            )
        else:
            ok = set_svg_text_by_id(root, svg_text_id, final_value, font_size=font_size)
        if ok:
            print(f"[LAYOUT] Updated title block field {svg_text_id} -> {final_value}")
        else:
            print(f"[WARN] Title block SVG text field not found: {svg_text_id}")

def parse_viewbox(root: ET.Element) -> Tuple[float, float, float, float]:
    view_box = root.get("viewBox") or root.get("viewbox")
    if view_box:
        parts = [p for p in view_box.replace(",", " ").split() if p.strip()]
        if len(parts) == 4:
            try:
                min_x, min_y, vb_w, vb_h = (float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]))
                return min_x, min_y, vb_w, vb_h
            except ValueError:
                pass
    width, height = parse_svg_size(root)
    return 0.0, 0.0, width, height

def parse_points_attr(points_attr: str) -> List[Tuple[float, float]]:
    nums = [float(n) for n in re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", points_attr or "")]
    if len(nums) < 2:
        return []
    return list(zip(nums[0::2], nums[1::2]))

def bbox_from_points(points: List[Tuple[float, float]]) -> Optional[Tuple[float, float, float, float]]:
    if not points:
        return None
    xs = [pt[0] for pt in points]
    ys = [pt[1] for pt in points]
    return min(xs), min(ys), max(xs), max(ys)

def union_bbox(
    a: Optional[Tuple[float, float, float, float]],
    b: Optional[Tuple[float, float, float, float]],
) -> Optional[Tuple[float, float, float, float]]:
    if a is None:
        return b
    if b is None:
        return a
    return (
        min(a[0], b[0]),
        min(a[1], b[1]),
        max(a[2], b[2]),
        max(a[3], b[3]),
    )

def element_bbox(elem: ET.Element, root: ET.Element) -> Optional[Tuple[float, float, float, float]]:
    tag = elem.tag.split("}")[-1]
    points: List[Tuple[float, float]] = []
    if tag == "rect":
        x = parse_float(elem.get("x", "0")) or 0.0
        y = parse_float(elem.get("y", "0")) or 0.0
        w = parse_float(elem.get("width", "0")) or 0.0
        h = parse_float(elem.get("height", "0")) or 0.0
        points = [(x, y), (x + w, y), (x + w, y + h), (x, y + h)]
    elif tag == "line":
        x1 = parse_float(elem.get("x1", "0")) or 0.0
        y1 = parse_float(elem.get("y1", "0")) or 0.0
        x2 = parse_float(elem.get("x2", "0")) or 0.0
        y2 = parse_float(elem.get("y2", "0")) or 0.0
        points = [(x1, y1), (x2, y2)]
    elif tag in {"polyline", "polygon"}:
        points = parse_points_attr(elem.get("points", ""))
    elif tag == "circle":
        cx = parse_float(elem.get("cx", "0")) or 0.0
        cy = parse_float(elem.get("cy", "0")) or 0.0
        r = parse_float(elem.get("r", "0")) or 0.0
        points = [(cx - r, cy - r), (cx + r, cy + r)]
    elif tag == "ellipse":
        cx = parse_float(elem.get("cx", "0")) or 0.0
        cy = parse_float(elem.get("cy", "0")) or 0.0
        rx = parse_float(elem.get("rx", "0")) or 0.0
        ry = parse_float(elem.get("ry", "0")) or 0.0
        points = [(cx - rx, cy - ry), (cx + rx, cy + ry)]
    elif tag == "path":
        coords = parse_points_attr(elem.get("d", ""))
        if coords:
            points = coords
    else:
        return None
    if not points:
        return None
    matrix = compute_cumulative_transform(root, elem)
    transformed = [apply_matrix(matrix, px, py) for px, py in points]
    return bbox_from_points(transformed)

def compute_group_bbox(group: ET.Element, root: ET.Element) -> Optional[Tuple[float, float, float, float]]:
    bbox: Optional[Tuple[float, float, float, float]] = None
    for elem in group.iter():
        elem_bbox = element_bbox(elem, root)
        bbox = union_bbox(bbox, elem_bbox)
    return bbox

def compute_group_bbox_in_quadrant(
    group: ET.Element,
    root: ET.Element,
    width: float,
    height: float,
) -> Optional[Tuple[float, float, float, float]]:
    bbox: Optional[Tuple[float, float, float, float]] = None
    for elem in group.iter():
        elem_bbox = element_bbox(elem, root)
        if not elem_bbox:
            continue
        cx = (elem_bbox[0] + elem_bbox[2]) / 2.0
        cy = (elem_bbox[1] + elem_bbox[3]) / 2.0
        if cx >= width * 0.5 and cy >= height * 0.5:
            bbox = union_bbox(bbox, elem_bbox)
    return bbox

def find_title_block_bbox(root: ET.Element) -> Optional[Tuple[float, float, float, float]]:
    keywords = ("title", "titleblock", "title_block", "frame", "layer2", "layer_2")
    candidates: List[Tuple[float, float, float, float]] = []
    page_w, page_h = parse_svg_size(root)
    page_area = max(page_w * page_h, 1.0)

    def area(b: Tuple[float, float, float, float]) -> float:
        return max(b[2] - b[0], 0.0) * max(b[3] - b[1], 0.0)

    def matches(elem: ET.Element) -> bool:
        ident = elem.get("id", "")
        cls = elem.get("class", "")
        label = elem.get("{http://www.inkscape.org/namespaces/inkscape}label", "")
        text = f"{ident} {cls} {label}".lower()
        return any(keyword in text for keyword in keywords)

    # Pass 1: prefer group elements with reasonable area.
    for elem in root.iter():
        if elem.tag.split("}")[-1] != "g":
            continue
        if not matches(elem):
            continue
        bbox = compute_group_bbox(elem, root)
        if bbox and area(bbox) <= page_area * 0.6:
            candidates.append(bbox)
        elif bbox:
            quadrant_bbox = compute_group_bbox_in_quadrant(elem, root, page_w, page_h)
            if quadrant_bbox and area(quadrant_bbox) <= page_area * 0.6:
                candidates.append(quadrant_bbox)

    # Pass 2: fallback to any matched element (still filter out huge boxes).
    if not candidates:
        for elem in root.iter():
            if not matches(elem):
                continue
            bbox = compute_group_bbox(elem, root)
            if bbox and area(bbox) <= page_area * 0.6:
                candidates.append(bbox)
            elif bbox:
                quadrant_bbox = compute_group_bbox_in_quadrant(elem, root, page_w, page_h)
                if quadrant_bbox and area(quadrant_bbox) <= page_area * 0.6:
                    candidates.append(quadrant_bbox)

    if not candidates:
        return None
    return min(candidates, key=area)

def find_element_by_id(root: ET.Element, target_id: str) -> Optional[ET.Element]:
    for elem in root.iter():
        if elem.get("id") == target_id:
            return elem
    return None

def extract_circle_center(element: ET.Element) -> Tuple[float, float]:
    tag = element.tag.split("}")[-1]
    if tag in {"circle", "ellipse"}:
        cx = parse_float(element.get("cx", "0")) or 0.0
        cy = parse_float(element.get("cy", "0")) or 0.0
        return cx, cy
    if tag == "rect":
        x = parse_float(element.get("x", "0")) or 0.0
        y = parse_float(element.get("y", "0")) or 0.0
        w = parse_float(element.get("width", "0")) or 0.0
        h = parse_float(element.get("height", "0")) or 0.0
        return x + w / 2.0, y + h / 2.0
    raise RuntimeError(f"Unsupported port marker element: {tag}")

def mat_mul(m1: Tuple[float, float, float, float, float, float], m2: Tuple[float, float, float, float, float, float]) -> Tuple[float, float, float, float, float, float]:
    a1, b1, c1, d1, e1, f1 = m1
    a2, b2, c2, d2, e2, f2 = m2
    return (
        a1 * a2 + c1 * b2,
        b1 * a2 + d1 * b2,
        a1 * c2 + c1 * d2,
        b1 * c2 + d1 * d2,
        a1 * e2 + c1 * f2 + e1,
        b1 * e2 + d1 * f2 + f1,
    )

def apply_matrix(matrix: Tuple[float, float, float, float, float, float], x: float, y: float) -> Tuple[float, float]:
    a, b, c, d, e, f = matrix
    return a * x + c * y + e, b * x + d * y + f

def parse_transform_list(transform: str) -> Tuple[float, float, float, float, float, float]:
    result = (1.0, 0.0, 0.0, 1.0, 0.0, 0.0)
    for match in re.finditer(r"([a-zA-Z]+)\s*\(([^)]*)\)", transform):
        name = match.group(1).strip().lower()
        values = [float(v) for v in re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", match.group(2))]
        if name == "translate":
            tx = values[0] if values else 0.0
            ty = values[1] if len(values) > 1 else 0.0
            mat = (1.0, 0.0, 0.0, 1.0, tx, ty)
        elif name == "scale":
            sx = values[0] if values else 1.0
            sy = values[1] if len(values) > 1 else sx
            mat = (sx, 0.0, 0.0, sy, 0.0, 0.0)
        elif name == "rotate":
            angle = values[0] if values else 0.0
            rad = math.radians(angle)
            cos_a = math.cos(rad)
            sin_a = math.sin(rad)
            rot = (cos_a, sin_a, -sin_a, cos_a, 0.0, 0.0)
            if len(values) >= 3:
                cx, cy = values[1], values[2]
                mat = mat_mul(mat_mul((1.0, 0.0, 0.0, 1.0, cx, cy), rot), (1.0, 0.0, 0.0, 1.0, -cx, -cy))
            else:
                mat = rot
        elif name == "matrix" and len(values) >= 6:
            mat = (values[0], values[1], values[2], values[3], values[4], values[5])
        else:
            continue
        result = mat_mul(result, mat)
    return result

def compute_cumulative_transform(root: ET.Element, element: ET.Element) -> Tuple[float, float, float, float, float, float]:
    parent_map = {child: parent for parent in root.iter() for child in parent}
    chain: List[ET.Element] = []
    current: Optional[ET.Element] = element
    while current is not None:
        chain.append(current)
        current = parent_map.get(current)
    chain.reverse()
    matrix = (1.0, 0.0, 0.0, 1.0, 0.0, 0.0)
    for node in chain:
        transform = node.get("transform", "")
        if transform:
            matrix = mat_mul(matrix, parse_transform_list(transform))
    return matrix


def order_slot_ids_for_layout(selection: Dict[str, ComponentAAS]) -> List[str]:
    supply_types = {
        "Tank",
        "ConstantPump",
        "VariablePump",
        "PressureReliefValve",
        "CheckValve",
        "Accumulator",
    }
    actuator_types = {
        "Double-ActingCylinder",
        "SynchronousCylinder",
        "PlungerCylinder",
        "TelescopicCylinder",
        "HydraulicMotor",
    }
    groups: Dict[str, List[str]] = {"supply": [], "other": [], "dcv": [], "actuator": []}
    for slot_id, comp in selection.items():
        comp_type = comp.component_type or ""
        if comp_type in supply_types:
            groups["supply"].append(slot_id)
        elif comp_type in actuator_types:
            groups["actuator"].append(slot_id)
        elif "DirectionalControlValve" in comp_type or comp_type in {
            "4-3DirectionalControlValve",
            "3-2DirectionalControlValve",
        }:
            groups["dcv"].append(slot_id)
        else:
            groups["other"].append(slot_id)
    ordered: List[str] = []
    ordered.extend(sorted(groups["supply"]))
    ordered.extend(sorted(groups["other"]))
    ordered.extend(sorted(groups["dcv"]))
    ordered.extend(sorted(groups["actuator"]))
    return ordered


def build_pc_cc_layout_sequence(selection: Dict[str, ComponentAAS]) -> List[str]:
    pump_slot = "PUMP" if "PUMP" in selection else None
    if pump_slot is None:
        pump_slot = next(
            (
                slot_id
                for slot_id, comp in selection.items()
                if comp.component_type in {"VariablePump", "ConstantPump", "HydraulicMotor"}
            ),
            None,
        )

    prv_slots = [slot_id for slot_id in selection.keys() if slot_id.startswith("PRV")]
    ordered_prv: List[str] = []
    if "PRV_A2B" in selection:
        ordered_prv.append("PRV_A2B")
    if "PRV_B2A" in selection:
        ordered_prv.append("PRV_B2A")
    if len(ordered_prv) < 2:
        for slot_id in sorted(prv_slots):
            if slot_id not in ordered_prv:
                ordered_prv.append(slot_id)

    cyl_slot = "CYL" if "CYL" in selection else None
    if cyl_slot is None:
        cyl_slot = next(
            (
                slot_id
                for slot_id, comp in selection.items()
                if comp.component_type
                in {"Double-ActingCylinder", "SynchronousCylinder", "PlungerCylinder", "TelescopicCylinder"}
            ),
            None,
        )

    sequence: List[str] = []
    if pump_slot:
        sequence.append(pump_slot)
    if ordered_prv:
        sequence.extend(ordered_prv[:2])
    if cyl_slot:
        sequence.append(cyl_slot)
    return sequence


def find_tank_slots(selection: Dict[str, ComponentAAS]) -> List[str]:
    tank_slots = []
    for slot_id, comp in selection.items():
        if slot_id.upper() == "TANK" or (comp.component_type or "") == "Tank":
            tank_slots.append(slot_id)
    return tank_slots


def tank_layer_constraint(direction: str, flipped: bool) -> Optional[str]:
    if direction == "UP":
        base = "FIRST"
    elif direction == "DOWN":
        base = "LAST"
    else:
        return None
    if not flipped:
        return base
    return "LAST" if base == "FIRST" else "FIRST"


def extract_available_svg_port_ids(component: ComponentAAS) -> List[str]:
    if component.interface_specs:
        return sorted(component.interface_specs.keys())
    if component.aasx_xml_root is not None:
        specs, _roles = extract_interface_specs(component.aasx_xml_root)
        return sorted(specs.keys())
    return []

def normalize_port_key(port_key: str) -> str:
    text = port_key.strip()
    text = re.sub(r"^port[_\\s-]*", "", text, flags=re.IGNORECASE)
    return text.upper()

def clamp(value: float, lo: float, hi: float) -> float:
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value

def project_to_nearest_boundary(
    x: float, y: float, width: float, height: float
) -> Tuple[float, float, str]:
    if width <= 0 or height <= 0:
        return 0.0, 0.0, "WEST"
    eps = min(PORT_EPS, width / 2.0, height / 2.0)
    x_clamped = clamp(x, eps, width - eps)
    y_clamped = clamp(y, eps, height - eps)
    dl = x_clamped
    dr = width - x_clamped
    dt = y_clamped
    db = height - y_clamped
    min_dist = min(dl, dr, dt, db)
    if min_dist == dl:
        return eps, y_clamped, "WEST"
    if min_dist == dr:
        return width - eps, y_clamped, "EAST"
    if min_dist == dt:
        return x_clamped, eps, "NORTH"
    return x_clamped, height - eps, "SOUTH"

def normalize_rotation_deg(rotation_deg: Any) -> int:
    try:
        value = int(rotation_deg)
    except (TypeError, ValueError):
        return 0
    value = value % 360
    if value < 0:
        value += 360
    return value

def rotate_point(x: float, y: float, width: float, height: float, rotation_deg: int) -> Tuple[float, float]:
    rotation = normalize_rotation_deg(rotation_deg)
    if rotation == 0:
        return x, y
    cx = width / 2.0
    cy = height / 2.0
    dx = x - cx
    dy = y - cy
    if rotation == 90:
        return cx - dy, cy + dx
    if rotation == 180:
        return width - x, height - y
    if rotation == 270:
        return cx + dy, cy - dx
    print(f"[WARN] Unsupported rotationDeg={rotation}; only 0/90/180/270 supported.")
    return x, y

def rotate_dir_hint(dir_hint: Optional[str], rotation_deg: int) -> Optional[str]:
    if not dir_hint:
        return None
    rotation = normalize_rotation_deg(rotation_deg)
    hint = dir_hint.strip().lower()
    if rotation == 0:
        return hint
    if rotation == 90:
        mapping = {"north": "east", "east": "south", "south": "west", "west": "north"}
        return mapping.get(hint, hint)
    if rotation == 180:
        mapping = {"north": "south", "south": "north", "east": "west", "west": "east"}
        return mapping.get(hint, hint)
    if rotation == 270:
        mapping = {"north": "west", "west": "south", "south": "east", "east": "north"}
        return mapping.get(hint, hint)
    print(f"[WARN] Unsupported rotationDeg={rotation}; only 0/90/180/270 supported.")
    return hint

def simplify_polyline(points: List[Tuple[float, float]], tol: float = 1e-6) -> List[Tuple[float, float]]:
    if len(points) <= 2:
        return points
    cleaned = [points[0]]
    for x, y in points[1:]:
        px, py = cleaned[-1]
        if abs(x - px) <= tol and abs(y - py) <= tol:
            continue
        cleaned.append((x, y))
    if len(cleaned) <= 2:
        return cleaned
    simplified = [cleaned[0]]
    for idx in range(1, len(cleaned) - 1):
        x0, y0 = simplified[-1]
        x1, y1 = cleaned[idx]
        x2, y2 = cleaned[idx + 1]
        if (abs(x0 - x1) <= tol and abs(x1 - x2) <= tol) or (abs(y0 - y1) <= tol and abs(y1 - y2) <= tol):
            continue
        simplified.append((x1, y1))
    simplified.append(cleaned[-1])
    return simplified

def count_bends(points: List[Tuple[float, float]], tol: float = 1e-6) -> int:
    if len(points) < 3:
        return 0
    directions: List[str] = []
    for idx in range(1, len(points)):
        dx = points[idx][0] - points[idx - 1][0]
        dy = points[idx][1] - points[idx - 1][1]
        if abs(dx) <= tol and abs(dy) <= tol:
            continue
        direction = "H" if abs(dx) >= abs(dy) else "V"
        directions.append(direction)
    bends = 0
    for idx in range(1, len(directions)):
        if directions[idx] != directions[idx - 1]:
            bends += 1
    return bends

def project_to_boundary_with_direction(
    x: float, y: float, width: float, height: float, direction: str
) -> Tuple[float, float, str]:
    if width <= 0 or height <= 0:
        return 0.0, 0.0, "WEST"
    eps = min(PORT_EPS, width / 2.0, height / 2.0)
    x_clamped = clamp(x, eps, width - eps)
    y_clamped = clamp(y, eps, height - eps)
    dir_norm = direction.strip().lower()
    if dir_norm == "north":
        return x_clamped, eps, "NORTH"
    if dir_norm == "south":
        return x_clamped, height - eps, "SOUTH"
    if dir_norm == "west":
        return eps, y_clamped, "WEST"
    if dir_norm == "east":
        return width - eps, y_clamped, "EAST"
    return project_to_nearest_boundary(x_clamped, y_clamped, width, height)

def resolve_port_mapping(
    slot_id: str,
    required_ports: List[str],
    available_ports: List[str],
    symbol_key: str,
) -> Dict[str, str]:
    alias_candidates = {
        "IN": ["P", "INLET"],
        "OUT": ["A", "OUTLET"],
        "P": ["A", "IN", "INLET"],
        "A": ["P", "OUT", "OUTLET"],
        "B": ["S", "RETURN"],
        "S": ["B", "RETURN"],
    }
    mapping: Dict[str, str] = {}
    available_set = {p for p in available_ports if p}
    available_lower = {p.lower(): p for p in available_ports if isinstance(p, str)}
    for port_key in required_ports:
        if port_key in available_set:
            mapping[port_key] = port_key
            continue
        lowered = port_key.lower()
        if lowered in available_lower:
            mapping[port_key] = available_lower[lowered]
            continue
        normalized = normalize_port_key(port_key)
        if normalized in available_set:
            mapping[port_key] = normalized
            continue
        lowered_norm = normalized.lower()
        if lowered_norm in available_lower:
            mapping[port_key] = available_lower[lowered_norm]
            continue
        for alias in alias_candidates.get(port_key, []):
            if alias in available_set:
                mapping[port_key] = alias
                break
            alias_lower = alias.lower()
            if alias_lower in available_lower:
                mapping[port_key] = available_lower[alias_lower]
                break
        if port_key in mapping:
            continue
        raise RuntimeError(
            f"Missing SVG port id for slot {slot_id}, portKey={port_key}, "
            f"available={available_ports}, symbolKey={symbol_key}"
        )
    return mapping

def extract_port_local_meta(
    svg_path: Path,
    svg_port_ids: List[str],
    comp_w: float,
    comp_h: float,
) -> Tuple[Dict[str, Tuple[float, float]], Dict[str, Optional[str]]]:
    alias_candidates = {
        "IN": ["P", "INLET"],
        "OUT": ["A", "OUTLET"],
        "P": ["A", "IN", "INLET"],
        "A": ["P", "OUT", "OUTLET"],
        "B": ["S", "RETURN"],
        "S": ["B", "RETURN"],
    }
    tree = ET.parse(svg_path)
    root = tree.getroot()
    min_x, min_y, vb_w, vb_h = parse_viewbox(root)
    if vb_w <= 0 or vb_h <= 0:
        raise RuntimeError(f"Invalid viewBox for symbol: {svg_path}")
    scale = min(comp_w / vb_w, comp_h / vb_h)
    coords: Dict[str, Tuple[float, float]] = {}
    directions: Dict[str, Optional[str]] = {}
    for port_id in svg_port_ids:
        elem = find_element_by_id(root, port_id)
        if elem is None:
            for alias in alias_candidates.get(port_id, []):
                elem = find_element_by_id(root, alias)
                if elem is not None:
                    break
        if elem is None:
            raise RuntimeError(f"SVG port marker id '{port_id}' not found in {svg_path}")
        cx, cy = extract_circle_center(elem)
        matrix = compute_cumulative_transform(root, elem)
        gx, gy = apply_matrix(matrix, cx, cy)
        local_x = (gx - min_x) * scale
        local_y = (gy - min_y) * scale
        coords[port_id] = (local_x, local_y)
        direction = elem.get("data-direction")
        if direction is None:
            for attr_name, attr_value in elem.attrib.items():
                if attr_name.lower().endswith("data-direction"):
                    direction = attr_value
                    break
        if direction is not None:
            dir_norm = str(direction).strip().lower()
            if dir_norm in {"north", "south", "east", "west"}:
                directions[port_id] = dir_norm
            else:
                directions[port_id] = None
        else:
            directions[port_id] = None
    return coords, directions

def layout_positions_simple(
    skeleton: Dict[str, Any], width: float, height: float, margin: float
) -> Dict[str, Tuple[float, float]]:
    slots = [slot["slotId"] for slot in skeleton.get("componentSlots", [])]
    adjacency: Dict[str, List[str]] = {slot_id: [] for slot_id in slots}
    for connection in skeleton.get("connections", []):
        a = connection["from"]["slotId"]
        b = connection["to"]["slotId"]
        adjacency[a].append(b)
        adjacency[b].append(a)

    root_slot = slots[0] if slots else ""
    for slot in skeleton.get("componentSlots", []):
        if slot.get("componentType") in {"ConstantPump", "VariablePump"}:
            root_slot = slot["slotId"]
            break

    level: Dict[str, int] = {}
    if root_slot:
        queue = [root_slot]
        level[root_slot] = 0
        while queue:
            current = queue.pop(0)
            for neighbor in adjacency.get(current, []):
                if neighbor in level:
                    continue
                level[neighbor] = level[current] + 1
                queue.append(neighbor)
    max_level = max(level.values(), default=0)
    for slot_id in slots:
        if slot_id not in level:
            max_level += 1
            level[slot_id] = max_level

    levels: Dict[int, List[str]] = {}
    for slot_id, lvl in level.items():
        levels.setdefault(lvl, []).append(slot_id)

    x_spacing = (width - 2 * margin) / max(1, max(levels.keys(), default=0))
    positions: Dict[str, Tuple[float, float]] = {}
    for lvl, nodes in sorted(levels.items()):
        nodes.sort()
        y_spacing = (height - 2 * margin) / (len(nodes) + 1)
        for index, slot_id in enumerate(nodes):
            x = margin + lvl * x_spacing
            y = margin + (index + 1) * y_spacing
            positions[slot_id] = (x, y)
    return positions

def normalize_positions(
    positions: Dict[str, Tuple[float, float]],
    width: float,
    height: float,
    margin: float,
) -> Dict[str, Tuple[float, float]]:
    if not positions:
        return positions
    xs = [pos[0] for pos in positions.values()]
    ys = [pos[1] for pos in positions.values()]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    span_x = max(max_x - min_x, 1.0)
    span_y = max(max_y - min_y, 1.0)
    target_w = max(width - 2 * margin, 1.0)
    target_h = max(height - 2 * margin, 1.0)
    normalized: Dict[str, Tuple[float, float]] = {}
    for key, (x, y) in positions.items():
        nx = margin + ((x - min_x) / span_x) * target_w
        ny = margin + ((y - min_y) / span_y) * target_h
        normalized[key] = (nx, ny)
    return normalized

def resolve_node_executable() -> Optional[str]:
    node_path = shutil.which("node")
    if node_path:
        return node_path
    program_files = Path(os.environ.get("ProgramFiles", "C:/Program Files"))
    default = program_files / "nodejs" / "node.exe"
    if default.exists():
        return str(default)
    return None

def run_elk_layout(graph: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    script = (
        "const elk = new (require('elkjs/lib/elk.bundled.js'))();"
        "let data='';"
        "process.stdin.on('data', chunk => data += chunk);"
        "process.stdin.on('end', () => {"
        "const graph = JSON.parse(data);"
        "elk.layout(graph).then(result => {"
        "process.stdout.write(JSON.stringify(result));"
        "}).catch(err => {"
        "console.error(err && err.stack ? err.stack : err);"
        "process.exit(1);"
        "});"
        "});"
    )
    node_exe = resolve_node_executable()
    if not node_exe:
        raise RuntimeError("Node.js not found for ELK layout.")
    proc = subprocess.run(
        [node_exe, "-e", script],
        input=json.dumps(graph),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
        cwd=PROJECT_ROOT,
    )
    if proc.returncode != 0 or not proc.stdout:
        if proc.stderr:
            print(proc.stderr.strip())
        raise RuntimeError("ELK layout failed.")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Failed to parse ELK output JSON.") from exc

def build_required_ports(skeleton: Dict[str, Any], selection: Dict[str, ComponentAAS]) -> Dict[str, List[str]]:
    required: Dict[str, List[str]] = {slot_id: [] for slot_id in selection.keys()}
    for connection in skeleton.get("connections", []):
        from_slot = connection["from"]["slotId"]
        to_slot = connection["to"]["slotId"]
        from_port = connection["from"]["portKey"]
        to_port = connection["to"]["portKey"]
        if from_slot in required:
            if from_port not in required[from_slot]:
                required[from_slot].append(from_port)
        if to_slot in required:
            if to_port not in required[to_slot]:
                required[to_slot].append(to_port)
    return required

def rects_overlap(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float], eps: float = 0.5) -> bool:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    return not (
        ax + aw <= bx + eps
        or bx + bw <= ax + eps
        or ay + ah <= by + eps
        or by + bh <= ay + eps
    )

def segment_intersects_rect(
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    rect: Tuple[float, float, float, float],
    eps: float = 0.1,
) -> bool:
    x1, y1 = p1
    x2, y2 = p2
    rx, ry, rw, rh = rect
    left = rx + eps
    right = rx + rw - eps
    top = ry + eps
    bottom = ry + rh - eps
    if abs(x1 - x2) < 1e-6:
        x = x1
        if x <= left or x >= right:
            return False
        y_min, y_max = sorted([y1, y2])
        return y_max > top and y_min < bottom
    if abs(y1 - y2) < 1e-6:
        y = y1
        if y <= top or y >= bottom:
            return False
        x_min, x_max = sorted([x1, x2])
        return x_max > left and x_min < right
    # non-orthogonal segment, fallback to bounding box intersection
    x_min, x_max = sorted([x1, x2])
    y_min, y_max = sorted([y1, y2])
    return x_max > left and x_min < right and y_max > top and y_min < bottom

def polyline_total_length(points: List[Tuple[float, float]]) -> float:
    total = 0.0
    for idx in range(1, len(points)):
        dx = points[idx][0] - points[idx - 1][0]
        dy = points[idx][1] - points[idx - 1][1]
        total += math.hypot(dx, dy)
    return total

def polyline_point_at(points: List[Tuple[float, float]], distance: float) -> Tuple[float, float]:
    if not points:
        return 0.0, 0.0
    if distance <= 0:
        return points[0]
    remaining = distance
    for idx in range(1, len(points)):
        x1, y1 = points[idx - 1]
        x2, y2 = points[idx]
        seg = math.hypot(x2 - x1, y2 - y1)
        if seg <= 0:
            continue
        if remaining <= seg:
            t = remaining / seg
            return x1 + (x2 - x1) * t, y1 + (y2 - y1) * t
        remaining -= seg
    return points[-1]

def dedupe_points(points: List[Tuple[float, float]], tol: float = 1.0) -> List[Tuple[float, float]]:
    if tol <= 0:
        return points
    buckets: Dict[Tuple[int, int], Tuple[float, float]] = {}
    for x, y in points:
        key = (int(round(x / tol)), int(round(y / tol)))
        if key in buckets:
            continue
        buckets[key] = (x, y)
    return list(buckets.values())

def compute_fit_transform(
    nodes: Dict[str, Tuple[float, float, float, float]],
    edges: List[Dict[str, Any]],
    width: float,
    height: float,
    margin: float,
) -> Tuple[float, float, float]:
    xs: List[float] = []
    ys: List[float] = []
    for x, y, w, h in nodes.values():
        xs.extend([x, x + w])
        ys.extend([y, y + h])
    for edge in edges:
        for x, y in edge.get("points", []):
            xs.append(x)
            ys.append(y)
        for x, y in edge.get("junctions", []):
            xs.append(x)
            ys.append(y)
    if not xs or not ys:
        return 1.0, 0.0, 0.0
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    content_w = max(max_x - min_x, 1.0)
    content_h = max(max_y - min_y, 1.0)
    target_w = max(width - 2 * margin, 1.0)
    target_h = max(height - 2 * margin, 1.0)
    scale = min(1.0, target_w / content_w, target_h / content_h)
    tx = margin + (target_w - content_w * scale) / 2.0 - min_x * scale
    ty = margin + (target_h - content_h * scale) / 2.0 - min_y * scale
    return scale, tx, ty

def bbox_intersects(
    a: Tuple[float, float, float, float],
    b: Tuple[float, float, float, float],
) -> bool:
    return not (a[2] <= b[0] or b[2] <= a[0] or a[3] <= b[1] or b[3] <= a[1])

def shift_bbox(
    bbox: Tuple[float, float, float, float], dx: float, dy: float
) -> Tuple[float, float, float, float]:
    return (bbox[0] + dx, bbox[1] + dy, bbox[2] + dx, bbox[3] + dy)

def compute_diagram_bbox(
    nodes: Dict[str, Tuple[float, float, float, float]],
    edges: List[Dict[str, Any]],
) -> Optional[Tuple[float, float, float, float]]:
    xs: List[float] = []
    ys: List[float] = []
    for x, y, w, h in nodes.values():
        xs.extend([x, x + w])
        ys.extend([y, y + h])
    for edge in edges:
        for x, y in edge.get("points", []):
            xs.append(x)
            ys.append(y)
    if not xs or not ys:
        return None
    return min(xs), min(ys), max(xs), max(ys)

def apply_transform_to_point(
    point: Tuple[float, float], scale: float, tx: float, ty: float
) -> Tuple[float, float]:
    return point[0] * scale + tx, point[1] * scale + ty

def layout_with_ports_elk(
    skeleton: Dict[str, Any],
    selection: Dict[str, ComponentAAS],
    port_local: Dict[str, Dict[str, Tuple[float, float]]],
    port_sides: Dict[str, Dict[str, str]],
    required_ports: Dict[str, List[str]],
    node_sizes: Dict[str, Tuple[float, float]],
    elk_direction: str,
    elk_timeout: int,
    allow_fallback: bool,
    debug_json: bool,
    page_size: Tuple[float, float],
    output_path: Path,
) -> Tuple[Dict[str, Tuple[float, float, float, float]], List[Dict[str, Any]], Dict[str, Dict[str, Tuple[float, float]]]]:
    skeleton_id = skeleton.get("skeletonId", "")
    is_pc_cc = skeleton_id.startswith("skeleton_v2_pc_cc_")
    is_target_cc = is_pc_cc
    target_cc_left_is_low = False
    target_cc_has_acc_branch = False
    enforced_sequence: List[str] = []
    layout_only_edges: List[Tuple[str, str]] = []
    effective_direction = elk_direction
    if is_pc_cc:
        enforced_sequence = build_pc_cc_layout_sequence(selection)
        if enforced_sequence:
            print(f"[LAYOUT] pc_cc order: {' -> '.join(enforced_sequence)}")
            layout_only_edges = list(zip(enforced_sequence, enforced_sequence[1:]))

    slot_ids = order_slot_ids_for_layout(selection)
    if enforced_sequence:
        slot_ids = enforced_sequence + [slot_id for slot_id in slot_ids if slot_id not in enforced_sequence]
    if is_pc_cc and effective_direction == "UP":
        required_nodes = {"PUMP", "PRV_A2B", "PRV_B2A", "CYL"}
        if required_nodes.issubset(set(selection.keys())):
            layout_only_edges = [
                ("PUMP", "PRV_A2B"),
                ("PUMP", "PRV_B2A"),
                ("PRV_A2B", "CYL"),
                ("PRV_B2A", "CYL"),
                ("PRV_A2B", "PRV_B2A"),
            ]
            print("[LAYOUT] pc_cc vertical ordering enforced (PUMP -> PRVs -> CYL)")
    tank_slots = find_tank_slots(selection)
    edges_info: List[Dict[str, Any]] = []
    for index, conn in enumerate(skeleton.get("connections", [])):
        from_slot = conn["from"]["slotId"]
        to_slot = conn["to"]["slotId"]
        if from_slot not in selection or to_slot not in selection:
            continue
        edge_id = f"e{index}"
        edges_info.append(
            {
                "id": edge_id,
                "fromSlot": from_slot,
                "toSlot": to_slot,
                "fromPort": conn["from"]["portKey"],
                "toPort": conn["to"]["portKey"],
            }
        )
    original_edges = edges_info
    junction_nodes: Dict[str, Dict[str, Any]] = {}
    junction_owner: Dict[str, str] = {}
    derived_edges: List[Dict[str, Any]] = original_edges
    junction_stats: List[Tuple[str, str, int, int, int]] = []
    port_degree_info: Dict[Tuple[str, str], Dict[str, int]] = {}
    incident_edges: Dict[Tuple[str, str], List[str]] = {}
    junction_edges_affected = 0
    if is_target_cc and {"PUMP", "PRV_A2B", "PRV_B2A", "CYL"}.issubset(set(selection.keys())):
        def find_edge_id(
            a_slot: str,
            a_port: str,
            b_slot: str,
            b_port: str,
        ) -> Optional[str]:
            for edge in original_edges:
                if (
                    edge["fromSlot"] == a_slot
                    and edge["fromPort"] == a_port
                    and edge["toSlot"] == b_slot
                    and edge["toPort"] == b_port
                ):
                    return edge["id"]
                if (
                    edge["fromSlot"] == b_slot
                    and edge["fromPort"] == b_port
                    and edge["toSlot"] == a_slot
                    and edge["toPort"] == a_port
                ):
                    return edge["id"]
            return None

        pump_port_high = "A"
        id_pump_cyl_p = find_edge_id("PUMP", "A", "CYL", "A")
        if id_pump_cyl_p is None:
            pump_port_high = "P"
            id_pump_cyl_p = find_edge_id("PUMP", "P", "CYL", "A")

        pump_port_low = "B"
        id_pump_cyl_s = find_edge_id("PUMP", "B", "CYL", "B")
        if id_pump_cyl_s is None:
            pump_port_low = "S"
            id_pump_cyl_s = find_edge_id("PUMP", "S", "CYL", "B")

        id_prv_a2b_p = find_edge_id("PRV_A2B", "P", "PUMP", pump_port_high)
        id_prv_a2b_t = find_edge_id("PRV_A2B", "T", "PUMP", pump_port_low)
        id_prv_b2a_p = find_edge_id("PRV_B2A", "P", "PUMP", pump_port_low)
        id_prv_b2a_t = find_edge_id("PRV_B2A", "T", "PUMP", pump_port_high)
        id_acc_to_pump = None
        if "ACC" in selection:
            id_acc_to_pump = find_edge_id("ACC", "P", "PUMP", pump_port_low)
            if id_acc_to_pump is None:
                id_acc_to_pump = find_edge_id("ACC", "P", "PUMP", pump_port_high)
        if None in {
            id_pump_cyl_p,
            id_pump_cyl_s,
            id_prv_a2b_p,
            id_prv_a2b_t,
            id_prv_b2a_p,
            id_prv_b2a_t,
        }:
            raise RuntimeError("[ELK] Missing expected connections for trunk routing.")

        target_cc_left_is_low = False
        junction_nodes = {
            "J_P_1": {"id": "J_P_1"},
            "J_P_2": {"id": "J_P_2"},
            "J_S_1": {"id": "J_S_1"},
            "J_S_2": {"id": "J_S_2"},
        }
        derived_edges = [
            {
                "id": "trunk_p_0",
                "fromSlot": "PUMP",
                "toSlot": "J_P_1",
                "fromPort": pump_port_high,
                "toPort": "J",
                "parentConnectionIds": [id_pump_cyl_p],
                "parentFromSlot": "PUMP",
                "parentToSlot": "CYL",
                "parentFromPort": pump_port_high,
                "parentToPort": "A",
            },
            {
                "id": "trunk_p_1",
                "fromSlot": "J_P_1",
                "toSlot": "J_P_2",
                "fromPort": "J",
                "toPort": "J",
                "parentConnectionIds": [id_pump_cyl_p],
                "parentFromSlot": "PUMP",
                "parentToSlot": "CYL",
                "parentFromPort": pump_port_high,
                "parentToPort": "A",
            },
            {
                "id": "trunk_p_2",
                "fromSlot": "J_P_2",
                "toSlot": "CYL",
                "fromPort": "J",
                "toPort": "A",
                "parentConnectionIds": [id_pump_cyl_p],
                "parentFromSlot": "PUMP",
                "parentToSlot": "CYL",
                "parentFromPort": pump_port_high,
                "parentToPort": "A",
            },
            {
                "id": "trunk_s_0",
                "fromSlot": "PUMP",
                "toSlot": "J_S_1",
                "fromPort": pump_port_low,
                "toPort": "J",
                "parentConnectionIds": [id_pump_cyl_s],
                "parentFromSlot": "PUMP",
                "parentToSlot": "CYL",
                "parentFromPort": pump_port_low,
                "parentToPort": "B",
            },
            {
                "id": "trunk_s_1",
                "fromSlot": "J_S_1",
                "toSlot": "J_S_2",
                "fromPort": "J",
                "toPort": "J",
                "parentConnectionIds": [id_pump_cyl_s],
                "parentFromSlot": "PUMP",
                "parentToSlot": "CYL",
                "parentFromPort": pump_port_low,
                "parentToPort": "B",
            },
            {
                "id": "trunk_s_2",
                "fromSlot": "J_S_2",
                "toSlot": "CYL",
                "fromPort": "J",
                "toPort": "B",
                "parentConnectionIds": [id_pump_cyl_s],
                "parentFromSlot": "PUMP",
                "parentToSlot": "CYL",
                "parentFromPort": pump_port_low,
                "parentToPort": "B",
            },
            {
                "id": "branch_a2b_p",
                "fromSlot": "PRV_A2B",
                "toSlot": "J_P_1",
                "fromPort": "P",
                "toPort": "J",
                "parentConnectionIds": [id_prv_a2b_p],
                "parentFromSlot": "PRV_A2B",
                "parentToSlot": "PUMP",
                "parentFromPort": "P",
                "parentToPort": pump_port_high,
            },
            {
                "id": "branch_b2a_t",
                "fromSlot": "PRV_B2A",
                "toSlot": "J_P_2",
                "fromPort": "T",
                "toPort": "J",
                "parentConnectionIds": [id_prv_b2a_t],
                "parentFromSlot": "PRV_B2A",
                "parentToSlot": "PUMP",
                "parentFromPort": "T",
                "parentToPort": pump_port_high,
            },
            {
                "id": "branch_a2b_t",
                "fromSlot": "PRV_A2B",
                "toSlot": "J_S_1",
                "fromPort": "T",
                "toPort": "J",
                "parentConnectionIds": [id_prv_a2b_t],
                "parentFromSlot": "PRV_A2B",
                "parentToSlot": "PUMP",
                "parentFromPort": "T",
                "parentToPort": pump_port_low,
            },
            {
                "id": "branch_b2a_p",
                "fromSlot": "PRV_B2A",
                "toSlot": "J_S_2",
                "fromPort": "P",
                "toPort": "J",
                "parentConnectionIds": [id_prv_b2a_p],
                "parentFromSlot": "PRV_B2A",
                "parentToSlot": "PUMP",
                "parentFromPort": "P",
                "parentToPort": pump_port_low,
            },
        ]
        if id_acc_to_pump is not None:
            target_cc_has_acc_branch = True
            derived_edges.append(
                {
                    "id": "branch_acc",
                    "fromSlot": "ACC",
                    "toSlot": "J_S_2",
                    "fromPort": "P",
                    "toPort": "J",
                    "parentConnectionIds": [id_acc_to_pump],
                    "parentFromSlot": "ACC",
                    "parentToSlot": "PUMP",
                    "parentFromPort": "P",
                    "parentToPort": pump_port_low,
                }
            )
        junction_stats = [
            (pump_port_high, "J_P_1", 3, 0, 0),
            (pump_port_high, "J_P_2", 3, 0, 0),
            (pump_port_low, "J_S_1", 3, 0, 0),
            (pump_port_low, "J_S_2", 3, 0, 0),
        ]
    elif JUNCTION_DEGREE_THRESHOLD and JUNCTION_DEGREE_THRESHOLD > 1:
        out_incidence: Dict[Tuple[str, str], List[str]] = {}
        in_incidence: Dict[Tuple[str, str], List[str]] = {}
        for edge in original_edges:
            out_key = (edge["fromSlot"], edge["fromPort"])
            in_key = (edge["toSlot"], edge["toPort"])
            out_incidence.setdefault(out_key, []).append(edge["id"])
            in_incidence.setdefault(in_key, []).append(edge["id"])

        neighbor_sets: Dict[Tuple[str, str], set] = {}
        seen_pairs: set = set()
        for edge in original_edges:
            a = (edge["fromSlot"], edge["fromPort"])
            b = (edge["toSlot"], edge["toPort"])
            incident_edges.setdefault(a, []).append(edge["id"])
            incident_edges.setdefault(b, []).append(edge["id"])
            if is_target_cc:
                key = tuple(sorted([a, b]))
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
            neighbor_sets.setdefault(a, set()).add(b)
            neighbor_sets.setdefault(b, set()).add(a)

        all_ports = set(out_incidence.keys()) | set(in_incidence.keys()) | set(neighbor_sets.keys())
        for key in all_ports:
            out_count = len(out_incidence.get(key, []))
            in_count = len(in_incidence.get(key, []))
            total = len(neighbor_sets.get(key, set()))
            port_degree_info[key] = {"out": out_count, "in": in_count, "total": total}

        junction_for_port: Dict[Tuple[str, str], str] = {}
        for key, info in port_degree_info.items():
            if info["total"] < JUNCTION_DEGREE_THRESHOLD:
                continue
            slot_id, port_key = key
            j_id = f"J_{slot_id}_{port_key}"
            junction_for_port[key] = j_id
            junction_nodes[j_id] = {"id": j_id}
            junction_owner[j_id] = slot_id
            junction_stats.append((slot_id, port_key, info["total"], info["out"], info["in"]))

        connector_edges: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        derived_edges = []
        counter = 0
        for edge in original_edges:
            src_key = (edge["fromSlot"], edge["fromPort"])
            dst_key = (edge["toSlot"], edge["toPort"])
            src_junction = junction_for_port.get(src_key)
            dst_junction = junction_for_port.get(dst_key)
            src_node = src_junction or edge["fromSlot"]
            dst_node = dst_junction or edge["toSlot"]
            if src_junction or dst_junction:
                junction_edges_affected += 1

            if src_junction:
                key = ("link", edge["fromSlot"], edge["fromPort"])
                connector = connector_edges.get(key)
                if connector is None:
                    connector = {
                        "id": f"{src_junction}_link",
                        "fromSlot": edge["fromSlot"],
                        "toSlot": src_junction,
                        "fromPort": edge["fromPort"],
                        "toPort": None,
                        "parentConnectionIds": list(dict.fromkeys(incident_edges.get(src_key, []))),
                        "kind": "connector",
                    }
                    connector_edges[key] = connector

            if dst_junction:
                key = ("link", edge["toSlot"], edge["toPort"])
                connector = connector_edges.get(key)
                if connector is None:
                    connector = {
                        "id": f"{dst_junction}_link",
                        "fromSlot": edge["toSlot"],
                        "toSlot": dst_junction,
                        "fromPort": edge["toPort"],
                        "toPort": None,
                        "parentConnectionIds": list(dict.fromkeys(incident_edges.get(dst_key, []))),
                        "kind": "connector",
                    }
                    connector_edges[key] = connector

            derived_edges.append(
                {
                    "id": f"{edge['id']}_j{counter}",
                    "fromSlot": src_node,
                    "toSlot": dst_node,
                    "fromPort": None if src_junction else edge["fromPort"],
                    "toPort": None if dst_junction else edge["toPort"],
                    "parentConnectionIds": [edge["id"]],
                    "parentFromSlot": edge["fromSlot"],
                    "parentToSlot": edge["toSlot"],
                    "parentFromPort": edge["fromPort"],
                    "parentToPort": edge["toPort"],
                }
            )
            counter += 1
        if connector_edges:
            derived_edges = list(connector_edges.values()) + derived_edges

        for (slot_id, port_key), j_id in junction_for_port.items():
            if not any(
                edge
                for edge in derived_edges
                if edge.get("kind") == "connector"
                and edge.get("fromSlot") == slot_id
                and edge.get("fromPort") == port_key
                and edge.get("toSlot") == j_id
            ):
                raise RuntimeError(
                    f"[ELK] Missing connector link for {slot_id}.{port_key} -> {j_id}"
                )
            for edge in derived_edges:
                if edge.get("kind") == "connector":
                    continue
                if edge.get("fromSlot") == slot_id or edge.get("toSlot") == slot_id:
                    raise RuntimeError(
                        f"[ELK] Edge {edge.get('id')} bypasses junction for {slot_id}.{port_key}"
                    )

    order_index = {slot_id: idx for idx, slot_id in enumerate(enforced_sequence)} if enforced_sequence else {}
    if junction_nodes:
        slot_ids.extend([node_id for node_id in sorted(junction_nodes.keys()) if node_id not in slot_ids])
    def layout_index(node_id: str) -> Optional[int]:
        if node_id in order_index:
            return order_index[node_id]
        owner = junction_owner.get(node_id)
        if owner in order_index:
            return order_index[owner]
        return None
    layout_edge_reversed: Dict[str, bool] = {}

    attempts = [
        (40, 80),
    ]
    last_error = None
    candidates: List[Dict[str, Any]] = []
    for attempt, (node_spacing, layer_spacing) in enumerate(attempts, start=1):
        print(
            f"[ELK] attempt={attempt} nodeNode={node_spacing} betweenLayers={layer_spacing} direction={effective_direction}"
        )
        result = None
        graph = None
        nodes: Dict[str, Tuple[float, float, float, float]] = {}
        output_ports: Dict[str, Dict[str, Tuple[float, float]]] = {}
        for flip in (False, True):
            layer_constraint = tank_layer_constraint(effective_direction, flip)
            graph = {
                "id": "root",
                "layoutOptions": {
                    "elk.algorithm": "layered",
                    "elk.edgeRouting": "ORTHOGONAL",
                    "elk.direction": effective_direction,
                    "elk.portConstraints": "FIXED_POS",
                    "elk.layered.considerModelOrder": "true",
                    "elk.layered.forceNodeModelOrder": "true",
                    "elk.layered.nodePlacement.strategy": "BRANDES_KOEPF",
                    "elk.spacing.nodeNode": str(node_spacing),
                    "elk.layered.spacing.nodeNodeBetweenLayers": str(layer_spacing),
                    "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
                    "elk.layered.nodePlacement.favorStraightEdges": "true",
                },
                "children": [],
                "edges": [],
            }
            for slot_id in slot_ids:
                if slot_id in junction_nodes:
                    node_w, node_h = (JUNCTION_NODE_SIZE, JUNCTION_NODE_SIZE)
                else:
                    node_w, node_h = node_sizes.get(slot_id, (DEFAULT_W, BASE_H))
                ports = []
                if is_target_cc and slot_id.startswith("J_"):
                    ports.append(
                        {
                            "id": f"{slot_id}:J",
                            "x": node_w / 2.0,
                            "y": node_h / 2.0,
                            "width": 1,
                            "height": 1,
                            "properties": {},
                        }
                    )
                else:
                    for port_key in required_ports.get(slot_id, []):
                        x, y = port_local[slot_id][port_key]
                        side = port_sides.get(slot_id, {}).get(port_key)
                        ports.append(
                            {
                                "id": f"{slot_id}:{port_key}",
                                "x": x,
                                "y": y,
                                "width": 1,
                                "height": 1,
                                "properties": {"org.eclipse.elk.port.side": side} if side else {},
                            }
                        )
                layout_opts = {"elk.portConstraints": "FIXED_POS"}
                if layer_constraint and slot_id in tank_slots:
                    layout_opts["elk.layered.layering.layerConstraint"] = layer_constraint
                graph["children"].append(
                    {
                        "id": slot_id,
                        "width": node_w,
                        "height": node_h,
                        "ports": ports,
                        "layoutOptions": layout_opts,
                    }
                )
            for edge in derived_edges:
                source = edge["fromSlot"]
                target = edge["toSlot"]
                source_port = edge.get("fromPort")
                target_port = edge.get("toPort")
                reversed_edge = False
                if order_index:
                    src_idx = layout_index(source)
                    tgt_idx = layout_index(target)
                    if src_idx is not None and tgt_idx is not None and src_idx > tgt_idx:
                        source, target = target, source
                        source_port, target_port = target_port, source_port
                        reversed_edge = True
                layout_edge_reversed[edge["id"]] = reversed_edge
                edge_obj = {
                    "id": edge["id"],
                    "source": source,
                    "target": target,
                }
                if source_port:
                    edge_obj["sourcePort"] = f"{source}:{source_port}"
                if target_port:
                    edge_obj["targetPort"] = f"{target}:{target_port}"
                graph["edges"].append(edge_obj)
            if layout_only_edges:
                for index, (src, dst) in enumerate(layout_only_edges):
                    graph["edges"].append(
                        {
                            "id": f"_layout_order_{index}",
                            "source": src,
                            "target": dst,
                            "layoutOptions": {"elk.edge.type": "ASSOCIATION"},
                            "properties": {"layoutOnly": "true"},
                        }
                    )
            junction_nodes_created = len(junction_nodes)
            edges_rewritten_count = len(derived_edges) - len(original_edges) if junction_nodes_created else 0
            out_count = len([entry for entry in junction_stats if entry[3] > 0])
            in_count = len([entry for entry in junction_stats if entry[4] > 0])
            ports_ge_threshold = len(
                [key for key, info in port_degree_info.items() if info["total"] >= JUNCTION_DEGREE_THRESHOLD]
            )
            top_ports = sorted(
                port_degree_info.items(), key=lambda item: item[1]["total"], reverse=True
            )[:5]
            print(
                "[ELK] junction threshold="
                f"{JUNCTION_DEGREE_THRESHOLD} ports>={JUNCTION_DEGREE_THRESHOLD}={ports_ge_threshold} "
                f"ports_with_out={out_count} ports_with_in={in_count}"
            )
            if top_ports:
                top_msg = ", ".join(
                    f"{slot}.{port}=total{info['total']}(out{info['out']},in{info['in']})"
                    for (slot, port), info in top_ports
                )
                print(f"[ELK] top port degrees: {top_msg}")
            print(
                "[ELK] junction nodes="
                f"{junction_nodes_created} edges_rewritten={edges_rewritten_count} "
                f"edges_affected={junction_edges_affected} final_nodes={len(graph['children'])} "
                f"final_edges={len(graph['edges'])}"
            )
            if graph["children"]:
                sample_nodes = ", ".join(child.get("id", "") for child in graph["children"][:10])
                print(f"[ELK] node sample: {sample_nodes}")
            if junction_nodes_created > 0:
                has_j_node = any(
                    str(child.get("id", "")).startswith("J_") for child in graph["children"]
                )
                has_j_edge = any(
                    str(edge.get("source", "")).startswith("J_")
                    or str(edge.get("target", "")).startswith("J_")
                    for edge in graph["edges"]
                )
                if not (has_j_node and has_j_edge):
                    raise RuntimeError(
                        "[ELK] Junction insertion lost: J_* nodes/edges missing from ELK input graph."
                    )
            try:
                result = run_elk_layout(graph, elk_timeout)
            except Exception as exc:
                print(f"[ELK] failed: {exc}")
                last_error = exc
                result = None
                continue

            nodes = {}
            output_ports = {}
            for child in result.get("children", []):
                node_id = child.get("id")
                if node_id not in selection and node_id not in junction_nodes:
                    continue
                node_w, node_h = node_sizes.get(node_id, (DEFAULT_W, BASE_H))
                if node_id in junction_nodes:
                    node_w, node_h = (JUNCTION_NODE_SIZE, JUNCTION_NODE_SIZE)
                nodes[node_id] = (
                    float(child.get("x", 0.0)),
                    float(child.get("y", 0.0)),
                    float(child.get("width", node_w)),
                    float(child.get("height", node_h)),
                )
                ports_out: Dict[str, Tuple[float, float]] = {}
                for port in child.get("ports", []) or []:
                    port_id = port.get("id")
                    if not port_id:
                        continue
                    if isinstance(port_id, str) and port_id.startswith(f"{node_id}:"):
                        port_key = port_id.split(":", 1)[1]
                    else:
                        port_key = port_id
                    px = float(port.get("x", 0.0))
                    py = float(port.get("y", 0.0))
                    pw = float(port.get("width", 1.0))
                    ph = float(port.get("height", 1.0))
                    ports_out[port_key] = (px + pw / 2.0, py + ph / 2.0)
                output_ports[node_id] = ports_out

            if tank_slots and layer_constraint:
                max_bottom = max((y + h) for (_x, y, _w, h) in nodes.values()) if nodes else 0.0
                tank_bottom = max(
                    (nodes[slot_id][1] + nodes[slot_id][3] for slot_id in tank_slots if slot_id in nodes),
                    default=None,
                )
                if tank_bottom is not None and max_bottom - tank_bottom > 0.5 and not flip:
                    print("[LAYOUT] tank not bottom-most; flipping layer constraint and retrying.")
                    continue
            break

        if result is None:
            continue

        if debug_json and graph is not None:
            if is_target_cc:
                output_path.with_suffix(f".elk_input.attempt{attempt}.json").write_text(
                    json.dumps(graph, indent=2),
                    encoding="utf-8",
                )
                output_path.with_suffix(f".elk_output.attempt{attempt}.json").write_text(
                    json.dumps(result, indent=2),
                    encoding="utf-8",
                )
            else:
                output_path.with_suffix(".elk_input.json").write_text(
                    json.dumps(graph, indent=2),
                    encoding="utf-8",
                )
                output_path.with_suffix(".elk_output.json").write_text(
                    json.dumps(result, indent=2),
                    encoding="utf-8",
                )

        edge_points: Dict[str, List[Tuple[float, float]]] = {}
        edge_junctions: Dict[str, List[Tuple[float, float]]] = {}
        for edge in result.get("edges", []):
            points: List[Tuple[float, float]] = []
            for section in edge.get("sections", []) or []:
                start = section.get("startPoint")
                end = section.get("endPoint")
                if start:
                    points.append((float(start["x"]), float(start["y"])))
                for bend in section.get("bendPoints", []) or []:
                    points.append((float(bend["x"]), float(bend["y"])))
                if end:
                    points.append((float(end["x"]), float(end["y"])))
            edge_id = edge.get("id")
            if points:
                if layout_edge_reversed.get(edge_id):
                    points = list(reversed(points))
                if is_target_cc:
                    points = simplify_polyline(points)
                edge_points[edge_id] = points
            junctions: List[Tuple[float, float]] = []
            for jp in edge.get("junctionPoints", []) or []:
                try:
                    junctions.append((float(jp["x"]), float(jp["y"])))
                except (KeyError, TypeError, ValueError):
                    continue
            if junctions and edge_id:
                edge_junctions[edge_id] = junctions

        port_globals: Dict[str, Dict[str, Tuple[float, float]]] = {}
        for slot_id, (x, y, _w, _h) in nodes.items():
            ports = {}
            for port_key, (px, py) in output_ports.get(slot_id, {}).items():
                ports[port_key] = (x + px, y + py)
            if not ports:
                for port_key, (px, py) in port_local.get(slot_id, {}).items():
                    ports[port_key] = (x + px + 0.5, y + py + 0.5)
            if not ports and is_target_cc and slot_id.startswith("J_"):
                ports["J"] = (x + (_w / 2.0), y + (_h / 2.0))
            port_globals[slot_id] = ports

        if is_target_cc and {"PUMP", "PRV_A2B", "PRV_B2A", "CYL"}.issubset(set(nodes.keys())):
            def center_x(node_id: str) -> float:
                nx, ny, nw, nh = nodes[node_id]
                return nx + nw / 2.0

            def center_y(node_id: str) -> float:
                nx, ny, nw, nh = nodes[node_id]
                return ny + nh / 2.0

            target_center = center_x("CYL")
            for comp_id in ("PUMP", "PRV_A2B", "PRV_B2A", "CYL"):
                x, y, w, h = nodes[comp_id]
                nodes[comp_id] = (target_center - w / 2.0, y, w, h)

            port_globals = {}
            for slot_id, (x, y, w, h) in nodes.items():
                if slot_id.startswith("J_"):
                    ports = {"J": (x + w / 2.0, y + h / 2.0)}
                else:
                    ports = {}
                    for port_key, (px, py) in output_ports.get(slot_id, {}).items():
                        ports[port_key] = (x + px, y + py)
                    if not ports:
                        for port_key, (px, py) in port_local.get(slot_id, {}).items():
                            ports[port_key] = (x + px + 0.5, y + py + 0.5)
                port_globals[slot_id] = ports

            pump_high_port = "P"
            pump_low_port = "S"
            for item in derived_edges:
                if item["id"] == "trunk_p_0":
                    pump_high_port = item["fromPort"]
                elif item["id"] == "trunk_s_0":
                    pump_low_port = item["fromPort"]

            pump_high_global = port_globals.get("PUMP", {}).get(pump_high_port)
            pump_low_global = port_globals.get("PUMP", {}).get(pump_low_port)
            if pump_high_global is None or pump_low_global is None:
                trunk_p_x = target_center - 20.0
                trunk_s_x = target_center + 20.0
            else:
                trunk_p_x = pump_high_global[0]
                trunk_s_x = pump_low_global[0]

            junction_x_map = {
                "J_P_1": trunk_p_x,
                "J_P_2": trunk_p_x,
                "J_S_1": trunk_s_x,
                "J_S_2": trunk_s_x,
            }
            for junction_id, trunk_x in junction_x_map.items():
                if junction_id in nodes:
                    jx, jy, jw, jh = nodes[junction_id]
                    target_y = None
                    if junction_id == "J_P_1":
                        target_y = port_globals.get("PRV_A2B", {}).get("P", (None, None))[1]
                    elif junction_id == "J_P_2":
                        target_y = port_globals.get("PRV_B2A", {}).get("T", (None, None))[1]
                    elif junction_id == "J_S_1":
                        target_y = port_globals.get("PRV_A2B", {}).get("T", (None, None))[1]
                    elif junction_id == "J_S_2":
                        target_y = port_globals.get("PRV_B2A", {}).get("P", (None, None))[1]
                    if target_y is None:
                        target_y = jy + jh / 2.0
                    nodes[junction_id] = (
                        trunk_x - jw / 2.0,
                        target_y - jh / 2.0,
                        jw,
                        jh,
                    )

            # ACC branch: connect to the lower junction on the left trunk
            # (matches legacy acceptable layout behavior).
            acc_target_junction = "J_S_1"
            if all(j in nodes for j in ("J_P_1", "J_P_2", "J_S_1", "J_S_2")):
                jp1_cx = nodes["J_P_1"][0] + nodes["J_P_1"][2] / 2.0
                js1_cx = nodes["J_S_1"][0] + nodes["J_S_1"][2] / 2.0
                left_prefix = "J_P" if jp1_cx <= js1_cx else "J_S"
                lower_a = f"{left_prefix}_1"
                lower_b = f"{left_prefix}_2"
                a_cy = nodes[lower_a][1] + nodes[lower_a][3] / 2.0
                b_cy = nodes[lower_b][1] + nodes[lower_b][3] / 2.0
                acc_target_junction = lower_a if a_cy >= b_cy else lower_b
            if target_cc_has_acc_branch:
                for item in derived_edges:
                    if item.get("id") == "branch_acc":
                        item["toSlot"] = acc_target_junction
                        item["toPort"] = "J"
                        break

            if target_cc_has_acc_branch and "ACC" in nodes and acc_target_junction in nodes:
                ax, ay, aw, ah = nodes["ACC"]
                jx, jy, jw, jh = nodes[acc_target_junction]
                acc_port = output_ports.get("ACC", {}).get("P")
                if acc_port is None:
                    acc_port = port_local.get("ACC", {}).get("P")
                acc_port_y = acc_port[1] if acc_port else ah / 2.0
                acc_center_y = jy + jh / 2.0
                component_left_x = min(
                    nodes.get("PUMP", (target_center, 0.0, 0.0, 0.0))[0],
                    nodes.get("PRV_A2B", (target_center, 0.0, 0.0, 0.0))[0],
                    nodes.get("PRV_B2A", (target_center, 0.0, 0.0, 0.0))[0],
                    nodes.get("CYL", (target_center, 0.0, 0.0, 0.0))[0],
                    trunk_s_x,
                )
                acc_x = component_left_x - aw - 24.0
                # Align ACC pressure port vertically with the target junction for a short, clean stub.
                acc_y = acc_center_y - acc_port_y
                nodes["ACC"] = (acc_x, acc_y, aw, ah)

            port_globals = {}
            for slot_id, (x, y, w, h) in nodes.items():
                ports = {}
                for port_key, (px, py) in output_ports.get(slot_id, {}).items():
                    ports[port_key] = (x + px, y + py)
                if not ports:
                    for port_key, (px, py) in port_local.get(slot_id, {}).items():
                        ports[port_key] = (x + px + 0.5, y + py + 0.5)
                if not ports and slot_id.startswith("J_"):
                    ports["J"] = (x + w / 2.0, y + h / 2.0)
                port_globals[slot_id] = ports

            pump_high_global = port_globals.get("PUMP", {}).get(pump_high_port)
            pump_low_global = port_globals.get("PUMP", {}).get(pump_low_port)
            if pump_high_global is None or pump_low_global is None:
                trunk_p_x = target_center - 20.0
                trunk_s_x = target_center + 20.0
            else:
                trunk_p_x = pump_high_global[0]
                trunk_s_x = pump_low_global[0]

            def make_vertical(x: float, y1: float, y2: float) -> List[Tuple[float, float]]:
                if abs(y1 - y2) <= 1e-6:
                    return [(x, y1)]
                return [(x, y1), (x, y2)]

            def make_orthogonal(start: Tuple[float, float], end: Tuple[float, float], mid_x: float) -> List[Tuple[float, float]]:
                sx, sy = start
                ex, ey = end
                points = [(sx, sy)]
                if abs(sx - mid_x) > 1e-6:
                    points.append((mid_x, sy))
                if abs(ey - sy) > 1e-6:
                    points.append((mid_x, ey))
                if abs(ex - mid_x) > 1e-6:
                    points.append((ex, ey))
                return simplify_polyline(points)

            edge_points_override: Dict[str, List[Tuple[float, float]]] = {}
            edge_points_override["trunk_p_0"] = make_orthogonal(
                port_globals["PUMP"][pump_high_port], port_globals["J_P_1"]["J"], trunk_p_x
            )
            edge_points_override["trunk_p_1"] = make_vertical(
                trunk_p_x, port_globals["J_P_1"]["J"][1], port_globals["J_P_2"]["J"][1]
            )
            edge_points_override["trunk_p_2"] = make_orthogonal(
                port_globals["J_P_2"]["J"], port_globals["CYL"]["A"], trunk_p_x
            )
            edge_points_override["trunk_s_0"] = make_orthogonal(
                port_globals["PUMP"][pump_low_port], port_globals["J_S_1"]["J"], trunk_s_x
            )
            edge_points_override["trunk_s_1"] = make_vertical(
                trunk_s_x, port_globals["J_S_1"]["J"][1], port_globals["J_S_2"]["J"][1]
            )
            edge_points_override["trunk_s_2"] = make_orthogonal(
                port_globals["J_S_2"]["J"], port_globals["CYL"]["B"], trunk_s_x
            )
            edge_points_override["branch_a2b_p"] = make_orthogonal(
                port_globals["PRV_A2B"]["P"], port_globals["J_P_1"]["J"], trunk_p_x
            )
            edge_points_override["branch_b2a_t"] = make_orthogonal(
                port_globals["PRV_B2A"]["T"], port_globals["J_P_2"]["J"], trunk_p_x
            )
            edge_points_override["branch_a2b_t"] = make_orthogonal(
                port_globals["PRV_A2B"]["T"], port_globals["J_S_1"]["J"], trunk_s_x
            )
            edge_points_override["branch_b2a_p"] = make_orthogonal(
                port_globals["PRV_B2A"]["P"], port_globals["J_S_2"]["J"], trunk_s_x
            )
            if target_cc_has_acc_branch and "ACC" in port_globals and acc_target_junction in port_globals:
                acc_mid_x = port_globals[acc_target_junction]["J"][0]
                edge_points_override["branch_acc"] = make_orthogonal(
                    port_globals["ACC"]["P"], port_globals[acc_target_junction]["J"], acc_mid_x
                )

            for edge_id, pts in edge_points_override.items():
                edge_points[edge_id] = pts
            edge_junctions = {}

        issues: List[str] = []
        for a_id, a_rect in nodes.items():
            for b_id, b_rect in nodes.items():
                if a_id >= b_id:
                    continue
                if rects_overlap(a_rect, b_rect):
                    issues.append(f"overlap:{a_id}:{b_id}")

        for edge in derived_edges:
            points = edge_points.get(edge["id"])
            if not points:
                issues.append(f"edge_missing:{edge['id']}")
                continue
            from_pos = port_globals.get(edge["fromSlot"], {}).get(edge["fromPort"])
            to_pos = port_globals.get(edge["toSlot"], {}).get(edge["toPort"])
            if from_pos:
                dist = math.hypot(points[0][0] - from_pos[0], points[0][1] - from_pos[1])
                if dist > PORT_ANCHOR_TOL:
                    issues.append(
                        "edge_port_mismatch:"
                        f"{edge['id']}:from expected=({from_pos[0]:.2f},{from_pos[1]:.2f}) "
                        f"actual=({points[0][0]:.2f},{points[0][1]:.2f}) dist={dist:.2f}"
                    )
            if to_pos:
                dist = math.hypot(points[-1][0] - to_pos[0], points[-1][1] - to_pos[1])
                if dist > PORT_ANCHOR_TOL:
                    issues.append(
                        "edge_port_mismatch:"
                        f"{edge['id']}:to expected=({to_pos[0]:.2f},{to_pos[1]:.2f}) "
                        f"actual=({points[-1][0]:.2f},{points[-1][1]:.2f}) dist={dist:.2f}"
                    )
            for idx in range(1, len(points)):
                p1, p2 = points[idx - 1], points[idx]
                if abs(p1[0] - p2[0]) > 1.0 and abs(p1[1] - p2[1]) > 1.0:
                    issues.append(f"edge_non_ortho:{edge['id']}")
                    break
            for node_id, rect in nodes.items():
                for idx in range(1, len(points)):
                    p1, p2 = points[idx - 1], points[idx]
                    if node_id in {edge["fromSlot"], edge["toSlot"]}:
                        port_pos = port_globals.get(node_id, {}).get(
                            edge["fromPort"] if node_id == edge["fromSlot"] else edge["toPort"]
                        )
                        if port_pos and (math.hypot(p1[0] - port_pos[0], p1[1] - port_pos[1]) <= 6.0 or math.hypot(p2[0] - port_pos[0], p2[1] - port_pos[1]) <= 6.0):
                            continue
                    if segment_intersects_rect(p1, p2, rect):
                        issues.append(f"edge_intersect:{edge['id']}:{node_id}")
                        break

        if issues:
            critical = []
            noncritical = []
            for issue in issues:
                if issue.startswith(("overlap:", "edge_missing:", "edge_intersect:", "edge_non_ortho:")):
                    critical.append(issue)
                else:
                    noncritical.append(issue)
            for issue in issues:
                print(f"[LAYOUT] {issue}")
            if critical:
                last_error = RuntimeError("ELK layout validation failed.")
                continue
            if noncritical:
                print("[WARN] Only port mismatches detected; continuing with layout.")

        print(f"[ELK] success attempt={attempt}")

        if is_target_cc:
            diagram_bbox = bbox_from_points(
                [pt for pts in edge_points.values() for pt in pts]
                + [(x, y) for (x, y, _w, _h) in nodes.values()]
                + [(x + w, y + h) for (x, y, w, h) in nodes.values()]
            )
            if diagram_bbox:
                dx0, dy0, dx1, dy1 = diagram_bbox
                diagram_w = max(dx1 - dx0, 1.0)
                diagram_h = max(dy1 - dy0, 1.0)
            else:
                diagram_w = diagram_h = 1.0
            total_bends = 0
            max_bends = 0
            outer_frame = 0
            for pts in edge_points.values():
                bends = count_bends(pts)
                total_bends += bends
                max_bends = max(max_bends, bends)
                bbox = bbox_from_points(pts)
                if bbox:
                    ex0, ey0, ex1, ey1 = bbox
                    if (ex1 - ex0) > 0.8 * diagram_w or (ey1 - ey0) > 0.8 * diagram_h:
                        outer_frame += 1
            pump_y = nodes.get("PUMP", (0.0, 0.0, 0.0, 0.0))[1] + nodes.get("PUMP", (0.0, 0.0, 0.0, 0.0))[3] / 2.0
            cyl_y = nodes.get("CYL", (0.0, 0.0, 0.0, 0.0))[1] + nodes.get("CYL", (0.0, 0.0, 0.0, 0.0))[3] / 2.0
            prv_ys = []
            for prv_id in ("PRV_A2B", "PRV_B2A"):
                if prv_id in nodes:
                    prv_ys.append(nodes[prv_id][1] + nodes[prv_id][3] / 2.0)
            vertical_ok = bool(prv_ys) and pump_y > max(prv_ys) and cyl_y < min(prv_ys)
            print(
                "[METRIC] attempt="
                f"{attempt} bends_total={total_bends} bends_max={max_bends} "
                f"outer_frame={outer_frame} junction_nodes={len(junction_nodes)} vertical_ok={vertical_ok}"
            )
            candidates.append(
                {
                    "attempt": attempt,
                    "nodes": nodes,
                    "edge_points": edge_points,
                    "edge_junctions": edge_junctions,
                    "port_globals": port_globals,
                    "graph": graph,
                    "result": result,
                    "metrics": {
                        "outer_frame": outer_frame,
                        "total_bends": total_bends,
                        "max_bends": max_bends,
                        "vertical_ok": vertical_ok,
                    },
                }
            )
            continue

        edges_render: List[Dict[str, Any]] = []
        for edge in derived_edges:
            points = edge_points.get(edge["id"], [])
            junctions = edge_junctions.get(edge["id"], [])
            edges_render.append({**edge, "points": points, "junctions": junctions})

        return nodes, edges_render, port_globals

    if is_target_cc and candidates:
        candidates.sort(
            key=lambda item: (
                not item["metrics"]["vertical_ok"],
                item["metrics"]["outer_frame"],
                item["metrics"]["total_bends"],
                item["metrics"]["max_bends"],
            )
        )
        best = candidates[0]
        if debug_json and best.get("graph") is not None and best.get("result") is not None:
            output_path.with_suffix(".elk_input.json").write_text(
                json.dumps(best["graph"], indent=2),
                encoding="utf-8",
            )
            output_path.with_suffix(".elk_output.json").write_text(
                json.dumps(best["result"], indent=2),
                encoding="utf-8",
            )
        print(
            "[ELK] selected attempt="
            f"{best['attempt']} bends_total={best['metrics']['total_bends']} "
            f"bends_max={best['metrics']['max_bends']} outer_frame={best['metrics']['outer_frame']} "
            f"vertical_ok={best['metrics']['vertical_ok']}"
        )
        edges_render: List[Dict[str, Any]] = []
        for edge in derived_edges:
            points = best["edge_points"].get(edge["id"], [])
            junctions = best["edge_junctions"].get(edge["id"], [])
            edges_render.append({**edge, "points": points, "junctions": junctions})
        return best["nodes"], edges_render, best["port_globals"]

    if allow_fallback:
        print("[WARN] ELK layout failed; falling back to simple layout.")
        nodes: Dict[str, Tuple[float, float, float, float]] = {}
        page_w, page_h = page_size
        positions = layout_positions_simple(skeleton, page_w, page_h, 60.0)
        for slot_id, (x, y) in positions.items():
            if slot_id in selection:
                node_w, node_h = node_sizes.get(slot_id, (DEFAULT_W, BASE_H))
                nodes[slot_id] = (x, y, node_w, node_h)
        edges_render = []
        for edge in original_edges:
            from_pos = port_local[edge["fromSlot"]][edge["fromPort"]]
            to_pos = port_local[edge["toSlot"]][edge["toPort"]]
            edges_render.append(
                {
                    **edge,
                    "points": [
                        (nodes[edge["fromSlot"]][0] + from_pos[0], nodes[edge["fromSlot"]][1] + from_pos[1]),
                        (nodes[edge["toSlot"]][0] + to_pos[0], nodes[edge["toSlot"]][1] + to_pos[1]),
                    ],
                    "junctions": [],
                }
            )
        port_globals = {
            slot_id: {
                pk: (nodes[slot_id][0] + px + 0.5, nodes[slot_id][1] + py + 0.5)
                for pk, (px, py) in ports.items()
            }
            for slot_id, ports in port_local.items()
        }
        return nodes, edges_render, port_globals

    raise RuntimeError("ELK layout failed after retries.") from last_error

def try_elk_layout(
    skeleton: Dict[str, Any],
    node_size: Tuple[float, float],
    width: float,
    height: float,
    margin: float,
) -> Optional[Dict[str, Tuple[float, float]]]:
    slots = [slot["slotId"] for slot in skeleton.get("componentSlots", [])]
    if not slots:
        return None
    graph = {
        "id": "root",
        "layoutOptions": {
            "elk.algorithm": "layered",
            "elk.direction": "RIGHT",
            "elk.spacing.nodeNode": "40",
            "elk.layered.spacing.nodeNodeBetweenLayers": "60",
        },
        "children": [{"id": slot_id, "width": node_size[0], "height": node_size[1]} for slot_id in slots],
        "edges": [
            {
                "id": f"e{index}",
                "sources": [conn["from"]["slotId"]],
                "targets": [conn["to"]["slotId"]],
            }
            for index, conn in enumerate(skeleton.get("connections", []))
        ],
    }

    script = (
        "const elk = new (require('elkjs/lib/elk.bundled.js'))();"
        "let data='';"
        "process.stdin.on('data', chunk => data += chunk);"
        "process.stdin.on('end', () => {"
        "const graph = JSON.parse(data);"
        "elk.layout(graph).then(result => {"
        "process.stdout.write(JSON.stringify(result));"
        "}).catch(err => {"
        "console.error(err);"
        "process.exit(1);"
        "});"
        "});"
    )

    node_exe = resolve_node_executable()
    if not node_exe:
        return None
    try:
        proc = subprocess.run(
            [node_exe, "-e", script],
            input=json.dumps(graph),
            text=True,
            capture_output=True,
            timeout=15,
            check=False,
            cwd=PROJECT_ROOT,
        )
    except FileNotFoundError:
        return None
    if proc.returncode != 0 or not proc.stdout:
        return None
    try:
        result = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    positions = {}
    for child in result.get("children", []):
        x = child.get("x")
        y = child.get("y")
        if x is None or y is None:
            continue
        positions[child["id"]] = (x + node_size[0] / 2, y + node_size[1] / 2)
    return normalize_positions(positions, width, height, margin)

def layout_positions(
    skeleton: Dict[str, Any], width: float, height: float, node_size: Tuple[float, float]
) -> Dict[str, Tuple[float, float]]:
    margin = 60.0
    elk_positions = try_elk_layout(skeleton, node_size, width, height, margin)
    if elk_positions:
        return elk_positions
    return layout_positions_simple(skeleton, width, height, margin)

def svg_data_uri(path: Path) -> str:
    data = path.read_bytes()
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"

def normalize_symbol_render_mode(mode: str) -> str:
    normalized = (mode or "").strip().lower()
    if normalized not in SYMBOL_RENDER_MODES:
        raise RuntimeError(f"Unsupported symbol render mode: {mode}")
    return normalized

def parse_symbol_key_csv(raw: Optional[str]) -> set[str]:
    if not raw:
        return set()
    keys: set[str] = set()
    for part in str(raw).split(","):
        key = normalize_symbol_key(part)
        if key:
            keys.add(key)
    return keys

def sanitize_svg_identifier(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", text or "")
    cleaned = cleaned.strip("_")
    if not cleaned:
        cleaned = "symbol"
    if cleaned[0].isdigit():
        cleaned = f"id_{cleaned}"
    return cleaned

def load_inline_symbol_template(symbol_path: Path) -> InlineSymbolTemplate:
    cached = INLINE_SYMBOL_CACHE.get(symbol_path)
    if cached is not None:
        return cached
    root = ET.parse(symbol_path).getroot()
    min_x, min_y, vb_w, vb_h = parse_viewbox(root)
    if vb_w <= 0 or vb_h <= 0:
        raise RuntimeError(f"Invalid viewBox for inline symbol: {symbol_path}")
    defs_children: List[ET.Element] = []
    content_children: List[ET.Element] = []
    for child in list(root):
        local_name = child.tag.split("}")[-1]
        if local_name == "defs":
            defs_children.extend(copy.deepcopy(grand) for grand in list(child))
            continue
        if local_name in {"title", "desc", "metadata", "namedview"}:
            continue
        content_children.append(copy.deepcopy(child))
    if not content_children:
        raise RuntimeError(f"Inline symbol has no drawable content: {symbol_path}")
    template = InlineSymbolTemplate(
        min_x=min_x,
        min_y=min_y,
        vb_w=vb_w,
        vb_h=vb_h,
        defs_children=defs_children,
        content_children=content_children,
    )
    INLINE_SYMBOL_CACHE[symbol_path] = template
    return template

def rewrite_svg_reference_text(text: str, id_map: Dict[str, str]) -> str:
    if not text or not id_map:
        return text
    rewritten = text
    for old_id, new_id in sorted(id_map.items(), key=lambda item: len(item[0]), reverse=True):
        rewritten = re.sub(
            rf"url\(\s*(['\"]?)#{re.escape(old_id)}\1\s*\)",
            lambda match: f"url({match.group(1)}#{new_id}{match.group(1)})",
            rewritten,
        )
        rewritten = re.sub(
            rf"(?<![\w-])#{re.escape(old_id)}(?![\w-])",
            f"#{new_id}",
            rewritten,
        )
    return rewritten

def rewrite_svg_ids_and_references(roots: List[ET.Element], prefix: str) -> Dict[str, str]:
    id_map: Dict[str, str] = {}
    for root in roots:
        for elem in root.iter():
            old_id = elem.get("id")
            if old_id and old_id not in id_map:
                id_map[old_id] = f"{prefix}__{old_id}"
    if not id_map:
        return id_map

    href_attr_names = {"href", f"{{{XLINK_NS}}}href"}
    for root in roots:
        for elem in root.iter():
            old_id = elem.get("id")
            if old_id in id_map:
                elem.set("id", id_map[old_id])
            for attr_name, attr_value in list(elem.attrib.items()):
                if attr_name == "id" or attr_value is None:
                    continue
                new_value = attr_value
                if attr_name in href_attr_names and attr_value.startswith("#"):
                    ref_id = attr_value[1:]
                    if ref_id in id_map:
                        new_value = f"#{id_map[ref_id]}"
                new_value = rewrite_svg_reference_text(new_value, id_map)
                if new_value != attr_value:
                    elem.set(attr_name, new_value)
            local_name = elem.tag.split("}")[-1]
            if local_name == "style" and elem.text:
                elem.text = rewrite_svg_reference_text(elem.text, id_map)
    return id_map

def render_symbol_image(
    parent: ET.Element,
    symbol_path: Path,
    x: float,
    y: float,
    w: float,
    h: float,
    rotation_deg: int,
) -> None:
    image = ET.SubElement(
        parent,
        f"{{{SVG_NS}}}image",
        {
            "x": str(x),
            "y": str(y),
            "width": str(w),
            "height": str(h),
            "preserveAspectRatio": "xMinYMin meet",
        },
    )
    data_uri = svg_data_uri(symbol_path)
    image.set(f"{{{XLINK_NS}}}href", data_uri)
    image.set("href", data_uri)
    if rotation_deg:
        cx = x + w / 2.0
        cy = y + h / 2.0
        image.set("transform", f"rotate({rotation_deg} {cx:.2f} {cy:.2f})")

def render_symbol_inline(
    parent: ET.Element,
    symbol_path: Path,
    slot_id: str,
    symbol_key: str,
    x: float,
    y: float,
    w: float,
    h: float,
    rotation_deg: int,
) -> None:
    template = load_inline_symbol_template(symbol_path)
    instance_prefix = sanitize_svg_identifier(f"{slot_id}_{symbol_key}")
    defs_children = [copy.deepcopy(child) for child in template.defs_children]
    content_children = [copy.deepcopy(child) for child in template.content_children]
    rewrite_svg_ids_and_references(defs_children + content_children, instance_prefix)

    wrapper = ET.SubElement(parent, f"{{{SVG_NS}}}g", {"id": f"symbol_{instance_prefix}"})
    defs_elem = ET.SubElement(wrapper, f"{{{SVG_NS}}}defs")
    for child in defs_children:
        defs_elem.append(child)
    clip_id = f"{instance_prefix}__clip"
    clip_path = ET.SubElement(
        defs_elem,
        f"{{{SVG_NS}}}clipPath",
        {"id": clip_id, "clipPathUnits": "userSpaceOnUse"},
    )
    ET.SubElement(
        clip_path,
        f"{{{SVG_NS}}}rect",
        {"x": str(x), "y": str(y), "width": str(w), "height": str(h)},
    )

    host_group = ET.SubElement(wrapper, f"{{{SVG_NS}}}g")
    host_group.set("clip-path", f"url(#{clip_id})")
    if rotation_deg:
        cx = x + w / 2.0
        cy = y + h / 2.0
        host_group.set("transform", f"rotate({rotation_deg} {cx:.2f} {cy:.2f})")

    scale_factor = min(w / template.vb_w, h / template.vb_h)
    content_transform = (
        f"translate({x:.6f} {y:.6f}) "
        f"scale({scale_factor:.6f}) "
        f"translate({-template.min_x:.6f} {-template.min_y:.6f})"
    )
    content_group = ET.SubElement(host_group, f"{{{SVG_NS}}}g", {"transform": content_transform})
    for child in content_children:
        content_group.append(child)

def format_num(value: Optional[float]) -> str:
    if value is None:
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    rounded = round(num, 1)
    if math.isclose(rounded, round(rounded), abs_tol=1e-9):
        return str(int(round(rounded)))
    return f"{rounded:.1f}"

def format_constraint_value(constraint: Optional[Constraint]) -> str:
    if constraint is None or constraint.value is None:
        return ""
    unit = constraint.unit or ""
    sep = " " if unit else ""
    return f"{format_num(constraint.value)}{sep}{unit}"

def add_text(parent: ET.Element, x: float, y: float, text: str, size: int = 10) -> None:
    elem = ET.SubElement(
        parent,
        f"{{{SVG_NS}}}text",
        {
            "x": str(x),
            "y": str(y),
            "font-size": str(size),
            "fill": "#000",
            "font-family": "Arial",
        },
    )
    elem.text = text

def parse_style_value(style: str, key: str) -> Optional[str]:
    for part in style.split(";"):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        if k.strip() == key:
            return v.strip()
    return None

def detect_template_background_fill(root: ET.Element) -> str:
    best_fill: Optional[str] = None
    best_area = 0.0
    for rect in root.findall(f".//{{{SVG_NS}}}rect"):
        fill = rect.get("fill")
        if not fill:
            style = rect.get("style", "")
            fill = parse_style_value(style, "fill")
        if not fill:
            continue
        fill_norm = fill.strip().lower()
        if fill_norm in {"none", "transparent"}:
            continue
        width = parse_float(rect.get("width"))
        height = parse_float(rect.get("height"))
        if width is None or height is None or width <= 0 or height <= 0:
            continue
        area = width * height
        if area > best_area:
            best_area = area
            best_fill = fill
    return best_fill or "#e6e6e6"

def estimate_text_bbox(x: float, y: float, text: str, size: float) -> Tuple[float, float, float, float]:
    width = max(1.0, len(text)) * size * LABEL_CHAR_WIDTH
    height = size * LABEL_HEIGHT_FACTOR
    return (x, y - height, width, height)

def build_wire_soft_rects(
    edges: List[Dict[str, Any]], margin: float
) -> List[Tuple[float, float, float, float]]:
    rects: List[Tuple[float, float, float, float]] = []
    for edge in edges:
        points = edge.get("points", [])
        for idx in range(1, len(points)):
            x1, y1 = points[idx - 1]
            x2, y2 = points[idx]
            min_x = min(x1, x2) - margin
            min_y = min(y1, y2) - margin
            max_x = max(x1, x2) + margin
            max_y = max(y1, y2) + margin
            rects.append((min_x, min_y, max_x - min_x, max_y - min_y))
    return rects

def choose_label_position(
    anchor: Tuple[float, float],
    text: str,
    size: float,
    hard_rects: List[Tuple[float, float, float, float]],
    soft_rects: List[Tuple[float, float, float, float]],
    placed_rects: List[Tuple[float, float, float, float]],
    candidate_radii: Optional[Tuple[float, ...]] = None,
) -> Tuple[float, float]:
    ax, ay = anchor
    candidates: List[Tuple[float, float]] = [(ax, ay)]
    dirs = [(0, -1), (0, 1), (-1, 0), (1, 0), (-1, -1), (1, -1), (-1, 1), (1, 1)]
    radii = candidate_radii or LABEL_CANDIDATE_RADII
    for radius in radii:
        for dx, dy in dirs:
            candidates.append((ax + dx * radius, ay + dy * radius))

    def hard_collides(bbox: Tuple[float, float, float, float]) -> bool:
        return any(rects_overlap(bbox, rect) for rect in hard_rects)

    def soft_collisions(bbox: Tuple[float, float, float, float]) -> int:
        count = 0
        for rect in soft_rects:
            if rects_overlap(bbox, rect, eps=0.0):
                count += 1
        for rect in placed_rects:
            if rects_overlap(bbox, rect, eps=0.0):
                count += 1
        return count

    best = None
    for cx, cy in candidates:
        bbox = estimate_text_bbox(cx, cy, text, size)
        hard = hard_collides(bbox)
        soft = soft_collisions(bbox)
        dist = math.hypot(cx - ax, cy - ay)
        score = (1 if hard else 0, soft, dist)
        if best is None or score < best[0]:
            best = (score, (cx, cy), bbox)

    if best is None:
        return ax, ay
    return best[1]

def choose_line_label_anchor(
    points: List[Tuple[float, float]], page_width: float
) -> Tuple[float, float]:
    if len(points) < 2:
        return points[0] if points else (0.0, 0.0)
    best_len = -1.0
    best_seg: Optional[Tuple[Tuple[float, float], Tuple[float, float]]] = None
    for idx in range(1, len(points)):
        p1 = points[idx - 1]
        p2 = points[idx]
        seg_len = math.hypot(p2[0] - p1[0], p2[1] - p1[1])
        if seg_len > best_len:
            best_len = seg_len
            best_seg = (p1, p2)
    if best_seg is None:
        return points[0]
    (x1, y1), (x2, y2) = best_seg
    mid_x = (x1 + x2) / 2.0
    mid_y = (y1 + y2) / 2.0
    is_left_half = mid_x < (page_width / 2.0)
    side_dx = -2.2 if is_left_half else 1.0
    if abs(x2 - x1) >= abs(y2 - y1):
        return mid_x + side_dx, mid_y - 1.6
    return mid_x + side_dx, mid_y - 0.9

def compute_junction_radius() -> float:
    return 1.0

def snap_edge_endpoints(
    edges: List[Dict[str, Any]],
    selection: Dict[str, ComponentAAS],
    internal_ports: Dict[str, Dict[str, Tuple[float, float]]],
    boundary_ports: Dict[str, Dict[str, Tuple[float, float]]],
) -> None:
    def orthogonal_stub(
        a: Tuple[float, float], b: Tuple[float, float], eps: float = 0.5
    ) -> List[Tuple[float, float]]:
        ax, ay = a
        bx, by = b
        if abs(ax - bx) <= eps or abs(ay - by) <= eps:
            return [a, b]
        mid1 = (ax, by)
        mid2 = (bx, ay)
        len1 = math.hypot(ax - mid1[0], ay - mid1[1]) + math.hypot(mid1[0] - bx, mid1[1] - by)
        len2 = math.hypot(ax - mid2[0], ay - mid2[1]) + math.hypot(mid2[0] - bx, mid2[1] - by)
        mid = mid1 if len1 <= len2 else mid2
        pts = [a]
        if math.hypot(mid[0] - ax, mid[1] - ay) > eps and math.hypot(mid[0] - bx, mid[1] - by) > eps:
            pts.append(mid)
        pts.append(b)
        return pts

    for edge in edges:
        points = edge.get("points", [])
        if len(points) < 2:
            continue
        from_slot = edge.get("fromSlot")
        to_slot = edge.get("toSlot")
        from_port = edge.get("fromPort")
        to_port = edge.get("toPort")

        # Junction endpoints must stay exactly on their junction anchor.
        if isinstance(from_slot, str) and from_slot.startswith("J_") and isinstance(from_port, str):
            anchor = boundary_ports.get(from_slot, {}).get(from_port)
            if anchor is not None:
                points[0] = anchor
        if isinstance(to_slot, str) and to_slot.startswith("J_") and isinstance(to_port, str):
            anchor = boundary_ports.get(to_slot, {}).get(to_port)
            if anchor is not None:
                points[-1] = anchor

        # Start: internal -> boundary -> existing outside route (boundary is points[0])
        if (
            from_slot in selection
            and isinstance(from_slot, str)
            and not from_slot.startswith("J_")
            and isinstance(from_port, str)
            and selection[from_slot].component_type not in INTERNAL_SNAP_EXCLUDED_COMPONENTS
        ):
            internal = internal_ports.get(from_slot, {}).get(from_port)
            if internal is not None:
                boundary = boundary_ports.get(from_slot, {}).get(from_port) or points[0]
                boundary = points[0]  # keep external ELK route unchanged
                stub = orthogonal_stub(internal, boundary)
                points = stub + points[1:]

        # End: keep outside route unchanged, then boundary -> internal
        if (
            to_slot in selection
            and isinstance(to_slot, str)
            and not to_slot.startswith("J_")
            and isinstance(to_port, str)
            and selection[to_slot].component_type not in INTERNAL_SNAP_EXCLUDED_COMPONENTS
        ):
            internal = internal_ports.get(to_slot, {}).get(to_port)
            if internal is not None:
                boundary = boundary_ports.get(to_slot, {}).get(to_port) or points[-1]
                boundary = points[-1]  # keep external ELK route unchanged
                stub = orthogonal_stub(boundary, internal)
                points = points[:-1] + stub

        edge["points"] = simplify_polyline(points)

def generate_diagram_svg(
    template_path: Path,
    output_path: Path,
    skeleton: Dict[str, Any],
    selection: Dict[str, ComponentAAS],
    symbol_mapping: Dict[str, Path],
    global_constraints: Dict[str, Any],
    layout_engine: str,
    allow_fallback: bool,
    elk_direction: str,
    elk_timeout: int,
    debug_ports: bool,
    debug_elk_json: bool,
    symbol_render_mode: str,
    symbol_force_image_keys: set[str],
    symbol_force_inline_keys: set[str],
    diagram_date: Optional[str] = None,
    title_block_values: Optional[Dict[str, str]] = None,
) -> None:
    render_mode = normalize_symbol_render_mode(symbol_render_mode)
    tree = ET.parse(template_path)
    root = tree.getroot()
    width, height = parse_svg_size(root)
    title_bbox = find_title_block_bbox(root)
    if title_bbox:
        margin = 3.0
        xmin, ymin, xmax, ymax = title_bbox
        xmin = max(0.0, xmin - margin)
        ymin = max(0.0, ymin - margin)
        xmax = min(width, xmax + margin)
        ymax = min(height, ymax + margin)
        title_bbox = (xmin, ymin, xmax, ymax)
        print(
            f"[LAYOUT] title_block_bbox xmin={xmin:.2f} ymin={ymin:.2f} xmax={xmax:.2f} ymax={ymax:.2f}"
        )

    group = ET.SubElement(root, f"{{{SVG_NS}}}g", {"id": "circuit-diagram"})
    draw_symbol_mask = render_mode == "image"
    symbol_mask_fill = detect_template_background_fill(root) if draw_symbol_mask else "none"

    required_ports = build_required_ports(skeleton, selection)
    port_local_internal: Dict[str, Dict[str, Tuple[float, float]]] = {}
    port_local_elk: Dict[str, Dict[str, Tuple[float, float]]] = {}
    port_sides: Dict[str, Dict[str, str]] = {}
    port_mappings: Dict[str, Dict[str, str]] = {}
    symbol_paths: Dict[str, Path] = {}
    symbol_keys: Dict[str, str] = {}
    node_sizes: Dict[str, Tuple[float, float]] = {}
    slot_rotations: Dict[str, int] = {
        slot.get("slotId"): normalize_rotation_deg(slot.get("graphics", {}).get("rotationDeg", 0))
        for slot in skeleton.get("componentSlots", [])
    }
    skeleton_id = skeleton.get("skeletonId", "")
    target_slots = {"PUMP", "PRV_A2B", "PRV_B2A", "CYL"}
    apply_pc_cc_rotation = (
        skeleton_id.startswith("skeleton_v2_pc_cc_")
        and target_slots.issubset(set(selection.keys()))
    )
    if apply_pc_cc_rotation:
        slot_rotations = {**slot_rotations, "PUMP": 270, "PRV_A2B": 270, "PRV_B2A": 90}
        print("[LAYOUT] Applied pc_cc rotation overrides for PUMP/PRVs.")

    for slot_id, comp in selection.items():
        rotation_deg = slot_rotations.get(slot_id, 0)
        symbol_key = normalize_symbol_key(
            get_property_value_by_semantic_id(comp, SYMBOL_KEY_SEMANTIC_ID)
        )
        symbol_keys[slot_id] = symbol_key or ""
        symbol_path = symbol_mapping.get(symbol_key or "")
        exists = bool(symbol_path and symbol_path.exists())
        print(f"[SYMBOL] slot={slot_id} key={symbol_key} mapped={symbol_path or None} exists={exists}")
        node_w = DEFAULT_W
        node_h = BASE_H
        if exists and symbol_path:
            try:
                symbol_root = ET.parse(symbol_path).getroot()
                _min_x, _min_y, vb_w, vb_h = parse_viewbox(symbol_root)
                if vb_w > 0 and vb_h > 0:
                    ar = vb_w / vb_h
                    node_w = clamp(BASE_H * ar, W_MIN, W_MAX)
            except Exception as exc:
                print(f"[WARN] Failed to read viewBox for {symbol_path}: {exc}; using default size.")
        size_multiplier = COMPONENT_SIZE_MULTIPLIERS.get(comp.component_type, 1.0)
        if size_multiplier != 1.0:
            node_w *= size_multiplier
            node_h *= size_multiplier
        node_sizes[slot_id] = (node_w, node_h)
        if not exists:
            if allow_fallback:
                print(f"[WARN] Missing symbol for slot {slot_id}; fallback enabled.")
                internal = {
                    port_key: rotate_point(node_w / 2.0, node_h / 2.0, node_w, node_h, rotation_deg)
                    for port_key in required_ports.get(slot_id, [])
                }
                port_local_internal[slot_id] = internal
                elk_ports: Dict[str, Tuple[float, float]] = {}
                side_map: Dict[str, str] = {}
                for port_key, (x_i, y_i) in internal.items():
                    print(
                        f"[PORTS] missing data-direction slot={slot_id} port={port_key}, using fallback."
                    )
                    x_b, y_b, side = project_to_nearest_boundary(x_i, y_i, node_w, node_h)
                    elk_ports[port_key] = (x_b, y_b)
                    side_map[port_key] = side
                port_local_elk[slot_id] = elk_ports
                port_sides[slot_id] = side_map
                continue
            raise RuntimeError(f"Missing symbol SVG for slot {slot_id} (key={symbol_key}).")
        symbol_paths[slot_id] = symbol_path

        available_ports = extract_available_svg_port_ids(comp)
        mapping = resolve_port_mapping(slot_id, required_ports.get(slot_id, []), available_ports, symbol_key or "")
        port_mappings[slot_id] = mapping
        print(f"[PORTS] slot={slot_id} mapping={mapping}")

        svg_ids = list({svg_id for svg_id in mapping.values()})
        coords, dir_hints = extract_port_local_meta(symbol_path, svg_ids, node_w, node_h)
        internal = {
            port_key: rotate_point(
                coords[svg_id][0],
                coords[svg_id][1],
                node_w,
                node_h,
                rotation_deg,
            )
            for port_key, svg_id in mapping.items()
        }
        port_dir = {
            port_key: rotate_dir_hint(dir_hints.get(svg_id), rotation_deg)
            for port_key, svg_id in mapping.items()
        }
        port_local_internal[slot_id] = internal
        elk_ports = {}
        side_map = {}
        for port_key, (x_i, y_i) in internal.items():
            dir_hint = port_dir.get(port_key)
            if dir_hint:
                x_b, y_b, side = project_to_boundary_with_direction(
                    x_i, y_i, node_w, node_h, dir_hint
                )
            else:
                print(
                    f"[PORTS] missing data-direction slot={slot_id} port={port_key}, using fallback."
                )
                x_b, y_b, side = project_to_nearest_boundary(x_i, y_i, node_w, node_h)
            elk_ports[port_key] = (x_b, y_b)
            side_map[port_key] = side
        port_local_elk[slot_id] = elk_ports
        port_sides[slot_id] = side_map
        print(f"[PORTS] slot={slot_id} internal={internal}")
        print(f"[PORTS] slot={slot_id} boundary={elk_ports}")
        print(f"[PORTS] slot={slot_id} sides={side_map}")

    if layout_engine != "elk":
        raise RuntimeError(f"Unsupported layout engine: {layout_engine}")

    nodes, edges, port_globals = layout_with_ports_elk(
        skeleton,
        selection,
        port_local_elk,
        port_sides,
        required_ports,
        node_sizes,
        elk_direction,
        elk_timeout,
        allow_fallback,
        debug_elk_json,
        (width, height),
        output_path,
    )

    scale, tx, ty = compute_fit_transform(nodes, edges, width, height, margin=40.0)

    nodes_t: Dict[str, Tuple[float, float, float, float]] = {}
    for slot_id, (x, y, w, h) in nodes.items():
        nx, ny = apply_transform_to_point((x, y), scale, tx, ty)
        nodes_t[slot_id] = (nx, ny, w * scale, h * scale)

    for edge in edges:
        edge["points"] = [apply_transform_to_point(pt, scale, tx, ty) for pt in edge["points"]]
        edge["junctions"] = [
            apply_transform_to_point(pt, scale, tx, ty) for pt in edge.get("junctions", [])
        ]

    port_globals_t: Dict[str, Dict[str, Tuple[float, float]]] = {}
    for slot_id, ports in port_globals.items():
        port_globals_t[slot_id] = {
            port_key: apply_transform_to_point(pos, scale, tx, ty) for port_key, pos in ports.items()
        }

    internal_globals_t: Dict[str, Dict[str, Tuple[float, float]]] = {}
    boundary_globals_t: Dict[str, Dict[str, Tuple[float, float]]] = {}
    for slot_id, (x, y, _w, _h) in nodes.items():
        internal_ports = {}
        boundary_ports = {}
        for port_key, (px, py) in port_local_internal.get(slot_id, {}).items():
            internal_ports[port_key] = apply_transform_to_point((x + px, y + py), scale, tx, ty)
        for port_key, (px, py) in port_local_elk.get(slot_id, {}).items():
            boundary_ports[port_key] = apply_transform_to_point((x + px, y + py), scale, tx, ty)
        if internal_ports:
            internal_globals_t[slot_id] = internal_ports
        if boundary_ports:
            boundary_globals_t[slot_id] = boundary_ports

    if title_bbox:
        diagram_bbox = compute_diagram_bbox(nodes_t, edges)
        if diagram_bbox and bbox_intersects(diagram_bbox, title_bbox):
            gap = 2.0
            shift_dx = 0.0
            shift_dy = 0.0
            dy = (title_bbox[1] - gap) - diagram_bbox[3]
            dx = (title_bbox[0] - gap) - diagram_bbox[2]
            if dy < 0 and diagram_bbox[1] + dy >= 0:
                test_bbox = shift_bbox(diagram_bbox, 0.0, dy)
                if not bbox_intersects(test_bbox, title_bbox):
                    shift_dy = dy
                elif dx < 0 and diagram_bbox[0] + dx >= 0:
                    combo_bbox = shift_bbox(test_bbox, dx, 0.0)
                    if not bbox_intersects(combo_bbox, title_bbox):
                        shift_dy = dy
                        shift_dx = dx
            if shift_dx == 0.0 and shift_dy == 0.0 and dx < 0 and diagram_bbox[0] + dx >= 0:
                test_bbox = shift_bbox(diagram_bbox, dx, 0.0)
                if not bbox_intersects(test_bbox, title_bbox):
                    shift_dx = dx
            if shift_dx != 0.0 or shift_dy != 0.0:
                print(f"[LAYOUT] avoid_title_block shift dx={shift_dx:.2f} dy={shift_dy:.2f}")
                for slot_id, (x, y, w, h) in list(nodes_t.items()):
                    nodes_t[slot_id] = (x + shift_dx, y + shift_dy, w, h)
                for edge in edges:
                    edge["points"] = [(x + shift_dx, y + shift_dy) for x, y in edge["points"]]
                    if edge.get("junctions"):
                        edge["junctions"] = [
                            (x + shift_dx, y + shift_dy) for x, y in edge["junctions"]
                        ]
                for slot_id, ports in port_globals_t.items():
                    port_globals_t[slot_id] = {
                        port_key: (px + shift_dx, py + shift_dy) for port_key, (px, py) in ports.items()
                    }
                for slot_id, ports in internal_globals_t.items():
                    internal_globals_t[slot_id] = {
                        port_key: (px + shift_dx, py + shift_dy) for port_key, (px, py) in ports.items()
                    }
                for slot_id, ports in boundary_globals_t.items():
                    boundary_globals_t[slot_id] = {
                        port_key: (px + shift_dx, py + shift_dy) for port_key, (px, py) in ports.items()
                    }
            else:
                print("[WARN] Unable to shift diagram away from title block.")

    snap_boundary_ports = {slot_id: dict(ports) for slot_id, ports in boundary_globals_t.items()}
    for slot_id, ports in port_globals_t.items():
        if slot_id.startswith("J_"):
            snap_boundary_ports[slot_id] = dict(ports)
    snap_edge_endpoints(edges, selection, internal_globals_t, snap_boundary_ports)
    hard_rects: List[Tuple[float, float, float, float]] = list(nodes_t.values())
    if title_bbox:
        hard_rects.append(
            (
                title_bbox[0],
                title_bbox[1],
                title_bbox[2] - title_bbox[0],
                title_bbox[3] - title_bbox[1],
            )
        )
    try:
        wire_margin = LABEL_SOFT_MARGIN + float(WIRE_STROKE_WIDTH)
    except ValueError:
        wire_margin = LABEL_SOFT_MARGIN + 1.0
    wire_soft_rects = build_wire_soft_rects(edges, wire_margin)
    wire_hard_rects = build_wire_soft_rects(edges, max(1.0, wire_margin * 0.85))
    wire_line_hard_rects = build_wire_soft_rects(edges, max(0.7, wire_margin * 0.55))
    placed_labels: List[Tuple[float, float, float, float]] = []
    line_label_requests: List[Tuple[float, float, str, float]] = []
    component_label_requests: List[Tuple[float, float, str, float]] = []

    port_role_map = build_port_role_map(skeleton)
    junction_points: List[Tuple[float, float]] = []
    for edge in edges:
        points = edge.get("points", [])
        if len(points) < 2:
            continue
        points_attr = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
        ET.SubElement(
            group,
            f"{{{SVG_NS}}}polyline",
            {
                "points": points_attr,
                "stroke": "#000",
                "stroke-width": WIRE_STROKE_WIDTH,
                "vector-effect": "non-scaling-stroke",
                "fill": "none",
            },
        )
        junction_points.extend(edge.get("junctions", []))
        if DRAW_PIPE_INTERFACE_LABELS and edge.get("kind") != "connector":
            parent_from = edge.get("parentFromSlot")
            parent_to = edge.get("parentToSlot")
            from_slot = parent_from or edge["fromSlot"]
            to_slot = parent_to or edge["toSlot"]
            from_port = edge.get("parentFromPort") or edge.get("fromPort")
            to_port = edge.get("parentToPort") or edge.get("toPort")
            if (
                from_slot in selection
                and to_slot in selection
                and isinstance(from_port, str)
                and isinstance(to_port, str)
            ):
                from_role = port_role_map.get(from_slot, {}).get(from_port)
                to_role = port_role_map.get(to_slot, {}).get(to_port)
                from_spec = get_port_spec(selection[from_slot], from_port, from_role)
                to_spec = get_port_spec(selection[to_slot], to_port, to_role)
                label = from_spec if from_spec and to_spec and from_spec == to_spec else ""
                if not label:
                    print(
                        f"[WARN] Missing InterfaceSpec for {from_slot}.{from_port} -> {to_slot}.{to_port}"
                    )
                if label:
                    label_x, label_y = choose_line_label_anchor(points, width)
                    line_label_requests.append((label_x, label_y, f"{label}", 3.0))

    for slot_id, comp in selection.items():
        node = nodes_t.get(slot_id)
        if node is None:
            continue
        x, y, w, h = node
        rotation_deg = slot_rotations.get(slot_id, 0)
        symbol_path = symbol_paths.get(slot_id)
        symbol_key = symbol_keys.get(slot_id, "")
        if draw_symbol_mask and comp.component_type != "BladderAccumulator":
            mask_rect = ET.SubElement(
                group,
                f"{{{SVG_NS}}}rect",
                {
                    "x": str(x),
                    "y": str(y),
                    "width": str(w),
                    "height": str(h),
                    "fill": symbol_mask_fill,
                    "stroke": "none",
                },
            )
            if rotation_deg:
                cx = x + w / 2.0
                cy = y + h / 2.0
                mask_rect.set("transform", f"rotate({rotation_deg} {cx:.2f} {cy:.2f})")
        if symbol_path:
            selected_mode = render_mode
            if symbol_key in symbol_force_image_keys:
                selected_mode = "image"
            elif symbol_key in symbol_force_inline_keys:
                selected_mode = "inline"

            if selected_mode == "image":
                render_symbol_image(group, symbol_path, x, y, w, h, rotation_deg)
            else:
                try:
                    render_symbol_inline(
                        group,
                        symbol_path,
                        slot_id,
                        symbol_key,
                        x,
                        y,
                        w,
                        h,
                        rotation_deg,
                    )
                except Exception as exc:
                    print(
                        f"[SYMBOL] inline_fallback slot={slot_id} key={symbol_key} "
                        f"reason={exc} path={symbol_path}"
                    )
                    render_symbol_image(group, symbol_path, x, y, w, h, rotation_deg)
        else:
            rect = ET.SubElement(
                group,
                f"{{{SVG_NS}}}rect",
                {
                    "x": str(x),
                    "y": str(y),
                    "width": str(w),
                    "height": str(h),
                    "fill": "none",
                    "stroke": "#000",
                },
            )
            if rotation_deg:
                cx = x + w / 2.0
                cy = y + h / 2.0
                rect.set("transform", f"rotate({rotation_deg} {cx:.2f} {cy:.2f})")
            component_label_requests.append((x + 4, y + 10, comp.component_type or slot_id, 3.0))

    for slot_id, ports in port_globals_t.items():
        if slot_id.startswith("J_"):
            j_pos = ports.get("J")
            if j_pos:
                junction_points.append(j_pos)

    if junction_points:
        radius = compute_junction_radius()
        for x, y in dedupe_points(junction_points, JUNCTION_DEDUP_TOL):
            ET.SubElement(
                group,
                f"{{{SVG_NS}}}circle",
                {"cx": str(x), "cy": str(y), "r": f"{radius:.2f}", "fill": "#000"},
            )

    if debug_ports:
        def draw_ports(port_map: Dict[str, Dict[str, Tuple[float, float]]], color: str) -> None:
            for slot_id, ports in port_map.items():
                for port_key, (x, y) in ports.items():
                    ET.SubElement(
                        group,
                        f"{{{SVG_NS}}}circle",
                        {"cx": str(x), "cy": str(y), "r": "2", "fill": color},
                    )
                    add_text(group, x + 2, y - 2, port_key, size=3.0)

        draw_ports(internal_globals_t, "#1f77b4")
        draw_ports(boundary_globals_t, "#ff7f0e")
        draw_ports(port_globals_t, "#d62728")

    tank_slot = next(
        (slot_id for slot_id, comp in selection.items() if comp.component_type == "Tank"), None
    )
    if tank_slot and tank_slot in nodes_t:
        x, y, w, h = nodes_t[tank_slot]
        tank_max = format_constraint_value(global_constraints.get("tankLevelMax"))
        tank_min = format_constraint_value(global_constraints.get("tankLevelMin"))
        fluid = global_constraints.get("hydraulicFluid")
        fluid_text = fluid.value_text if fluid else ""
        if tank_max:
            component_label_requests.append((x - 4, y + 2, f"max. {tank_max}", 3.0))
        if tank_min:
            component_label_requests.append((x - 4, y + 7, f"min. {tank_min}", 3.0))
        if fluid_text:
            component_label_requests.append((x - 4, y + 12, fluid_text, 3.0))

    pump_slot = next(
        (
            slot_id
            for slot_id, comp in selection.items()
            if comp.component_type in {"ConstantPump", "VariablePump"}
        ),
        None,
    )
    if pump_slot and pump_slot in nodes_t:
        comp = selection[pump_slot]
        validate_semantic_id(PUMP_FLOW_IRDI, "diagram_annotation")
        flow = comp.technical_properties.get(PUMP_FLOW_IRDI)
        if flow is not None:
            x, y, w, h = nodes_t[pump_slot]
            component_label_requests.append((x + w + 2, y + h / 2, f"{format_num(flow)} L/min", 3.0))

    prv_slot = next(
        (slot_id for slot_id, comp in selection.items() if comp.component_type == "PressureReliefValve"),
        None,
    )
    if prv_slot and prv_slot in nodes_t:
        comp = selection[prv_slot]
        validate_semantic_id(PRV_CRACKING_IRDI, "diagram_annotation")
        pressure = comp.technical_properties.get(PRV_CRACKING_IRDI)
        if pressure is not None:
            x, y, w, h = nodes_t[prv_slot]
            component_label_requests.append((x + w + 2, y + h / 2, f"{format_num(pressure)} bar", 3.0))

    cyl_slot = next(
        (
            slot_id
            for slot_id, comp in selection.items()
            if comp.component_type
            in {"Double-ActingCylinder", "SynchronousCylinder", "PlungerCylinder", "TelescopicCylinder"}
        ),
        None,
    )
    if cyl_slot and cyl_slot in nodes_t:
        comp = selection[cyl_slot]
        validate_semantic_id(ROD_DIAMETER_IRDI, "diagram_annotation")
        validate_semantic_id(CYL_STROKE_IRDI, "diagram_annotation")
        rod = comp.technical_properties.get(ROD_DIAMETER_IRDI)
        stroke = comp.technical_properties.get(CYL_STROKE_IRDI)
        if rod is not None and stroke is not None:
            x, y, w, h = nodes_t[cyl_slot]
            component_label_requests.append(
                (
                    x + w + 2,
                    y + h / 2,
                    f"{PHI_SYMBOL}{format_num(rod)} * {format_num(stroke)}",
                    3.0,
                )
            )

    component_hard_rects = list(hard_rects) + wire_hard_rects
    line_hard_rects = list(hard_rects) + wire_line_hard_rects
    for anchor_x, anchor_y, text, size in component_label_requests:
        if not text:
            continue
        x, y = choose_label_position(
            (anchor_x, anchor_y),
            text,
            size,
            component_hard_rects,
            [],
            placed_labels,
            candidate_radii=COMPONENT_LABEL_CANDIDATE_RADII,
        )
        bbox = estimate_text_bbox(x, y, text, size)
        placed_labels.append(bbox)
        add_text(group, x, y, text, size=size)

    for anchor_x, anchor_y, text, size in line_label_requests:
        if not text:
            continue
        x, y = choose_label_position(
            (anchor_x, anchor_y),
            text,
            size,
            line_hard_rects,
            wire_soft_rects,
            placed_labels,
            candidate_radii=LINE_LABEL_CANDIDATE_RADII,
        )
        bbox = estimate_text_bbox(x, y, text, size)
        placed_labels.append(bbox)
        add_text(group, x, y, text, size=size)

    if diagram_date:
        update_template_date(root, diagram_date)
    if title_block_values:
        update_template_title_block(root, title_block_values)

    tree.write(output_path, encoding="utf-8", xml_declaration=True)
