#!/usr/bin/env python3
from __future__ import annotations

import random
import re
from typing import Any, Dict, List, Optional, Tuple

from pipeline_03_automatic_diagram_generation import extract_available_svg_port_ids, resolve_port_mapping
from pipeline_04_aas_integration_adapter import (
    ACC_PRECHARGE_SEM,
    CHECK_VALVE_CRACKING_IRDI,
    CYL_FORCE_IRDI,
    CYL_PRESSURE_IRDI,
    CYL_STROKE_IRDI,
    DCV_FLOW_IRDI,
    DCV_PRESSURE_IRDI,
    GENERIC_FLOW_IRDI,
    GENERIC_PRESSURE_IRDI,
    PRV_CRACKING_IRDI,
    PUMP_FLOW_IRDI,
    PUMP_PRESSURE_IRDI,
    SYMBOL_KEY_SEMANTIC_ID,
    TANK_PRESSURE_IRDI,
    TANK_VOLUME_IRDI,
    ComponentAAS,
    Constraint,
    SemanticRecord,
    build_port_role_map,
    get_port_spec,
    get_property_value_by_semantic_id,
    normalize_symbol_key,
    normalize_value,
    validate_semantic_id,
)


def build_slot_order(skeleton: Dict[str, Any]) -> List[str]:
    degree: Dict[str, int] = {}
    for slot in skeleton.get("componentSlots", []):
        degree[slot["slotId"]] = 0
    for connection in skeleton.get("connections", []):
        degree[connection["from"]["slotId"]] += 1
        degree[connection["to"]["slotId"]] += 1
    return sorted(degree.keys(), key=lambda slot_id: (-degree[slot_id], slot_id))

def interface_compatible(
    slot_id: str,
    candidate: ComponentAAS,
    selected: Dict[str, ComponentAAS],
    connections: List[Dict[str, Any]],
    port_role_map: Dict[str, Dict[str, str]],
) -> Tuple[bool, List[Dict[str, Any]]]:
    evidence: List[Dict[str, Any]] = []
    for connection in connections:
        from_slot = connection["from"]["slotId"]
        to_slot = connection["to"]["slotId"]
        if slot_id not in {from_slot, to_slot}:
            continue
        if slot_id == from_slot:
            other_slot = to_slot
        else:
            other_slot = from_slot
        if other_slot not in selected:
            continue
        from_port = connection["from"]["portKey"]
        to_port = connection["to"]["portKey"]
        from_role = port_role_map.get(from_slot, {}).get(from_port)
        to_role = port_role_map.get(to_slot, {}).get(to_port)
        if slot_id == from_slot:
            from_comp = candidate
            to_comp = selected[to_slot]
        else:
            from_comp = selected[from_slot]
            to_comp = candidate
        from_spec = get_port_spec(from_comp, from_port, from_role)
        to_spec = get_port_spec(to_comp, to_port, to_role)
        match = (
            bool(from_spec)
            and bool(to_spec)
            and re.sub(r"\s+", "", from_spec.strip()).lower()
            == re.sub(r"\s+", "", to_spec.strip()).lower()
        )
        evidence.append(
            {
                "connection": f"{from_slot}.{from_port} -> {to_slot}.{to_port}",
                "leftSpec": from_spec or "",
                "rightSpec": to_spec or "",
                "match": match,
            }
        )
        if not match:
            return False, evidence
    return True, evidence

def candidate_has_required_ports(
    candidate: ComponentAAS,
    required_ports: List[Dict[str, Any]],
) -> Tuple[bool, List[str]]:
    missing: List[str] = []
    for port in required_ports:
        port_key = port.get("portKey")
        role_id = port.get("roleId")
        if not port_key:
            continue
        spec = get_port_spec(candidate, port_key, role_id)
        if not spec:
            missing.append(port_key)
    return not missing, missing

def lookup_semantic_record(candidate: ComponentAAS, semantic_id: str) -> Optional[SemanticRecord]:
    records = candidate.semantic_index.get(semantic_id, [])
    if not records:
        return None
    for record in records:
        if record.numeric_value is not None:
            return record
    return records[0]

def log_semantic_lookup(
    candidate: ComponentAAS,
    semantic_id: str,
    record: Optional[SemanticRecord],
) -> None:
    validate_semantic_id(semantic_id, "property_lookup")
    submodel = record.submodel_id_short if record else ""
    path = record.path if record else ""
    raw_value = record.raw_value if record else ""
    normalized = record.numeric_value if record else None
    unit = record.unit if record else ""
    raw_unit = record.raw_unit if record else ""
    print(
        "[LOOKUP] "
        f"aasId={candidate.aas_id} idShort={candidate.id_short or ''} "
        f"submodel={submodel} path={path} semanticId={semantic_id} "
        f"raw={raw_value!r} value={normalized} unit={unit} rawUnit={raw_unit}"
    )

def log_filter_decision(
    slot_id: str,
    candidate: ComponentAAS,
    rule: str,
    result: str,
    reason: str,
) -> None:
    print(
        f"[FILTER] slot={slot_id} candidate={candidate.aas_id} "
        f"rule={rule} result={result} reason={reason}"
    )

def log_selection_choice(
    slot_id: str,
    candidate: ComponentAAS,
    margins: Dict[str, Optional[float]],
    fallback: bool,
) -> None:
    margin_parts = [f"{key}={value}" for key, value in margins.items() if value is not None]
    margin_text = ", ".join(margin_parts) if margin_parts else "no margins"
    print(
        f"[RANK] slot={slot_id} selected={candidate.aas_id} "
        f"fallback={fallback} {margin_text}"
    )

def get_constraint_by_concept(
    constraints: List[Constraint], concept: str
) -> Optional[Constraint]:
    for constraint in constraints:
        if constraint.concept == concept and constraint.value is not None:
            return constraint
    return None

def normalize_constraint_value(constraint: Optional[Constraint]) -> Optional[float]:
    if not constraint or constraint.value is None:
        return None
    normalized, _unit = normalize_value(constraint.value, constraint.unit)
    return normalized

def check_requirement_min(
    slot_id: str,
    candidate: ComponentAAS,
    rule: str,
    semantic_id: str,
    requirement: Optional[float],
) -> Tuple[Optional[float], Optional[float], bool, bool]:
    if requirement is None:
        return None, None, True, False
    record = lookup_semantic_record(candidate, semantic_id)
    log_semantic_lookup(candidate, semantic_id, record)
    if record is None or record.numeric_value is None:
        log_filter_decision(slot_id, candidate, rule, "pass", "missing value")
        return None, None, True, True
    value = record.numeric_value
    if value >= requirement:
        margin = value - requirement
        log_filter_decision(slot_id, candidate, rule, "pass", f"value={value} >= required={requirement}")
        return value, margin, True, False
    log_filter_decision(slot_id, candidate, rule, "drop", f"value={value} < required={requirement}")
    return value, None, False, False

def check_requirement_max(
    slot_id: str,
    candidate: ComponentAAS,
    rule: str,
    semantic_id: str,
    requirement: Optional[float],
) -> Tuple[Optional[float], Optional[float], bool, bool]:
    if requirement is None:
        return None, None, True, False
    record = lookup_semantic_record(candidate, semantic_id)
    log_semantic_lookup(candidate, semantic_id, record)
    if record is None or record.numeric_value is None:
        log_filter_decision(slot_id, candidate, rule, "pass", "missing value")
        return None, None, True, True
    value = record.numeric_value
    if value <= requirement:
        margin = requirement - value
        log_filter_decision(slot_id, candidate, rule, "pass", f"value={value} <= required={requirement}")
        return value, margin, True, False
    log_filter_decision(slot_id, candidate, rule, "drop", f"value={value} > required={requirement}")
    return value, None, False, False

def check_requirement_gt(
    slot_id: str,
    candidate: ComponentAAS,
    rule: str,
    semantic_id: str,
    requirement: Optional[float],
) -> Tuple[Optional[float], Optional[float], bool, bool]:
    if requirement is None:
        return None, None, True, False
    record = lookup_semantic_record(candidate, semantic_id)
    log_semantic_lookup(candidate, semantic_id, record)
    if record is None or record.numeric_value is None:
        log_filter_decision(slot_id, candidate, rule, "pass", "missing value")
        return None, None, True, True
    value = record.numeric_value
    if value > requirement:
        margin = value - requirement
        log_filter_decision(slot_id, candidate, rule, "pass", f"value={value} > required={requirement}")
        return value, margin, True, False
    log_filter_decision(slot_id, candidate, rule, "drop", f"value={value} <= required={requirement}")
    return value, None, False, False

def build_margin_key(
    record: Dict[str, Any],
    margin_keys: List[str],
    any_numeric: bool,
) -> Tuple[Any, ...]:
    if not any_numeric:
        return (record["candidate"].aas_id,)
    parts: List[Any] = []
    for key in margin_keys:
        margin = record["margins"].get(key)
        parts.append(1 if margin is None else 0)
        parts.append(margin if margin is not None else float("inf"))
    parts.append(record["candidate"].aas_id)
    return tuple(parts)

def filter_candidates_for_slot(
    slot_id: str,
    slot: Dict[str, Any],
    candidates: List[ComponentAAS],
    requirements: Dict[str, Optional[float]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    component_type = slot.get("componentType", "")
    records: List[Dict[str, Any]] = []
    stats = {
        "candidateCount": len(candidates),
        "candidateCountAfterNumeric": 0,
        "numericFallback": False,
    }
    any_numeric = False

    for candidate in candidates:
        record: Dict[str, Any] = {"candidate": candidate, "values": {}, "margins": {}}
        dropped = False

        if component_type == "Tank":
            volume_req = requirements.get("tankNominalVolume")
            pressure_req = requirements.get("maxOperatingPressure")
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "tank_nominal_volume", TANK_VOLUME_IRDI, volume_req
            )
            record["values"]["volume"] = value
            record["margins"]["volume"] = margin
            if not passed:
                dropped = True
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "tank_max_pressure", TANK_PRESSURE_IRDI, pressure_req
            )
            record["values"]["pressure"] = value
            record["margins"]["pressure"] = margin
            if not passed:
                dropped = True
        elif component_type in {"ConstantPump", "VariablePump"}:
            flow_req = requirements.get("ratedFlowRate")
            pressure_req = requirements.get("maxOperatingPressure")
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "pump_flow_rate", PUMP_FLOW_IRDI, flow_req
            )
            record["values"]["flow"] = value
            record["margins"]["flow"] = margin
            if not passed:
                dropped = True
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "pump_outlet_pressure", PUMP_PRESSURE_IRDI, pressure_req
            )
            record["values"]["pressure"] = value
            record["margins"]["pressure"] = margin
            if not passed:
                dropped = True
        elif component_type in {
            "Double-ActingCylinder",
            "SynchronousCylinder",
            "PlungerCylinder",
            "TelescopicCylinder",
        }:
            pressure_req = requirements.get("maxOperatingPressure")
            force_req = requirements.get("cylinderLoad")
            stroke_req = requirements.get("cylinderStroke")
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "cylinder_max_pressure", CYL_PRESSURE_IRDI, pressure_req
            )
            record["values"]["pressure"] = value
            record["margins"]["pressure"] = margin
            if not passed:
                dropped = True
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "cylinder_force", CYL_FORCE_IRDI, force_req
            )
            record["values"]["force"] = value
            record["margins"]["force"] = margin
            if not passed:
                dropped = True
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "cylinder_stroke", CYL_STROKE_IRDI, stroke_req
            )
            record["values"]["stroke"] = value
            record["margins"]["stroke"] = margin
            if not passed:
                dropped = True
        elif component_type in {"4-3DirectionalControlValve", "3-2DirectionalControlValve"}:
            flow_req = requirements.get("ratedFlowRate")
            pressure_req = requirements.get("maxOperatingPressure")
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "dcv_flow_rate", DCV_FLOW_IRDI, flow_req
            )
            record["values"]["flow"] = value
            record["margins"]["flow"] = margin
            if not passed:
                dropped = True
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "dcv_max_pressure", DCV_PRESSURE_IRDI, pressure_req
            )
            record["values"]["pressure"] = value
            record["margins"]["pressure"] = margin
            if not passed:
                dropped = True
        elif component_type == "PressureReliefValve":
            setpoint_req = requirements.get("prvSetpoint")
            value, margin, passed, _missing = check_requirement_max(
                slot_id, candidate, "prv_cracking_pressure", PRV_CRACKING_IRDI, setpoint_req
            )
            record["values"]["cracking"] = value
            record["margins"]["cracking"] = margin
            if not passed:
                dropped = True
        elif component_type == "CheckValve":
            pressure_req = requirements.get("maxOperatingPressure")
            # TODO: Confirm check valve rule; placeholder per requirement.
            print("[WARN] Check valve rule is a temporary placeholder; review later.")
            value, margin, passed, _missing = check_requirement_gt(
                slot_id, candidate, "check_valve_cracking_pressure", CHECK_VALVE_CRACKING_IRDI, pressure_req
            )
            record["values"]["cracking"] = value
            record["margins"]["cracking"] = margin
            if not passed:
                dropped = True
        elif component_type == "BladderAccumulator":
            volume_req = requirements.get("accNominalVolume")
            precharge_req = requirements.get("accPreChargePressure")
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "acc_volume", TANK_VOLUME_IRDI, volume_req
            )
            record["values"]["volume"] = value
            record["margins"]["volume"] = margin
            if not passed:
                dropped = True
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "acc_precharge", ACC_PRECHARGE_SEM, precharge_req
            )
            record["values"]["precharge"] = value
            record["margins"]["precharge"] = margin
            if not passed:
                dropped = True
        else:
            flow_req = requirements.get("ratedFlowRate")
            pressure_req = requirements.get("maxOperatingPressure")
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "generic_flow_rate", GENERIC_FLOW_IRDI, flow_req
            )
            record["values"]["flow"] = value
            record["margins"]["flow"] = margin
            if not passed:
                dropped = True
            value, margin, passed, _missing = check_requirement_min(
                slot_id, candidate, "generic_max_pressure", GENERIC_PRESSURE_IRDI, pressure_req
            )
            record["values"]["pressure"] = value
            record["margins"]["pressure"] = margin
            if not passed:
                dropped = True

        if dropped:
            continue

        if any(value is not None for value in record["values"].values()):
            any_numeric = True
        records.append(record)

    if not records:
        return [], stats

    if not any_numeric:
        stats["numericFallback"] = True
        print(f"[WARN] No numeric data for slot {slot_id}; fallback to deterministic order.")

    if component_type == "Tank":
        margin_keys = ["volume", "pressure"]
    elif component_type in {"ConstantPump", "VariablePump"}:
        margin_keys = ["flow", "pressure"]
    elif component_type in {
        "Double-ActingCylinder",
        "SynchronousCylinder",
        "PlungerCylinder",
        "TelescopicCylinder",
    }:
        margin_keys = ["pressure", "force", "stroke"]
    elif component_type in {"4-3DirectionalControlValve", "3-2DirectionalControlValve"}:
        margin_keys = ["flow", "pressure"]
    elif component_type == "PressureReliefValve":
        margin_keys = ["cracking"]
    elif component_type == "CheckValve":
        margin_keys = ["cracking"]
    elif component_type == "BladderAccumulator":
        margin_keys = ["volume", "precharge"]
    else:
        margin_keys = ["flow", "pressure"]

    records.sort(key=lambda rec: build_margin_key(rec, margin_keys, any_numeric))
    stats["candidateCountAfterNumeric"] = len(records)
    return records, stats

def select_components(
    skeleton: Dict[str, Any],
    candidates_by_asset: Dict[str, List[ComponentAAS]],
    requirements: Dict[str, Optional[float]],
) -> Tuple[Dict[str, ComponentAAS], List[Dict[str, Any]], List[Dict[str, Any]]]:
    slot_map = {slot["slotId"]: slot for slot in skeleton.get("componentSlots", [])}
    connections = skeleton.get("connections", [])
    slot_order = build_slot_order(skeleton)
    port_role_map = build_port_role_map(skeleton)
    selection: Dict[str, ComponentAAS] = {}
    selection_results: Dict[str, Dict[str, Any]] = {}
    backtracking_log: List[Dict[str, Any]] = []

    candidate_records_by_slot: Dict[str, List[Dict[str, Any]]] = {}
    for slot_id, slot in slot_map.items():
        asset_type = slot.get("assetType", "")
        candidates = candidates_by_asset.get(asset_type, [])
        filtered, stats = filter_candidates_for_slot(slot_id, slot, candidates, requirements)
        candidate_records_by_slot[slot_id] = filtered
        selection_results[slot_id] = {
            "slotId": slot_id,
            "componentType": slot.get("componentType", ""),
            "assetType": asset_type,
            "candidateCount": stats["candidateCount"],
            "candidateCountAfterNumeric": stats["candidateCountAfterNumeric"],
            "numericFallback": stats["numericFallback"],
        }

    def backtrack(index: int) -> bool:
        if index >= len(slot_order):
            return True
        slot_id = slot_order[index]
        slot = slot_map[slot_id]
        required_ports = slot.get("ports", [])
        records = candidate_records_by_slot.get(slot_id, [])
        if not records:
            print(f"[ERROR] No candidates after numeric filtering for slot {slot_id}.")
            return False

        for record in records:
            candidate = record["candidate"]
            has_ports, missing_ports = candidate_has_required_ports(candidate, required_ports)
            if not has_ports:
                log_filter_decision(
                    slot_id,
                    candidate,
                    "interface_ports",
                    "drop",
                    f"missing ports: {', '.join(missing_ports)}",
                )
                continue
            log_filter_decision(slot_id, candidate, "interface_ports", "pass", "required ports present")
            compatible, interface_evidence = interface_compatible(
                slot_id, candidate, selection, connections, port_role_map
            )
            if not compatible:
                log_filter_decision(slot_id, candidate, "interface_match", "drop", "interface mismatch")
                continue
            log_filter_decision(slot_id, candidate, "interface_match", "pass", "interface matched")

            selection[slot_id] = candidate
            selection_results[slot_id]["selected"] = {
                "aasId": candidate.aas_id,
                "aasFile": candidate.aas_file,
                "supplier": candidate.supplier or "",
            }
            selection_results[slot_id]["interfaceEvidence"] = interface_evidence
            if backtrack(index + 1):
                return True
            backtracking_log.append(
                {
                    "slotId": slot_id,
                    "candidateAasId": candidate.aas_id,
                    "reason": "downstream slot had no viable candidates",
                }
            )
            selection.pop(slot_id, None)
        return False

    if not backtrack(0):
        raise RuntimeError("Component selection failed with given constraints and interfaces.")

    for slot_id, candidate in selection.items():
        record = next(
            (rec for rec in candidate_records_by_slot.get(slot_id, []) if rec["candidate"] == candidate),
            None,
        )
        margins = record.get("margins", {}) if record else {}
        fallback = selection_results.get(slot_id, {}).get("numericFallback", False)
        log_selection_choice(slot_id, candidate, margins, fallback)

    results_list = [selection_results[slot_id] for slot_id in slot_order if slot_id in selection_results]
    return selection, results_list, backtracking_log

def select_components_random_by_type(
    skeleton: Dict[str, Any],
    candidates_by_asset: Dict[str, List[ComponentAAS]],
    seed: Optional[int] = None,
) -> Tuple[Dict[str, ComponentAAS], List[Dict[str, Any]], List[Dict[str, Any]]]:
    rng = random.Random(seed)
    by_type: Dict[str, List[ComponentAAS]] = {}
    for components in candidates_by_asset.values():
        for comp in components:
            by_type.setdefault(comp.component_type, []).append(comp)

    selection: Dict[str, ComponentAAS] = {}
    selection_results: List[Dict[str, Any]] = []
    for slot in skeleton.get("componentSlots", []):
        slot_id = slot.get("slotId", "")
        component_type = slot.get("componentType", "")
        required_ports = [p.get("portKey", "") for p in slot.get("ports", []) if p.get("portKey")]
        pool = by_type.get(component_type, [])
        if not pool:
            raise RuntimeError(f"No candidates found for componentType '{component_type}' (slot {slot_id}).")

        valid_pool: List[ComponentAAS] = []
        for candidate in pool:
            available_ports = extract_available_svg_port_ids(candidate)
            symbol_key = normalize_symbol_key(
                get_property_value_by_semantic_id(candidate, SYMBOL_KEY_SEMANTIC_ID)
            ) or ""
            try:
                resolve_port_mapping(slot_id, required_ports, available_ports, symbol_key)
            except RuntimeError:
                continue
            valid_pool.append(candidate)

        if not valid_pool:
            raise RuntimeError(
                f"No candidates with required ports for slot {slot_id} ({component_type}); required={required_ports}"
            )

        chosen = rng.choice(valid_pool)
        selection[slot_id] = chosen
        selection_results.append(
            {
                "slotId": slot_id,
                "requiredAssetType": slot.get("assetType", ""),
                "candidateCount": len(valid_pool),
                "selectedAasId": chosen.aas_id,
                "matchingEvidence": {
                    "selectionMode": "random_by_component_type",
                    "componentType": component_type,
                    "requiredPorts": required_ports,
                    "seed": seed,
                },
            }
        )
    return selection, selection_results, []
