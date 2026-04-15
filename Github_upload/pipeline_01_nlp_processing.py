#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline_04_aas_integration_adapter import (
    ACC_PRECHARGE_SEM,
    CONCEPT_KEYWORDS,
    Constraint,
    TANK_VOLUME_IRDI,
    CYL_FORCE_IRDI,
    CYL_STROKE_IRDI,
    GENERIC_FLOW_IRDI,
    GENERIC_PRESSURE_IRDI,
    PRV_CRACKING_IRDI,
    extract_properties_from_aasx,
    detect_unit_from_text,
    load_json,
    parse_float,
    register_known_semantic_ids,
    render_prompt,
    validate_semantic_id,
)


AUTHORITATIVE_CONCEPT_SEMANTIC_IDS: Dict[str, List[str]] = {
    "maxOperatingPressure": [GENERIC_PRESSURE_IRDI, "0173-1#02-ABC510#003", "0173-1#02-ABI682#002"],
    "ratedFlowRate": [GENERIC_FLOW_IRDI, "0173-1#02-AAZ826#003", "0173-1#02-AAZ845#003"],
    "hydraulicFluid": ["0173-1#02-AAR454#003"],
    "cylinderLoad": [CYL_FORCE_IRDI],
    "cylinderStroke": [CYL_STROKE_IRDI],
    "tankNominalVolume": [TANK_VOLUME_IRDI],
    "prvSetpoint": [PRV_CRACKING_IRDI],
    "accNominalVolume": [TANK_VOLUME_IRDI],
    "accPreChargePressure": [ACC_PRECHARGE_SEM],
}

AUTHORITATIVE_LABELS: Dict[str, str] = {
    GENERIC_PRESSURE_IRDI: "MaxOperatingPressure",
    "0173-1#02-ABC510#003": "SpecifiedMaximumOutletPressure",
    "0173-1#02-ABI682#002": "MaxOperatingPressure",
    GENERIC_FLOW_IRDI: "NominalFlowRate",
    "0173-1#02-AAZ826#003": "SpecifiedMaximumTotalFlowRate",
    "0173-1#02-AAZ845#003": "NominalFlowRate",
    "0173-1#02-AAR454#003": "HydraulicFluid",
    CYL_FORCE_IRDI: "RatedCylinderAdvancingForce",
    CYL_STROKE_IRDI: "RatedStroke",
    TANK_VOLUME_IRDI: "NominalVolume",
    PRV_CRACKING_IRDI: "NominalCrackingPressure",
    ACC_PRECHARGE_SEM: "PreChargePressure",
}

CONCEPT_SEMANTIC_ALIASES: Dict[str, Dict[str, str]] = {
    "ratedFlowRate": {
        "urn:sdf:cd:hydraulic:SpecifiedMaximumFlowRate:1.0": GENERIC_FLOW_IRDI,
    },
    "hydraulicFluid": {
        "urn:sdf:cd:hydraulic:hydraulicFluid:1.0": "0173-1#02-AAR454#003",
        "urn:sdf:cd:hydraulic:HydraulicFluid:1.0": "0173-1#02-AAR454#003",
        "urn:sdf:cd:hydraulic:HydraulicFluidGrade:1.0": "0173-1#02-AAR454#003",
    },
    "cylinderStroke": {
        "0173-1#02-AAE604#007": CYL_STROKE_IRDI,
    },
    "accNominalVolume": {
        "urn:sdf:cd:hydraulic:AccVolume:1.0": TANK_VOLUME_IRDI,
    },
    "accBARequirements": {
        "urn:sdf:cd:hydraulic:AccVolume:1.0": TANK_VOLUME_IRDI,
    },
}


@dataclass
class OllamaResponse:
    text: str
    mode: str
    model: str

class OllamaClient:
    def __init__(
        self,
        *,
        model: str = "llama3.1",
        mode: str = "auto",
        endpoint: str = "http://localhost:11434/api/generate",
        timeout: int = 120,
    ) -> None:
        self.model = model
        self.mode = mode
        self.endpoint = endpoint
        self.timeout = timeout

    def _call_http(self, prompt: str) -> OllamaResponse:
        payload = json.dumps(
            {"model": self.model, "prompt": prompt, "stream": False}
        ).encode("utf-8")
        req = urllib.request.Request(
            self.endpoint,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        data = json.loads(body)
        return OllamaResponse(text=data.get("response", ""), mode="http", model=self.model)

    def _call_cli(self, prompt: str) -> OllamaResponse:
        proc = subprocess.run(
            ["ollama", "run", self.model],
            input=prompt,
            text=True,
            capture_output=True,
            timeout=self.timeout,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"ollama CLI failed: {proc.stderr.strip()}")
        return OllamaResponse(text=proc.stdout, mode="cli", model=self.model)

    def generate(self, prompt: str) -> OllamaResponse:
        if self.mode == "http":
            return self._call_http(prompt)
        if self.mode == "cli":
            return self._call_cli(prompt)
        if self.mode == "auto":
            try:
                return self._call_http(prompt)
            except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
                return self._call_cli(prompt)
        raise ValueError(f"Unknown ollama mode: {self.mode}")

    @staticmethod
    def _extract_json_text(text: str) -> Optional[str]:
        stripped = text.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            return stripped
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start != -1 and end != -1 and end > start:
            return stripped[start : end + 1]
        return None

    def extract_json(self, prompt: str, *, max_retries: int = 1) -> Tuple[Any, OllamaResponse]:
        response = self.generate(prompt)
        parsed = self._parse_json_response(response.text)
        if parsed is not None:
            return parsed, response
        if max_retries <= 0:
            raise ValueError("Failed to parse JSON from Ollama response.")
        repair_prompt = self._build_repair_prompt(response.text)
        retry_response = self.generate(repair_prompt)
        parsed = self._parse_json_response(retry_response.text)
        if parsed is None:
            raise ValueError("Failed to parse JSON after repair attempt.")
        return parsed, retry_response

    def _parse_json_response(self, text: str) -> Optional[Any]:
        candidate = self._extract_json_text(text)
        if candidate is None:
            return None
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _build_repair_prompt(raw_text: str) -> str:
        return (
            "Return strict JSON only. Do not include explanations or code fences.\n"
            "Repair the previous response to valid JSON that matches the requested schema.\n"
            "Previous response:\n"
            f"{raw_text}\n"
        )

def get_ollama_version() -> Optional[str]:
    try:
        proc = subprocess.run(
            ["ollama", "--version"], capture_output=True, text=True, check=False
        )
        if proc.returncode == 0:
            return proc.stdout.strip() or proc.stderr.strip() or None
    except FileNotFoundError:
        return None
    return None

def split_camel(label: str) -> List[str]:
    parts = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+", label)
    return [p for p in parts if p]

def normalize_synonyms(label: str) -> List[str]:
    words = split_camel(label)
    if not words:
        return [label.lower()]
    spaced = " ".join(word.lower() for word in words)
    return list({label.lower(), spaced})


def normalize_enum_text(text: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", (text or "").strip().lower())
    return re.sub(r"\s+", " ", normalized).strip()


def deterministic_stage1_entities(node: Dict[str, Any], answer: str) -> Optional[Dict[str, Any]]:
    entities = node.get("expects", {}).get("entities", [])
    if not isinstance(entities, list) or len(entities) != 1:
        return None
    entity = entities[0]
    entity_name = entity.get("name")
    values = entity.get("values")
    if not entity_name or not isinstance(values, list):
        return None

    answer_norm = normalize_enum_text(answer)
    alias_map = {
        "valve_controlled": ["valve controlled", "directional valve", "valve"],
        "pump_controlled": ["pump controlled", "pump", "prime mover"],
        "load_independent": ["load independent", "constant supply pressure", "constant pressure"],
        "load_dependent_ls": ["load dependent", "ls", "load sensing", "load sensing system"],
        "open_circuit": ["open circuit", "with tank", "open"],
        "closed_circuit": ["closed circuit", "main loop", "closed"],
        "rotational_speed": ["rotational speed", "speed", "rpm"],
        "swivel_angle": ["swivel angle", "displacement", "angle"],
        "both": ["both", "both control", "speed and angle"],
        "differential": ["differential", "single rod", "single rod cylinder"],
        "double_rod": ["double rod", "double-rod"],
        "plunger": ["plunger", "single acting", "single-acting"],
        "telescopic": ["telescopic", "multi stage"],
    }
    for value in values:
        candidates = [value, value.replace("_", " ")] + alias_map.get(value, [])
        if any(candidate and candidate in answer_norm for candidate in map(normalize_enum_text, candidates)):
            return {entity_name: value}
    return None


def deterministic_stage1_response(node: Dict[str, Any], answer: str) -> Optional[Tuple[Dict[str, Any], OllamaResponse]]:
    entities = deterministic_stage1_entities(node, answer)
    if not entities:
        return None
    parsed = {
        "intent": "deterministic_stage1",
        "entities": entities,
        "confidence": 1.0,
        "evidence": answer,
    }
    response = OllamaResponse(text=json.dumps(parsed), mode="deterministic", model="deterministic")
    return parsed, response


def _find_lexicon_entry(lexicon_entries: List[Dict[str, Any]], semantic_id: str) -> Optional[Dict[str, Any]]:
    for entry in lexicon_entries:
        if entry.get("semanticId") == semantic_id:
            return entry
    return None


def apply_authoritative_semantic_overrides(
    lexicon_entries: List[Dict[str, Any]],
    semantic_to_label: Dict[str, str],
    concept_index: Dict[str, List[str]],
    semantic_to_components: Dict[str, List[str]],
) -> None:
    for concept, semantic_ids in AUTHORITATIVE_CONCEPT_SEMANTIC_IDS.items():
        allowed: List[str] = []
        for semantic_id in semantic_ids:
            entry = _find_lexicon_entry(lexicon_entries, semantic_id)
            if entry is None:
                label = AUTHORITATIVE_LABELS.get(semantic_id, semantic_id)
                entry = {
                    "semanticId": semantic_id,
                    "labels": [label],
                    "synonyms": normalize_synonyms(label),
                    "units": [],
                    "componentTypes": semantic_to_components.get(semantic_id, []),
                }
                lexicon_entries.append(entry)
            semantic_to_label.setdefault(semantic_id, AUTHORITATIVE_LABELS.get(semantic_id, semantic_id))
            semantic_to_components.setdefault(semantic_id, entry.get("componentTypes", []))
            allowed.append(semantic_id)
        if allowed:
            concept_index[concept] = allowed

    register_known_semantic_ids([entry.get("semanticId", "") for entry in lexicon_entries])
    register_known_semantic_ids(
        [semantic_id for ids in AUTHORITATIVE_CONCEPT_SEMANTIC_IDS.values() for semantic_id in ids]
    )


def resolve_semantic_id_for_concept(
    semantic_id: str,
    concept: Optional[str],
    allowed_semantic_ids: List[str],
) -> Optional[str]:
    raw = str(semantic_id or "").strip()
    if not raw:
        return None
    if not allowed_semantic_ids:
        return raw
    if raw in allowed_semantic_ids:
        return raw
    alias_map = CONCEPT_SEMANTIC_ALIASES.get(concept or "", {})
    remapped = alias_map.get(raw)
    if remapped and remapped in allowed_semantic_ids:
        return remapped
    preferred_ids = AUTHORITATIVE_CONCEPT_SEMANTIC_IDS.get(concept or "", [])
    for preferred in preferred_ids:
        if preferred in allowed_semantic_ids:
            return preferred
    if len(allowed_semantic_ids) == 1:
        return allowed_semantic_ids[0]
    return None


def _make_numeric_constraint(
    concept: str,
    semantic_id: str,
    answer: str,
    *,
    operator: str = "eq",
) -> Optional[Constraint]:
    value = parse_float(answer)
    if value is None:
        return None
    unit = detect_unit_from_text(answer)
    return Constraint(
        semantic_id=semantic_id,
        property_label=AUTHORITATIVE_LABELS.get(semantic_id),
        operator=operator,
        value=value,
        unit=unit,
        confidence=1.0,
        evidence=answer,
        concept=concept,
    )


def _make_text_constraint(concept: str, semantic_id: str, answer: str) -> Constraint:
    return Constraint(
        semantic_id=semantic_id,
        property_label=AUTHORITATIVE_LABELS.get(semantic_id),
        operator="eq",
        value_text=answer.strip(),
        confidence=1.0,
        evidence=answer,
        concept=concept,
    )


def build_deterministic_constraints(
    concept: Optional[str],
    answer: str,
    allowed_semantic_ids: List[str],
) -> Optional[Tuple[List[Constraint], List[Constraint]]]:
    if not concept:
        return None

    def choose(default: str = "") -> str:
        if allowed_semantic_ids:
            return allowed_semantic_ids[0]
        return default

    if concept in {
        "maxOperatingPressure",
        "ratedFlowRate",
        "cylinderLoad",
        "cylinderStroke",
        "tankNominalVolume",
        "tankLevelMax",
        "tankLevelMin",
        "prvSetpoint",
        "accNominalVolume",
        "accPreChargePressure",
    }:
        semantic_id = choose()
        if not semantic_id:
            return None
        constraint = _make_numeric_constraint(concept, semantic_id, answer)
        if constraint is None:
            return None
        return [constraint], []

    if concept == "hydraulicFluid":
        semantic_id = choose("0173-1#02-AAR454#003")
        return [], [_make_text_constraint(concept, semantic_id, answer)]

    if concept == "accBARequirements":
        volume_sid = TANK_VOLUME_IRDI
        pressure_sid = ACC_PRECHARGE_SEM
        parts = [segment.strip() for segment in re.split(r",|;| and ", answer) if segment.strip()]
        numeric_constraints: List[Constraint] = []
        for part in parts:
            value = parse_float(part)
            if value is None:
                continue
            unit = detect_unit_from_text(part)
            unit_key = (unit or "").lower()
            if unit_key in {"l", "liter", "litre", "m3"}:
                numeric_constraints.append(
                    Constraint(
                        semantic_id=volume_sid,
                        property_label=AUTHORITATIVE_LABELS.get(volume_sid),
                        operator="eq",
                        value=value,
                        unit=unit,
                        confidence=1.0,
                        evidence=answer,
                        concept="accNominalVolume",
                    )
                )
            elif unit_key in {"bar", "mpa", "kpa", "pa"}:
                numeric_constraints.append(
                    Constraint(
                        semantic_id=pressure_sid,
                        property_label=AUTHORITATIVE_LABELS.get(pressure_sid),
                        operator="eq",
                        value=value,
                        unit=unit,
                        confidence=1.0,
                        evidence=answer,
                        concept="accPreChargePressure",
                    )
                )
        if numeric_constraints:
            return numeric_constraints, []
    return None


def infer_question_concept(question: Dict[str, Any]) -> Optional[str]:
    concept = question.get("alignment", {}).get("concept")
    if concept:
        return concept
    entities = question.get("expects", {}).get("entities", [])
    if isinstance(entities, list) and entities:
        name = entities[0].get("name")
        if isinstance(name, str) and name:
            return name
    return None


def build_stage2_rule_text(concept: Optional[str], allowed_semantic_ids: List[str]) -> str:
    base = []
    if allowed_semantic_ids:
        base.append(f"Allowed semantic IDs: {', '.join(allowed_semantic_ids)}.")
    concept_rules = {
        "maxOperatingPressure": "Extract exactly one pressure value. Accept pressure units only.",
        "ratedFlowRate": "Extract exactly one flow value. Accept L/min, lpm, or m3/h only.",
        "hydraulicFluid": "Do not create numeric constraints. Return exactly one nonNumericConstraints item with the fluid text.",
        "cylinderLoad": "Extract exactly one force or mass value. Accept N, kN, kg, or t only.",
        "cylinderStroke": "Extract exactly one stroke length. Accept mm, cm, or m only.",
        "tankNominalVolume": "Extract exactly one volume. Accept L or m3 only.",
        "tankLevelMax": "Extract exactly one level/length value. Accept mm, cm, or m only.",
        "tankLevelMin": "Extract exactly one level/length value. Accept mm, cm, or m only.",
        "prvSetpoint": "Extract exactly one pressure value. Accept pressure units only.",
        "accNominalVolume": "Extract exactly one volume value. Accept L or m3 only.",
        "accPreChargePressure": "Extract exactly one pressure value. Accept pressure units only.",
        "accBARequirements": "Extract exactly two numeric constraints when both are present: one volume and one pressure.",
    }
    if concept in concept_rules:
        base.append(concept_rules[concept])
    return " ".join(base)


def build_stage2_prompt(
    stage2_template: str,
    prompt: str,
    concept: Optional[str],
    expects: str,
    lexicon_json: str,
    answer: str,
    allowed_semantic_ids: List[str],
    previous_issue: str = "",
) -> str:
    return render_prompt(
        stage2_template,
        QUESTION=prompt,
        ALIGNMENT_CONCEPT=concept or "",
        EXPECTS_JSON=expects,
        PROPERTY_LEXICON_JSON=lexicon_json,
        ALLOWED_SEMANTIC_IDS_JSON=json.dumps(allowed_semantic_ids, indent=2),
        STRICT_EXTRA_RULES=build_stage2_rule_text(concept, allowed_semantic_ids),
        PREVIOUS_ISSUE=previous_issue,
        USER_ANSWER=answer,
    )


def stage2_result_needs_retry(
    concept: Optional[str],
    answer: str,
    extracted_constraints: List[Constraint],
    extracted_non_numeric: List[Constraint],
) -> Optional[str]:
    if concept == "hydraulicFluid":
        if not extracted_non_numeric:
            return "The answer is a fluid grade/string. Return one nonNumericConstraints item and no numeric constraints."
        return None
    if concept == "accBARequirements":
        if len(extracted_constraints) < 2:
            return "The answer contains accumulator volume and pre-charge pressure. Return both constraints."
        return None

    answer_num = parse_float(answer)
    if answer_num is None:
        return None
    if not extracted_constraints:
        return "No numeric constraint was extracted. Use the numeric value from USER_ANSWER."
    first = extracted_constraints[0]
    if first.value is None:
        return "The constraint has no numeric value. Use the numeric value from USER_ANSWER."
    if abs(first.value - answer_num) > 1e-9:
        return "The numeric value must match the number stated in USER_ANSWER exactly before any downstream unit normalization."
    return None

def build_property_lexicon(
    summary_path: Path, data_root: Path
) -> Tuple[List[Dict[str, Any]], Dict[str, str], Dict[str, List[str]], Dict[str, List[str]]]:
    rows: List[Dict[str, Any]] = []
    if summary_path.exists():
        rows.extend(load_json(summary_path))
    if data_root.exists():
        for aasx_path in data_root.rglob("*.aasx"):
            rows.extend(extract_properties_from_aasx(aasx_path))

    by_semantic: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        semantic_id = row.get("TechnicalPropertySemanticId") or row.get("semanticId")
        if not semantic_id:
            continue
        entry = by_semantic.setdefault(
            semantic_id,
            {
                "semanticId": semantic_id,
                "labels": set(),
                "synonyms": set(),
                "units": set(),
                "componentTypes": set(),
            },
        )
        label = row.get("TechnicalPropertyIdShort") or row.get("label")
        if label:
            entry["labels"].add(label)
            entry["synonyms"].update(normalize_synonyms(label))
        unit = row.get("Unit") or row.get("unit")
        if unit:
            entry["units"].add(unit)
        component = row.get("ComponentName") or row.get("componentType")
        if component:
            entry["componentTypes"].add(component)

    lexicon_entries: List[Dict[str, Any]] = []
    semantic_to_label: Dict[str, str] = {}
    semantic_to_components: Dict[str, List[str]] = {}
    for semantic_id, entry in sorted(by_semantic.items()):
        labels = sorted(entry["labels"])
        synonyms = sorted(entry["synonyms"])
        units = sorted(entry["units"])
        components = sorted(entry["componentTypes"])
        lexicon_entries.append(
            {
                "semanticId": semantic_id,
                "labels": labels,
                "synonyms": synonyms,
                "units": units,
                "componentTypes": components,
            }
        )
        if labels:
            semantic_to_label[semantic_id] = labels[0]
        semantic_to_components[semantic_id] = components

    concept_index: Dict[str, List[str]] = {}
    for concept, keywords in CONCEPT_KEYWORDS.items():
        matches: List[str] = []
        for entry in lexicon_entries:
            text = " ".join(entry["labels"] + entry["synonyms"]).lower()
            if any(keyword in text for keyword in keywords):
                matches.append(entry["semanticId"])
        if matches:
            concept_index[concept] = matches

    apply_authoritative_semantic_overrides(
        lexicon_entries,
        semantic_to_label,
        concept_index,
        semantic_to_components,
    )

    return lexicon_entries, semantic_to_label, concept_index, semantic_to_components


def build_stage2_blocks(qa_tree: Dict[str, Any], skeleton: Dict[str, Any]) -> List[Dict[str, Any]]:
    stage2 = qa_tree.get("stage2", {})
    actuator_type = infer_actuator_type(skeleton)
    component_types = {slot.get("componentType") for slot in skeleton.get("componentSlots", [])}
    has_tank = "Tank" in component_types
    has_prv = "PressureReliefValve" in component_types
    has_acc = "BladderAccumulator" in component_types
    has_check_valve = "CheckValve" in component_types
    has_pump = bool(component_types.intersection({"ConstantPump", "VariablePump"}))
    has_dcv = bool(component_types.intersection({"4-3DirectionalControlValve", "3-2DirectionalControlValve"}))

    blocks: List[Dict[str, Any]] = []
    blocks.extend(stage2.get("globalBlocks", []))
    blocks.extend(stage2.get("actuatorBlocks", {}).get(actuator_type, []))
    for block in stage2.get("conditionalBlocks", []):
        when = block.get("when", {})
        if when.get("requiresTank") is True and not has_tank:
            continue
        if when.get("requiresPrv") is True and not has_prv:
            continue
        if when.get("requiresAccumulator") is True and not has_acc:
            continue
        if when.get("requiresCheckValve") is True and not has_check_valve:
            continue
        if when.get("requiresPump") is True and not has_pump:
            continue
        if when.get("requiresDcv") is True and not has_dcv:
            continue
        blocks.append(block)
    return blocks


def block_properties_by_concept(block: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        str(prop.get("concept", "")): prop
        for prop in block.get("properties", [])
        if str(prop.get("concept", ""))
    }


def property_keywords(prop: Dict[str, Any]) -> List[str]:
    keywords = set()
    label = str(prop.get("label", "")).strip()
    concept = str(prop.get("concept", "")).strip()
    for keyword in prop.get("keywords", []) or []:
        if keyword:
            keywords.add(str(keyword).strip())
    if label:
        keywords.add(label)
        keywords.update(normalize_synonyms(label))
    if concept:
        keywords.add(concept)
        keywords.add(" ".join(split_camel(concept)))
    return sorted({item for item in keywords if item}, key=len, reverse=True)


def build_block_lexicon_subset(
    block: Dict[str, Any],
    lexicon_entries: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    semantic_ids = {
        str(prop.get("semanticId", "")).strip()
        for prop in block.get("properties", [])
        if str(prop.get("semanticId", "")).strip()
    }
    subset = [entry for entry in lexicon_entries if entry.get("semanticId") in semantic_ids]
    existing_ids = {entry.get("semanticId") for entry in subset}
    for prop in block.get("properties", []):
        semantic_id = str(prop.get("semanticId", "")).strip()
        if not semantic_id or semantic_id in existing_ids:
            continue
        label = str(prop.get("label", "")).strip() or semantic_id
        subset.append(
            {
                "semanticId": semantic_id,
                "labels": [label],
                "synonyms": normalize_synonyms(label),
                "units": [],
                "componentTypes": list(prop.get("targetComponentTypes", []) or []),
            }
        )
    return subset


def build_stage2_block_rule_text(block: Dict[str, Any]) -> str:
    concepts = [str(prop.get("concept", "")).strip() for prop in block.get("properties", []) if prop.get("concept")]
    return (
        "Extract only properties explicitly present in USER_ANSWER. "
        "Do not invent omitted values. "
        "Return each extracted item with its matching concept from BLOCK_PROPERTIES. "
        f"Allowed concepts: {', '.join(concepts)}."
    )


def build_stage2_block_prompt(
    stage2_template: str,
    block: Dict[str, Any],
    lexicon_json: str,
    answer: str,
    previous_issue: str = "",
) -> str:
    return render_prompt(
        stage2_template,
        QUESTION=str(block.get("prompt", "")),
        BLOCK_ID=str(block.get("id", "")),
        EXPECTS_JSON=json.dumps(block.get("expects", {}), indent=2),
        BLOCK_PROPERTIES_JSON=json.dumps(block.get("properties", []), indent=2),
        PROPERTY_LEXICON_JSON=lexicon_json,
        STRICT_EXTRA_RULES=build_stage2_block_rule_text(block),
        PREVIOUS_ISSUE=previous_issue,
        USER_ANSWER=answer,
    )


def is_skipped_stage2_answer(answer: str) -> bool:
    return normalize_enum_text(answer) in {
        "",
        "skip",
        "none",
        "na",
        "n a",
        "not applicable",
        "not specified",
        "no additional constraints",
    }


def explode_answer_segments(answer: str) -> List[str]:
    segments: List[str] = []
    for chunk in re.split(r"[;\n]+", answer):
        text = chunk.strip()
        if not text:
            continue
        parts = [part.strip() for part in re.split(r",(?=\s*(?:[A-Za-z]|\d))", text) if part.strip()]
        if len(parts) == 1:
            segments.append(text)
        else:
            segments.extend(parts)
    return segments


def property_type_matches_segment(prop_type: str, segment: str) -> bool:
    unit = (detect_unit_from_text(segment) or "").lower()
    if prop_type == "quantity_pressure":
        return unit in {"bar", "mpa", "kpa", "pa"}
    if prop_type == "quantity_flow":
        return unit in {"l/min", "m3/h", "lpm"}
    if prop_type == "quantity_volume":
        return unit in {"l", "m3"}
    if prop_type == "quantity_force_or_mass":
        return unit in {"n", "kn", "kg", "t"}
    if prop_type == "quantity_length":
        return unit in {"mm", "cm", "m"}
    return False


def infer_numeric_operator(segment: str, default_operator: str) -> str:
    lowered = segment.lower()
    if " between " in lowered or re.search(r"\b\d+(?:\.\d+)?\s*(?:to|-)\s*\d+(?:\.\d+)?\b", lowered):
        return "range"
    if any(token in lowered for token in [">=", "at least", "not less than", "minimum of", "minimum "]):
        return "ge"
    if any(token in lowered for token in ["<=", "at most", "not more than", "up to"]):
        return "le"
    return default_operator or "eq"


def heuristic_stage2_parse(block: Dict[str, Any], answer: str) -> Dict[str, Any]:
    if is_skipped_stage2_answer(answer):
        return {
            "intent": "provide_sizing_constraints",
            "entities": {"constraints": [], "nonNumericConstraints": []},
            "confidence": 1.0,
            "evidence": answer,
        }
    properties = list(block.get("properties", []) or [])
    remaining = {str(prop.get("concept", "")): prop for prop in properties if prop.get("concept")}
    segments = explode_answer_segments(answer)
    constraints: List[Dict[str, Any]] = []
    non_numeric: List[Dict[str, Any]] = []
    used_segments: set[int] = set()

    def add_from_segment(prop: Dict[str, Any], segment: str) -> bool:
        concept = str(prop.get("concept", "")).strip()
        if not concept:
            return False
        if prop.get("kind") == "text":
            value_text = segment.strip()
            for keyword in property_keywords(prop):
                value_text = re.sub(re.escape(keyword), "", value_text, flags=re.IGNORECASE)
            value_text = value_text.strip(" :-")
            value_text = value_text or segment.strip()
            non_numeric.append(
                {
                    "concept": concept,
                    "semanticId": str(prop.get("semanticId", "")).strip(),
                    "propertyLabel": prop.get("label"),
                    "valueText": value_text,
                    "confidence": 0.85,
                    "evidence": segment,
                }
            )
            remaining.pop(concept, None)
            return True

        operator = infer_numeric_operator(segment, str(prop.get("defaultOperator", "eq")))
        value_max = None
        range_match = re.search(
            r"(-?\d+(?:\.\d+)?)\s*(?:to|-)\s*(-?\d+(?:\.\d+)?)",
            segment,
            flags=re.IGNORECASE,
        )
        if operator == "range" and range_match:
            value = parse_float(range_match.group(1))
            value_max = parse_float(range_match.group(2))
        else:
            value = parse_float(segment)
        if value is None:
            return False
        item: Dict[str, Any] = {
            "concept": concept,
            "semanticId": str(prop.get("semanticId", "")).strip(),
            "propertyLabel": prop.get("label"),
            "operator": operator,
            "value": value,
            "unit": detect_unit_from_text(segment),
            "confidence": 0.8,
            "evidence": segment,
        }
        if value_max is not None:
            item["valueMax"] = value_max
        constraints.append(item)
        remaining.pop(concept, None)
        return True

    for index, segment in enumerate(segments):
        segment_norm = normalize_enum_text(segment)
        matches = [
            prop
            for prop in list(remaining.values())
            if any(normalize_enum_text(keyword) in segment_norm for keyword in property_keywords(prop))
        ]
        if len(matches) == 1 and add_from_segment(matches[0], segment):
            used_segments.add(index)

    for index, segment in enumerate(segments):
        if index in used_segments:
            continue
        numeric_matches = [
            prop
            for prop in list(remaining.values())
            if prop.get("kind") == "numeric" and property_type_matches_segment(str(prop.get("type", "")), segment)
        ]
        if len(numeric_matches) == 1 and add_from_segment(numeric_matches[0], segment):
            used_segments.add(index)
            continue
        text_matches = [prop for prop in list(remaining.values()) if prop.get("kind") == "text"]
        if len(text_matches) == 1 and parse_float(segment) is None and add_from_segment(text_matches[0], segment):
            used_segments.add(index)

    return {
        "intent": "provide_sizing_constraints",
        "entities": {
            "constraints": constraints,
            "nonNumericConstraints": non_numeric,
        },
        "confidence": 0.8 if constraints or non_numeric else 0.0,
        "evidence": answer,
    }


def parse_block_constraints(
    parsed: Dict[str, Any],
    block: Dict[str, Any],
) -> Tuple[List[Constraint], List[Constraint]]:
    property_by_concept = block_properties_by_concept(block)
    property_by_semantic = {
        str(prop.get("semanticId", "")).strip(): prop
        for prop in block.get("properties", [])
        if str(prop.get("semanticId", "")).strip()
    }
    constraints: List[Constraint] = []
    non_numeric: List[Constraint] = []
    seen_numeric: set[str] = set()
    seen_text: set[str] = set()

    entities = parsed.get("entities", {}) if isinstance(parsed, dict) else {}
    for item in entities.get("constraints", []) or []:
        if not isinstance(item, dict):
            continue
        concept = str(item.get("concept", "")).strip()
        prop = property_by_concept.get(concept)
        if prop is None:
            semantic_id = str(item.get("semanticId", "")).strip()
            prop = property_by_semantic.get(semantic_id)
            if prop is not None:
                concept = str(prop.get("concept", "")).strip()
        if prop is None or prop.get("kind") != "numeric" or concept in seen_numeric:
            continue
        semantic_id = str(prop.get("semanticId", "")).strip()
        if not semantic_id:
            continue
        validate_semantic_id(semantic_id, f"constraint_alignment:{concept or 'unknown'}")
        operator = str(item.get("operator", "")).strip()
        if operator not in {"eq", "ge", "le", "range"}:
            operator = str(prop.get("defaultOperator", "eq")).strip() or "eq"
            defaulted = True
        else:
            defaulted = False
        value = item.get("value")
        value_num = value if isinstance(value, (int, float)) else parse_float(str(value))
        if value_num is None:
            continue
        value_max = item.get("valueMax")
        value_max_num = None
        if value_max is not None:
            value_max_num = value_max if isinstance(value_max, (int, float)) else parse_float(str(value_max))
        constraints.append(
            Constraint(
                semantic_id=semantic_id,
                property_label=str(prop.get("label", "")).strip() or item.get("propertyLabel"),
                operator=operator,
                value=value_num,
                value_max=value_max_num,
                unit=item.get("unit"),
                confidence=float(item.get("confidence", 0.0)),
                evidence=item.get("evidence"),
                concept=concept,
                defaulted_operator=defaulted,
                source_block_id=str(block.get("id", "")).strip() or None,
                target_component_types=list(prop.get("targetComponentTypes", []) or []),
            )
        )
        seen_numeric.add(concept)

    for item in entities.get("nonNumericConstraints", []) or []:
        if not isinstance(item, dict):
            continue
        concept = str(item.get("concept", "")).strip()
        prop = property_by_concept.get(concept)
        if prop is None:
            semantic_id = str(item.get("semanticId", "")).strip()
            prop = property_by_semantic.get(semantic_id)
            if prop is not None:
                concept = str(prop.get("concept", "")).strip()
        if prop is None or prop.get("kind") != "text" or concept in seen_text:
            continue
        semantic_id = str(prop.get("semanticId", "")).strip()
        if not semantic_id:
            continue
        validate_semantic_id(semantic_id, f"constraint_alignment:{concept or 'unknown'}")
        value_text = str(item.get("valueText", "")).strip()
        if not value_text:
            continue
        non_numeric.append(
            Constraint(
                semantic_id=semantic_id,
                property_label=str(prop.get("label", "")).strip() or item.get("propertyLabel"),
                operator="eq",
                value_text=value_text,
                confidence=float(item.get("confidence", 0.0)),
                evidence=item.get("evidence"),
                concept=concept,
                source_block_id=str(block.get("id", "")).strip() or None,
                target_component_types=list(prop.get("targetComponentTypes", []) or []),
            )
        )
        seen_text.add(concept)
    return constraints, non_numeric


def stage2_block_needs_retry(
    answer: str,
    extracted_constraints: List[Constraint],
    extracted_non_numeric: List[Constraint],
) -> Optional[str]:
    if is_skipped_stage2_answer(answer):
        return None
    if extracted_constraints or extracted_non_numeric:
        return None
    return "No properties were extracted. Return only the properties explicitly stated in USER_ANSWER."

def select_skeleton(
    qa_tree: Dict,
    skeletons: Dict[str, Dict[str, Any]],
    stage1_template: str,
    client: OllamaClient,
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    confidence_policy = qa_tree.get("nlpConventions", {}).get("confidencePolicy", {})
    low_conf = confidence_policy.get("askClarificationBelow", 0.55)

    node_map = {node["id"]: node for node in qa_tree["stage1"]["nodes"]}
    node_order = [node["id"] for node in qa_tree["stage1"]["nodes"]]
    current_id = node_order[0]

    user_inputs: List[Dict[str, Any]] = []
    nlp_outputs: List[Dict[str, Any]] = []
    routing_trace: List[Dict[str, Any]] = []
    routing_state: Dict[str, Any] = {}

    while True:
        node = node_map[current_id]
        prompt = node["prompt"]
        answer = input(f"{prompt}\n> ").strip()
        user_inputs.append({"stage": "stage1", "nodeId": node["id"], "prompt": prompt, "answer": answer})
        expects = json.dumps(node.get("expects", {}), indent=2)
        hints = json.dumps(node.get("nlp", {}).get("entityHints", {}), indent=2)
        deterministic = deterministic_stage1_response(node, answer)
        if deterministic is not None:
            parsed, response = deterministic
        else:
            nlp_prompt = render_prompt(
                stage1_template,
                QUESTION=prompt,
                EXPECTS_JSON=expects,
                ENTITY_HINTS_JSON=hints,
                USER_ANSWER=answer,
            )
            try:
                parsed, response = client.extract_json(nlp_prompt)
            except Exception:
                fallback = deterministic_stage1_response(node, answer)
                if fallback is None:
                    raise
                parsed, response = fallback
        confidence = float(parsed.get("confidence", 0.0))
        nlp_outputs.append(
            {
                "stage": "stage1",
                "nodeId": node["id"],
                "output": parsed,
                "ollamaMode": response.mode,
                "model": response.model,
            }
        )

        if confidence < low_conf:
            clarification = node.get("onLowConfidence", {}).get("clarificationPrompt")
            if clarification:
                answer = input(f"{clarification}\n> ").strip()
                user_inputs.append(
                    {"stage": "stage1", "nodeId": node["id"], "prompt": clarification, "answer": answer}
                )
                deterministic = deterministic_stage1_response(node, answer)
                if deterministic is not None:
                    parsed, response = deterministic
                else:
                    nlp_prompt = render_prompt(
                        stage1_template,
                        QUESTION=clarification,
                        EXPECTS_JSON=expects,
                        ENTITY_HINTS_JSON=hints,
                        USER_ANSWER=answer,
                    )
                    try:
                        parsed, response = client.extract_json(nlp_prompt)
                    except Exception:
                        fallback = deterministic_stage1_response(node, answer)
                        if fallback is None:
                            raise
                        parsed, response = fallback
                nlp_outputs.append(
                    {
                        "stage": "stage1",
                        "nodeId": node["id"],
                        "output": parsed,
                        "ollamaMode": response.mode,
                        "model": response.model,
                    }
                )

        entities = parsed.get("entities", {})
        routing_state.update(entities)
        routing_trace.append(
            {"nodeId": node["id"], "entities": entities, "state": dict(routing_state)}
        )

        selected = route_stage1(node, routing_state)
        if selected.get("select"):
            skeleton_id = selected["select"]
            if skeleton_id not in skeletons:
                raise RuntimeError(f"Selected unknown skeletonId: {skeleton_id}")
            return skeleton_id, user_inputs, nlp_outputs, routing_trace
        next_node = selected.get("next")
        if not next_node:
            raise RuntimeError(f"Stage1 routing failed at node {node['id']}.")
        current_id = next_node

def route_stage1(node: Dict[str, Any], entities: Dict[str, Any]) -> Dict[str, Optional[str]]:
    for route in node.get("routing", []):
        when = route.get("when", {})
        if all(entities.get(key) == value for key, value in when.items()):
            return {"select": route.get("select"), "next": route.get("next")}
    return {"select": None, "next": None}

def ensure_operator(value: Optional[str]) -> Tuple[str, bool]:
    if value in {"eq", "ge", "le", "range"}:
        return value, False
    return "eq", True

def parse_constraints(
    parsed: Dict[str, Any],
    concept: Optional[str],
    allowed_semantic_ids: Optional[List[str]] = None,
) -> Tuple[List[Constraint], List[Constraint]]:
    constraints: List[Constraint] = []
    non_numeric: List[Constraint] = []
    allowed = [semantic_id for semantic_id in (allowed_semantic_ids or []) if semantic_id]

    entities = parsed.get("entities", {})
    for item in entities.get("constraints", []) or []:
        if not isinstance(item, dict):
            continue
        semantic_id = resolve_semantic_id_for_concept(item.get("semanticId", ""), concept, allowed)
        if not semantic_id:
            continue
        validate_semantic_id(semantic_id, f"constraint_alignment:{concept or 'unknown'}")
        operator, defaulted = ensure_operator(item.get("operator"))
        value = item.get("value")
        value_num = value if isinstance(value, (int, float)) else parse_float(str(value))
        constraints.append(
            Constraint(
                semantic_id=semantic_id,
                property_label=item.get("propertyLabel"),
                operator=operator,
                value=value_num,
                unit=item.get("unit"),
                confidence=float(item.get("confidence", 0.0)),
                evidence=item.get("evidence"),
                concept=concept,
                defaulted_operator=defaulted,
            )
        )

    for item in entities.get("nonNumericConstraints", []) or []:
        if not isinstance(item, dict):
            continue
        semantic_id = resolve_semantic_id_for_concept(item.get("semanticId", ""), concept, allowed)
        if not semantic_id:
            continue
        validate_semantic_id(semantic_id, f"constraint_alignment:{concept or 'unknown'}")
        non_numeric.append(
            Constraint(
                semantic_id=semantic_id,
                property_label=item.get("propertyLabel"),
                operator="eq",
                value_text=item.get("valueText"),
                confidence=float(item.get("confidence", 0.0)),
                evidence=item.get("evidence"),
                concept=concept,
            )
        )
    return constraints, non_numeric

def run_stage2(
    qa_tree: Dict,
    skeleton: Dict[str, Any],
    stage2_template: str,
    client: OllamaClient,
    lexicon_entries: List[Dict[str, Any]],
    concept_index: Dict[str, List[str]],
) -> Tuple[List[Constraint], List[Constraint], Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    user_inputs: List[Dict[str, Any]] = []
    nlp_outputs: List[Dict[str, Any]] = []

    global_constraints: Dict[str, Any] = {}
    constraints: List[Constraint] = []
    non_numeric: List[Constraint] = []
    _ = concept_index
    promoted_concepts = {
        "maxOperatingPressure",
        "ratedFlowRate",
        "hydraulicFluid",
        "tankNominalVolume",
        "prvSetpoint",
        "accNominalVolume",
        "accPreChargePressure",
    }
    for block in build_stage2_blocks(qa_tree, skeleton):
        prompt = str(block.get("prompt", "")).strip()
        answer = input(f"{prompt}\n> ").strip()
        user_inputs.append({"stage": "stage2", "nodeId": block["id"], "prompt": prompt, "answer": answer})

        lexicon_subset = build_block_lexicon_subset(block, lexicon_entries)
        lexicon_json = json.dumps(lexicon_subset, indent=2)
        heuristic_parsed = heuristic_stage2_parse(block, answer)
        heuristic_constraints, heuristic_non_numeric = parse_block_constraints(heuristic_parsed, block)
        if is_skipped_stage2_answer(answer) or heuristic_constraints or heuristic_non_numeric:
            parsed = heuristic_parsed
            response = OllamaResponse(text=json.dumps(parsed), mode="deterministic", model="deterministic")
        else:
            nlp_prompt = build_stage2_block_prompt(stage2_template, block, lexicon_json, answer)
            try:
                parsed, response = client.extract_json(nlp_prompt)
            except Exception:
                parsed = heuristic_parsed
                response = OllamaResponse(text=json.dumps(parsed), mode="deterministic", model="deterministic")
        nlp_outputs.append(
            {
                "stage": "stage2",
                "nodeId": block["id"],
                "output": parsed,
                "ollamaMode": response.mode,
                "model": response.model,
            }
        )

        extracted_constraints, extracted_non_numeric = parse_block_constraints(parsed, block)
        retry_issue = stage2_block_needs_retry(answer, extracted_constraints, extracted_non_numeric)
        if retry_issue and response.mode != "deterministic":
            retry_prompt = build_stage2_block_prompt(stage2_template, block, lexicon_json, answer, retry_issue)
            try:
                parsed, response = client.extract_json(retry_prompt)
                nlp_outputs.append(
                    {
                        "stage": "stage2",
                        "nodeId": block["id"],
                        "output": parsed,
                        "ollamaMode": response.mode,
                        "model": response.model,
                    }
                )
                extracted_constraints, extracted_non_numeric = parse_block_constraints(parsed, block)
            except Exception:
                pass
        if retry_issue and not extracted_constraints and not extracted_non_numeric:
            parsed = heuristic_stage2_parse(block, answer)
            response = OllamaResponse(text=json.dumps(parsed), mode="deterministic", model="deterministic")
            nlp_outputs.append(
                {
                    "stage": "stage2",
                    "nodeId": block["id"],
                    "output": parsed,
                    "ollamaMode": response.mode,
                    "model": response.model,
                }
            )
            extracted_constraints, extracted_non_numeric = parse_block_constraints(parsed, block)

        constraints.extend(extracted_constraints)
        non_numeric.extend(extracted_non_numeric)
        for item in extracted_constraints + extracted_non_numeric:
            if item.concept in promoted_concepts and item.concept not in global_constraints:
                global_constraints[item.concept] = item
    return constraints, non_numeric, global_constraints, user_inputs, nlp_outputs

def infer_actuator_type(skeleton: Dict[str, Any]) -> str:
    for slot in skeleton.get("componentSlots", []):
        if slot.get("componentType") == "HydraulicMotor":
            return "motor"
        if slot.get("componentType") in {
            "Double-ActingCylinder",
            "SynchronousCylinder",
            "PlungerCylinder",
            "TelescopicCylinder",
        }:
            return "cylinder"
    return "cylinder"
