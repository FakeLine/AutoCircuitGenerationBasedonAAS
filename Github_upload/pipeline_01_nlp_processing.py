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
        nlp_prompt = render_prompt(
            stage1_template,
            QUESTION=prompt,
            EXPECTS_JSON=expects,
            ENTITY_HINTS_JSON=hints,
            USER_ANSWER=answer,
        )
        parsed, response = client.extract_json(nlp_prompt)
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
                nlp_prompt = render_prompt(
                    stage1_template,
                    QUESTION=clarification,
                    EXPECTS_JSON=expects,
                    ENTITY_HINTS_JSON=hints,
                    USER_ANSWER=answer,
                )
                parsed, response = client.extract_json(nlp_prompt)
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

    actuator_type = infer_actuator_type(skeleton)
    component_types = {slot.get("componentType") for slot in skeleton.get("componentSlots", [])}
    has_tank = "Tank" in component_types
    has_prv = "PressureReliefValve" in component_types
    has_acc = "BladderAccumulator" in component_types

    questions = []
    questions.extend(qa_tree.get("stage2", {}).get("globalQuestions", []))
    questions.extend(qa_tree.get("stage2", {}).get("actuatorQuestions", {}).get(actuator_type, []))
    for block in qa_tree.get("stage2", {}).get("conditionalQuestions", []):
        when = block.get("when", {})
        requires_tank = when.get("requiresTank")
        requires_prv = when.get("requiresPrv")
        requires_acc = when.get("requiresAccumulator")
        if requires_tank is not None and requires_tank != has_tank:
            continue
        if requires_prv is not None and requires_prv != has_prv:
            continue
        if requires_acc is not None and requires_acc != has_acc:
            continue
        questions.extend(block.get("questions", []))

    def extract_entity_quantity(entity_value: Any) -> Tuple[Optional[float], Optional[str]]:
        if entity_value is None:
            return None, None
        if isinstance(entity_value, (int, float)):
            return float(entity_value), None
        if isinstance(entity_value, dict):
            number = entity_value.get("value")
            if number is None:
                number = entity_value.get("number")
            if number is None:
                number = entity_value.get("amount")
            unit = entity_value.get("unit") or entity_value.get("uom")
            value_num = number if isinstance(number, (int, float)) else parse_float(str(number))
            if value_num is None and isinstance(entity_value.get("text"), str):
                value_num = parse_float(entity_value["text"])
            return value_num, str(unit) if unit else None
        if isinstance(entity_value, str):
            text = entity_value.strip()
            return parse_float(text), None
        return None, None

    for question in questions:
        prompt = question["prompt"]
        answer = input(f"{prompt}\n> ").strip()
        user_inputs.append({"stage": "stage2", "nodeId": question["id"], "prompt": prompt, "answer": answer})

        concept = infer_question_concept(question)
        lexicon_subset = lexicon_entries
        allowed_semantic_ids = concept_index.get(concept, []) if concept else []
        if concept and concept in concept_index:
            lexicon_subset = [
                entry for entry in lexicon_entries if entry["semanticId"] in concept_index[concept]
            ]
        expects = json.dumps(question.get("expects", {}), indent=2)
        lexicon_json = json.dumps(lexicon_subset, indent=2)
        nlp_prompt = build_stage2_prompt(
            stage2_template,
            prompt,
            concept,
            expects,
            lexicon_json,
            answer,
            allowed_semantic_ids,
        )
        parsed, response = client.extract_json(nlp_prompt)
        nlp_outputs.append(
            {
                "stage": "stage2",
                "nodeId": question["id"],
                "output": parsed,
                "ollamaMode": response.mode,
                "model": response.model,
            }
        )

        extracted_constraints, extracted_non_numeric = parse_constraints(
            parsed,
            concept,
            allowed_semantic_ids,
        )
        retry_issue = stage2_result_needs_retry(
            concept,
            answer,
            extracted_constraints,
            extracted_non_numeric,
        )
        if retry_issue:
            retry_prompt = build_stage2_prompt(
                stage2_template,
                prompt,
                concept,
                expects,
                lexicon_json,
                answer,
                allowed_semantic_ids,
                retry_issue,
            )
            parsed, response = client.extract_json(retry_prompt)
            nlp_outputs.append(
                {
                    "stage": "stage2",
                    "nodeId": question["id"],
                    "output": parsed,
                    "ollamaMode": response.mode,
                    "model": response.model,
                }
            )
            extracted_constraints, extracted_non_numeric = parse_constraints(
                parsed,
                concept,
                allowed_semantic_ids,
            )
        constraints.extend(extracted_constraints)
        non_numeric.extend(extracted_non_numeric)

        if concept == "maxOperatingPressure" and extracted_constraints:
            global_constraints["maxOperatingPressure"] = extracted_constraints[0]
        if concept == "ratedFlowRate" and extracted_constraints:
            global_constraints["ratedFlowRate"] = extracted_constraints[0]
        if concept == "hydraulicFluid" and extracted_non_numeric:
            global_constraints["hydraulicFluid"] = extracted_non_numeric[0]
        if concept in {"tankLevelMax", "tankLevelMin"} and extracted_constraints:
            global_constraints[concept] = extracted_constraints[0]
        if concept == "tankNominalVolume" and extracted_constraints:
            global_constraints["tankNominalVolume"] = extracted_constraints[0]
        if concept == "prvSetpoint" and extracted_constraints:
            global_constraints["prvSetpoint"] = extracted_constraints[0]
        if concept == "accNominalVolume" and extracted_constraints:
            global_constraints["accNominalVolume"] = extracted_constraints[0]
        if concept == "accPreChargePressure" and extracted_constraints:
            global_constraints["accPreChargePressure"] = extracted_constraints[0]
        if concept == "accBARequirements":
            volume_constraint = next(
                (item for item in extracted_constraints if item.semantic_id == TANK_VOLUME_IRDI),
                None,
            )
            precharge_constraint = next(
                (item for item in extracted_constraints if item.semantic_id == ACC_PRECHARGE_SEM),
                None,
            )
            if volume_constraint is None:
                volume_constraint = next(
                    (
                        item
                        for item in extracted_constraints
                        if (item.unit or "").strip().lower().replace(" ", "") in {"l", "liter", "litre"}
                    ),
                    None,
                )
            if precharge_constraint is None:
                precharge_constraint = next(
                    (
                        item
                        for item in extracted_constraints
                        if (item.unit or "").strip().lower().replace(" ", "") in {"bar", "mpa", "kpa", "pa"}
                    ),
                    None,
                )
            if volume_constraint is not None:
                global_constraints["accNominalVolume"] = volume_constraint
            if precharge_constraint is not None:
                global_constraints["accPreChargePressure"] = precharge_constraint
            entities = parsed.get("entities", {}) if isinstance(parsed, dict) else {}
            if isinstance(entities, dict):
                if "accNominalVolume" not in global_constraints:
                    volume_value, volume_unit = extract_entity_quantity(entities.get("acc_volume"))
                    if volume_value is not None:
                        global_constraints["accNominalVolume"] = Constraint(
                            semantic_id=TANK_VOLUME_IRDI,
                            property_label="NominalVolume",
                            operator="ge",
                            value=volume_value,
                            unit=volume_unit or "L",
                            confidence=float(parsed.get("confidence", 0.0)),
                            evidence=answer,
                            concept="accNominalVolume",
                        )
                if "accPreChargePressure" not in global_constraints:
                    precharge_value, precharge_unit = extract_entity_quantity(entities.get("acc_precharge"))
                    if precharge_value is not None:
                        global_constraints["accPreChargePressure"] = Constraint(
                            semantic_id=ACC_PRECHARGE_SEM,
                            property_label="PreChargePressure",
                            operator="ge",
                            value=precharge_value,
                            unit=precharge_unit or "bar",
                            confidence=float(parsed.get("confidence", 0.0)),
                            evidence=answer,
                            concept="accPreChargePressure",
                        )

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
