#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Tuple


REQUIRED_SKELETON_COUNT = 6
REQUIRED_GLOBAL_FIELDS = {"maxOperatingPressure", "ratedFlowRate", "hydraulicFluid"}
REQUIRED_TANK_FIELDS = {"tank_level_max", "tank_level_min", "tank_volume"}
REQUIRED_PRV_FIELDS = {"prv_setpoint"}


def load_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def build_skeleton_index(library: Dict) -> Dict[str, Dict[str, str]]:
    index: Dict[str, Dict[str, str]] = {}
    for skeleton in library.get("skeletons", []):
        skeleton_id = skeleton.get("skeletonId")
        title = skeleton.get("title")
        if skeleton_id:
            index[skeleton_id] = {"title": title or ""}
    return index


def validate_stage1_routing(qa_tree: Dict, skeleton_ids: set) -> List[str]:
    errors: List[str] = []
    stage1 = qa_tree.get("stage1", {})
    for node in stage1.get("nodes", []):
        for route in node.get("routing", []):
            selected = route.get("select")
            if selected and selected not in skeleton_ids:
                errors.append(f"Stage1 routing references unknown skeletonId: {selected}")
    return errors


def validate_stage2_questions(qa_tree: Dict) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    stage2 = qa_tree.get("stage2", {})
    global_questions = stage2.get("globalQuestions", [])
    global_entities = set()
    for question in global_questions:
        for entity in question.get("expects", {}).get("entities", []):
            name = entity.get("name")
            if name:
                global_entities.add(name)

    missing_global = REQUIRED_GLOBAL_FIELDS - global_entities
    if missing_global:
        errors.append(f"Missing required global questions for: {', '.join(sorted(missing_global))}")

    actuator_questions = stage2.get("actuatorQuestions", {})
    if "cylinder" not in actuator_questions:
        errors.append("Missing actuatorQuestions.cylinder")
    if "motor" not in actuator_questions:
        errors.append("Missing actuatorQuestions.motor")

    tank_entities = set()
    prv_entities = set()
    for block in stage2.get("conditionalQuestions", []):
        if block.get("when", {}).get("requiresTank") is True:
            for question in block.get("questions", []):
                for entity in question.get("expects", {}).get("entities", []):
                    name = entity.get("name")
                    if name:
                        tank_entities.add(name)
        if block.get("when", {}).get("requiresPrv") is True:
            for question in block.get("questions", []):
                for entity in question.get("expects", {}).get("entities", []):
                    name = entity.get("name")
                    if name:
                        prv_entities.add(name)

    missing_tank = REQUIRED_TANK_FIELDS - tank_entities
    if missing_tank:
        warnings.append(
            "Tank level questions are missing or incomplete: "
            + ", ".join(sorted(missing_tank))
        )

    missing_prv = REQUIRED_PRV_FIELDS - prv_entities
    if missing_prv:
        warnings.append(
            "Pressure relief valve setpoint question is missing or incomplete: "
            + ", ".join(sorted(missing_prv))
        )

    return errors, warnings


def rebuild_bundle(library: Dict, qa_tree: Dict) -> Dict:
    return {
        "schema": "urn:sdf:hydraulic:bundle:1.0",
        "generatedAt": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "library": library,
        "qaTree": qa_tree,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and fix skeleton/QA configuration files.")
    parser.add_argument(
        "--library",
        type=Path,
        default=Path(__file__).resolve().parent / "skeleton_library.json",
        help="Path to skeleton_library.json",
    )
    parser.add_argument(
        "--qa-tree",
        type=Path,
        default=Path(__file__).resolve().parent / "qa_tree.json",
        help="Path to qa_tree.json",
    )
    parser.add_argument(
        "--bundle",
        type=Path,
        default=Path(__file__).resolve().parent / "bundle.json",
        help="Path to bundle.json",
    )
    parser.add_argument("--fix", action="store_true", help="Apply fixes to files.")
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(__file__).resolve().parent / "validation_report.md",
        help="Path to validation_report.md",
    )

    args = parser.parse_args()
    report_lines: List[str] = []
    report_lines.append("# Validation Report")
    report_lines.append(f"- Timestamp: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"- Fix mode: {'enabled' if args.fix else 'disabled'}")

    library = load_json(args.library)
    qa_tree = load_json(args.qa_tree)

    skeletons = library.get("skeletons", [])
    skeleton_ids = {s.get("skeletonId") for s in skeletons if s.get("skeletonId")}

    errors: List[str] = []
    warnings: List[str] = []
    fixes: List[str] = []

    if len(skeletons) != REQUIRED_SKELETON_COUNT:
        errors.append(
            f"Skeleton count mismatch: expected {REQUIRED_SKELETON_COUNT}, got {len(skeletons)}"
        )

    if len(skeleton_ids) != len(skeletons):
        errors.append("Skeleton IDs are missing or duplicated.")

    # Validate stage1 routing
    errors.extend(validate_stage1_routing(qa_tree, skeleton_ids))

    # Validate stage2 questions
    stage2_errors, stage2_warnings = validate_stage2_questions(qa_tree)
    errors.extend(stage2_errors)
    warnings.extend(stage2_warnings)

    # Fix skeletonIndex if needed
    expected_index = build_skeleton_index(library)
    if qa_tree.get("skeletonIndex") != expected_index:
        if args.fix:
            qa_tree["skeletonIndex"] = expected_index
            qa_tree["generatedAt"] = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            fixes.append("Updated qa_tree.skeletonIndex to match skeleton library.")
        else:
            warnings.append("qa_tree.skeletonIndex does not match skeleton library.")

    # Fix bundle if requested or missing
    if args.bundle.exists():
        bundle = load_json(args.bundle)
        expected_bundle = rebuild_bundle(library, qa_tree)
        if bundle.get("library") != expected_bundle["library"] or bundle.get("qaTree") != expected_bundle["qaTree"]:
            if args.fix:
                write_json(args.bundle, expected_bundle)
                fixes.append("Rebuilt bundle.json from current library and QA tree.")
            else:
                warnings.append("bundle.json does not match current library/QA tree.")
    else:
        if args.fix:
            write_json(args.bundle, rebuild_bundle(library, qa_tree))
            fixes.append("Created bundle.json from current library and QA tree.")
        else:
            warnings.append("bundle.json missing.")

    if args.fix and fixes:
        write_json(args.qa_tree, qa_tree)
        write_json(args.library, library)

    report_lines.append("## Checks")
    report_lines.append(f"- Skeleton count: {len(skeletons)} (expected {REQUIRED_SKELETON_COUNT})")
    report_lines.append(f"- Skeleton IDs: {len(skeleton_ids)} unique")
    report_lines.append("- Stage1 routing: validated against skeleton IDs")
    report_lines.append("- Stage2 required questions: validated")
    report_lines.append("- Bundle consistency: validated")

    if errors:
        report_lines.append("## Errors")
        for err in errors:
            report_lines.append(f"- {err}")
    else:
        report_lines.append("## Errors")
        report_lines.append("- None")

    if warnings:
        report_lines.append("## Warnings")
        for warn in warnings:
            report_lines.append(f"- {warn}")
    else:
        report_lines.append("## Warnings")
        report_lines.append("- None")

    if fixes:
        report_lines.append("## Fixes Applied")
        for fix in fixes:
            report_lines.append(f"- {fix}")
    else:
        report_lines.append("## Fixes Applied")
        report_lines.append("- None")

    args.report.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    if errors:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
