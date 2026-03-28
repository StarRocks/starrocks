#!/usr/bin/env python3

# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_WAIVERS = "build-support/schema_compatibility_waivers.json"
SCHEMA_PREFIXES = ("gensrc/proto/", "gensrc/thrift/")
DEFAULT_CHANGED_FULL_CHECK_PATHS = {
    ".github/workflows/ci-pipeline.yml",
    ".github/workflows/ci-pipeline-branch.yml",
    "build-support/check_gensrc_schema_compatibility.py",
    "build-support/test_check_gensrc_schema_compatibility.py",
    DEFAULT_WAIVERS,
}


@dataclass(frozen=True)
class FieldDecl:
    path: str
    container: str
    container_kind: str
    number: int
    name: str
    type_repr: str
    cardinality: str
    line: int
    syntax: str | None = None

    @property
    def signature(self) -> str:
        if self.path.endswith(".proto"):
            prefix = ""
            if self.cardinality in {"optional", "required", "repeated"}:
                prefix = f"{self.cardinality} "
            return f"{prefix}{self.type_repr} {self.name} = {self.number}"

        prefix = ""
        if self.cardinality in {"optional", "required"}:
            prefix = f"{self.cardinality} "
        return f"{self.number}: {prefix}{self.type_repr} {self.name}"


@dataclass(frozen=True)
class ContainerDecl:
    name: str
    kind: str
    syntax: str | None = None


@dataclass(frozen=True)
class UnsupportedConstruct:
    kind: str
    scope: str
    construct: str
    detail: str

    @property
    def key(self) -> tuple[str, str]:
        return (self.kind, self.scope)


@dataclass
class ParsedSchema:
    path: str
    containers: dict[str, ContainerDecl]
    fields: dict[str, dict[int, FieldDecl]]
    unsupported: list[UnsupportedConstruct] = field(default_factory=list)


@dataclass(frozen=True)
class Waiver:
    path: str
    container_or_method: str
    field_number: int
    field_name: str
    rule: str
    base_signature: str | None
    reason: str
    owner: str


@dataclass(frozen=True)
class Violation:
    path: str
    container: str
    field_number: int | None
    field_name: str
    rule: str
    detail: str
    remediation: str
    base_signature: str | None = None


class UnsupportedSyntaxError(RuntimeError):
    def __init__(self, path: str, detail: str, kind: str, scope: str, construct: str):
        self.path = path
        self.detail = detail
        self.kind = kind
        self.scope = scope
        self.construct = construct
        super().__init__(detail)


def load_waivers(path: Path) -> list[Waiver]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text())
    waivers = []
    for raw in payload.get("waivers", []):
        waivers.append(
            Waiver(
                path=raw["path"],
                container_or_method=raw["container_or_method"],
                field_number=int(raw["field_number"]),
                field_name=raw["field_name"],
                rule=raw["rule"],
                base_signature=raw.get("base_signature"),
                reason=raw["reason"],
                owner=raw["owner"],
            )
        )
    return waivers


def check_repo(
    repo_root: Path,
    mode: str = "full",
    base: str | None = None,
    waivers_path: Path | None = None,
) -> list[Violation]:
    repo_root = repo_root.resolve()
    waivers_path = waivers_path or (repo_root / DEFAULT_WAIVERS)
    selected_paths = select_schema_paths(repo_root, mode, base)
    all_schema_paths = _all_schema_paths(repo_root, base) if base is not None else set(selected_paths)
    waivers = load_waivers(waivers_path)

    violations: list[Violation] = []
    for repo_path in sorted(selected_paths):
        violations.extend(compare_schema_path(repo_root, repo_path, base))

    active_keys = {
        (issue.path, issue.container, issue.field_number, issue.field_name, issue.rule, issue.base_signature): issue
        for issue in violations
    }

    used_waivers: set[Waiver] = set()
    filtered: list[Violation] = []
    for issue in violations:
        matching = _match_waiver(issue, waivers)
        if matching is not None:
            used_waivers.add(matching)
            continue
        filtered.append(issue)

    scoped_waivers = [waiver for waiver in waivers if waiver.path in selected_paths or waiver.path not in all_schema_paths]
    for waiver in scoped_waivers:
        key = (
            waiver.path,
            waiver.container_or_method,
            waiver.field_number,
            waiver.field_name,
            waiver.rule,
            waiver.base_signature,
        )
        if waiver in used_waivers:
            continue
        if key in active_keys:
            continue
        filtered.append(
            Violation(
                path=waiver.path,
                container=waiver.container_or_method,
                field_number=waiver.field_number,
                field_name=waiver.field_name,
                rule="stale_waiver",
                detail=f"waiver owned by {waiver.owner} no longer matches a live compatibility violation",
                remediation="Remove or update the stale waiver entry.",
                base_signature=waiver.base_signature,
            )
        )

    return sorted(filtered, key=lambda item: (item.path, item.container, item.field_number or -1, item.rule, item.field_name))


def select_schema_paths(repo_root: Path, mode: str, base: str | None) -> set[str]:
    if mode == "changed":
        if not base:
            raise ValueError("--base is required when --mode changed")
        changed_paths = _git_changed_paths(repo_root, base)
        if changed_paths & DEFAULT_CHANGED_FULL_CHECK_PATHS:
            return _all_schema_paths(repo_root, base)
        return {path for path in changed_paths if _is_schema_path(path)}

    if mode == "full":
        if not base:
            raise ValueError("--base is required when --mode full")
        return _all_schema_paths(repo_root, base)

    raise ValueError(f"unsupported mode: {mode}")


def compare_schema_path(repo_root: Path, repo_path: str, base: str | None) -> list[Violation]:
    base_text = _git_read_text(repo_root, base, repo_path) if base else None
    head_path = repo_root / repo_path
    head_text = head_path.read_text() if head_path.exists() else None

    base_schema = None
    head_schema = None
    base_error: UnsupportedSyntaxError | None = None
    head_error: UnsupportedSyntaxError | None = None
    try:
        base_schema = parse_schema(repo_path, base_text) if base_text is not None else None
    except UnsupportedSyntaxError as error:
        base_error = error
    try:
        head_schema = parse_schema(repo_path, head_text) if head_text is not None else None
    except UnsupportedSyntaxError as error:
        head_error = error

    if base_error or head_error:
        if (
            base_error is not None
            and head_error is not None
            and base_text == head_text
            and base_error.detail == head_error.detail
        ):
            return []
        error = head_error or base_error
        return [
            Violation(
                path=repo_path,
                container=head_error.scope if head_error is not None else base_error.scope,
                field_number=None,
                field_name="",
                rule="unsupported_syntax",
                detail=error.detail,
                remediation="Avoid unsupported schema constructs in v1 of the compatibility harness.",
            )
        ]

    unsupported_issues = _compare_unsupported_constructs(repo_path, base_schema, head_schema)
    if unsupported_issues:
        return unsupported_issues

    return compare_schemas(repo_path, base_schema, head_schema)


def _compare_unsupported_constructs(
    repo_path: str,
    base_schema: ParsedSchema | None,
    head_schema: ParsedSchema | None,
) -> list[Violation]:
    base_schema = base_schema or ParsedSchema(repo_path, {}, {})
    head_schema = head_schema or ParsedSchema(repo_path, {}, {})

    base_by_key: defaultdict[tuple[str, str], list[UnsupportedConstruct]] = defaultdict(list)
    head_by_key: defaultdict[tuple[str, str], list[UnsupportedConstruct]] = defaultdict(list)
    for item in base_schema.unsupported:
        base_by_key[item.key].append(item)
    for item in head_schema.unsupported:
        head_by_key[item.key].append(item)

    issues: list[Violation] = []
    for key in sorted(set(base_by_key) | set(head_by_key)):
        base_items = base_by_key.get(key, [])
        head_items = head_by_key.get(key, [])
        base_constructs = Counter(item.construct for item in base_items)
        head_constructs = Counter(item.construct for item in head_items)
        if base_constructs == head_constructs:
            continue

        changed_items = _select_unsupported_constructs(head_items, head_constructs - base_constructs)
        if not changed_items:
            changed_items = _select_unsupported_constructs(base_items, base_constructs - head_constructs)
        for item in changed_items:
            issues.append(
                Violation(
                    path=repo_path,
                    container=item.scope,
                    field_number=None,
                    field_name="",
                    rule="unsupported_syntax",
                    detail=item.detail,
                    remediation="Avoid unsupported schema constructs in v1 of the compatibility harness.",
                )
            )

    return issues


def _select_unsupported_constructs(
    items: list[UnsupportedConstruct],
    construct_counts: Counter[str],
) -> list[UnsupportedConstruct]:
    remaining = construct_counts.copy()
    selected: list[UnsupportedConstruct] = []
    for item in items:
        if remaining[item.construct] <= 0:
            continue
        selected.append(item)
        remaining[item.construct] -= 1
    return selected


def _normalize_type_repr(type_repr: str) -> str:
    normalized = " ".join(type_repr.split())
    return re.sub(r"\s*([<>,()])\s*", r"\1", normalized)


def compare_schemas(repo_path: str, base_schema: ParsedSchema | None, head_schema: ParsedSchema | None) -> list[Violation]:
    base_schema = base_schema or ParsedSchema(repo_path, {}, {})
    head_schema = head_schema or ParsedSchema(repo_path, {}, {})

    issues: list[Violation] = []
    container_names = set(base_schema.containers) | set(head_schema.containers)
    for container_name in sorted(container_names):
        base_fields = dict(base_schema.fields.get(container_name, {}))
        head_fields = dict(head_schema.fields.get(container_name, {}))
        container_decl = head_schema.containers.get(container_name) or base_schema.containers.get(container_name)
        if container_decl is None:
            continue

        common_numbers = set(base_fields) & set(head_fields)
        for number in sorted(common_numbers):
            before = base_fields[number]
            after = head_fields[number]
            if _normalize_type_repr(before.type_repr) != _normalize_type_repr(after.type_repr):
                issues.append(
                    Violation(
                        path=repo_path,
                        container=container_name,
                        field_number=number,
                        field_name=after.name,
                        rule="field_type_changed",
                        detail=f"field {after.name} changed type from {before.type_repr} to {after.type_repr}",
                        remediation="Keep the existing field type and add a new field number for incompatible data.",
                        base_signature=before.signature,
                    )
                )
                continue
            if before.cardinality != after.cardinality:
                issues.append(
                    Violation(
                        path=repo_path,
                        container=container_name,
                        field_number=number,
                        field_name=after.name,
                        rule="field_cardinality_changed",
                        detail=f"field {after.name} changed cardinality from {before.cardinality} to {after.cardinality}",
                        remediation="Keep cardinality stable for existing fields and add a new field when behavior must change.",
                        base_signature=before.signature,
                    )
                )

        added_numbers = {number: field for number, field in head_fields.items() if number not in base_fields}
        deleted_numbers = {number: field for number, field in base_fields.items() if number not in head_fields}
        renumber_pairs = _match_renumbered_fields(added_numbers, deleted_numbers)

        for before, after in renumber_pairs:
            issues.append(
                Violation(
                    path=repo_path,
                    container=container_name,
                    field_number=before.number,
                    field_name=before.name,
                    rule="field_renumbered",
                    detail=f"field {before.name} moved from {before.number} to {after.number}",
                    remediation="Keep the original field number and add a new field for incompatible evolution.",
                    base_signature=before.signature,
                )
            )
            added_numbers.pop(after.number, None)
            deleted_numbers.pop(before.number, None)

        for number in sorted(deleted_numbers):
            before = deleted_numbers[number]
            issues.append(
                Violation(
                    path=repo_path,
                    container=container_name,
                    field_number=number,
                    field_name=before.name,
                    rule="field_deleted",
                    detail=f"field {before.name} was removed from {container_name}",
                    remediation="Keep the field definition in place or add a waiver entry for a reviewed compatibility exception.",
                    base_signature=before.signature,
                )
            )

        for number in sorted(added_numbers):
            after = added_numbers[number]
            rule = _new_field_rule(after, container_decl)
            if rule is None:
                continue
            issues.append(
                Violation(
                    path=repo_path,
                    container=container_name,
                    field_number=number,
                    field_name=after.name,
                    rule=rule,
                    detail=_new_field_detail(after, container_decl),
                    remediation="Use an optional-compatible new field shape for added schema surface.",
                )
            )

    return issues


def parse_schema(path: str, text: str) -> ParsedSchema:
    if path.endswith(".thrift"):
        return parse_thrift_schema(path, text)
    if path.endswith(".proto"):
        return parse_proto_schema(path, text)
    raise ValueError(f"unsupported schema path: {path}")


def parse_thrift_schema(path: str, text: str) -> ParsedSchema:
    lines = _strip_comments(text).splitlines()
    containers: dict[str, ContainerDecl] = {}
    fields: dict[str, dict[int, FieldDecl]] = defaultdict(dict)
    unsupported: list[UnsupportedConstruct] = []

    index = 0
    while index < len(lines):
        line = lines[index].strip()
        if not line:
            index += 1
            continue
        union_match = re.match(r"union\s+([A-Za-z_]\w*)\s*\{", line)
        if union_match:
            union_name = union_match.group(1)
            block_lines = [line]
            unsupported.append(
                UnsupportedConstruct(
                    kind="thrift_union",
                    scope=union_name,
                    construct="",
                    detail="thrift union parsing is not supported by the schema compatibility harness",
                )
            )
            index += 1
            while index < len(lines):
                body_line = lines[index].strip()
                if body_line:
                    block_lines.append(body_line)
                if body_line == "}":
                    break
                index += 1
            unsupported[-1] = UnsupportedConstruct(
                kind="thrift_union",
                scope=union_name,
                construct="\n".join(block_lines),
                detail="thrift union parsing is not supported by the schema compatibility harness",
            )
            index += 1
            continue

        container_match = re.match(r"(struct|exception)\s+([A-Za-z_]\w*)\s*\{", line)
        if container_match:
            kind, name = container_match.groups()
            containers[name] = ContainerDecl(name=name, kind=kind)
            index += 1
            current_field: list[str] = []
            current_start = index + 1
            while index < len(lines):
                body_line = lines[index].strip()
                if body_line == "}":
                    break
                if not body_line:
                    index += 1
                    continue
                if re.match(r"^\d+\s*:", body_line) and current_field:
                    try:
                        field = _parse_or_raise_thrift_field(path, name, kind, " ".join(current_field), current_start)
                        fields[name][field.number] = field
                    except UnsupportedSyntaxError as error:
                        unsupported.append(
                            UnsupportedConstruct(
                                kind=error.kind,
                                scope=error.scope,
                                construct=error.construct,
                                detail=error.detail,
                            )
                        )
                    current_field = [body_line]
                    current_start = index + 1
                else:
                    if not current_field:
                        current_start = index + 1
                    current_field.append(body_line)
                candidate = " ".join(current_field)
                if _has_balanced_type_delimiters(candidate):
                    try:
                        field = _parse_or_raise_thrift_field(path, name, kind, candidate, current_start)
                        fields[name][field.number] = field
                    except UnsupportedSyntaxError as error:
                        unsupported.append(
                            UnsupportedConstruct(
                                kind=error.kind,
                                scope=error.scope,
                                construct=error.construct,
                                detail=error.detail,
                            )
                        )
                    current_field = []
                index += 1
            index += 1
            continue

        service_match = re.match(r"service\s+([A-Za-z_]\w*)(?:\s+extends\s+[A-Za-z_][\w.]*)?\s*\{", line)
        if service_match:
            service_name = service_match.group(1)
            index += 1
            current: list[str] = []
            current_start = index + 1
            balance = 0
            while index < len(lines):
                stripped = lines[index].strip()
                if stripped == "}":
                    break
                if not stripped:
                    index += 1
                    continue
                if not current:
                    current_start = index + 1
                current.append(stripped)
                balance += stripped.count("(") - stripped.count(")")
                if balance <= 0 and "(" in " ".join(current):
                    signature = " ".join(current)
                    try:
                        _parse_thrift_method_signature(path, service_name, signature, current_start, containers, fields)
                    except UnsupportedSyntaxError as error:
                        unsupported.append(
                            UnsupportedConstruct(
                                kind=error.kind,
                                scope=error.scope,
                                construct=error.construct,
                                detail=error.detail,
                            )
                        )
                    current = []
                    balance = 0
                index += 1
            index += 1
            continue

        index += 1

    return ParsedSchema(path=path, containers=containers, fields=fields, unsupported=unsupported)


def parse_proto_schema(path: str, text: str) -> ParsedSchema:
    stripped = _strip_comments(text)
    lines = stripped.splitlines()
    syntax = None
    containers: dict[str, ContainerDecl] = {}
    fields: dict[str, dict[int, FieldDecl]] = defaultdict(dict)
    unsupported: list[UnsupportedConstruct] = []
    block_stack: list[tuple[str, str]] = []
    oneof_capture_stack: list[tuple[str, list[str]]] = []
    message_stack: list[str] = []
    aggregate_option_depth = 0

    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line:
            close_count = raw_line.count("}")
            for _ in range(close_count):
                if block_stack:
                    kind, _ = block_stack.pop()
                    if kind == "message" and message_stack:
                        message_stack.pop()
            continue

        if syntax is None:
            syntax_match = re.match(r'syntax\s*=\s*"(proto[23])"\s*;', line)
            if syntax_match:
                syntax = syntax_match.group(1)
                continue

        message_match = re.match(r"message\s+([A-Za-z_]\w*)\s*\{", line)
        if message_match:
            name = message_match.group(1)
            full_name = f"{message_stack[-1]}.{name}" if message_stack else name
            containers[full_name] = ContainerDecl(name=full_name, kind="message", syntax=syntax)
            block_stack.append(("message", full_name))
            message_stack.append(full_name)
            continue

        oneof_match = re.match(r"oneof\s+([A-Za-z_]\w*)\s*\{", line)
        if oneof_match:
            current_message = message_stack[-1] if message_stack else ""
            oneof_name = oneof_match.group(1)
            scope = f"{current_message}.{oneof_name}" if current_message else oneof_name
            block_stack.append(("oneof", scope))
            oneof_capture_stack.append((scope, [line]))
            continue

        enum_match = re.match(r"enum\s+([A-Za-z_]\w*)\s*\{", line)
        if enum_match:
            block_stack.append(("enum", enum_match.group(1)))
            continue

        if oneof_capture_stack:
            oneof_capture_stack[-1][1].append(line)

        if aggregate_option_depth > 0 or _is_proto_aggregate_option_line(line):
            aggregate_option_depth += raw_line.count("{") - raw_line.count("}")
            continue

        current_message = message_stack[-1] if message_stack else None
        if current_message and block_stack and block_stack[-1][0] == "message":
            field = _parse_proto_field_line(path, current_message, syntax, raw_line, line_number)
            if field is not None:
                fields[current_message][field.number] = field

        close_count = raw_line.count("}")
        for _ in range(close_count):
            if block_stack:
                kind, _ = block_stack.pop()
                if kind == "oneof":
                    scope, capture_lines = oneof_capture_stack.pop()
                    unsupported.append(
                        UnsupportedConstruct(
                            kind="proto_oneof",
                            scope=scope,
                            construct="\n".join(capture_lines),
                            detail="proto oneof parsing is not supported by the schema compatibility harness",
                        )
                    )
                if kind == "message" and message_stack:
                    message_stack.pop()

    return ParsedSchema(path=path, containers=containers, fields=fields, unsupported=unsupported)


def _parse_thrift_field_line(path: str, container: str, container_kind: str, raw_line: str, line_number: int) -> FieldDecl | None:
    match = re.match(
        r"^\s*(\d+)\s*:\s*(?:(required|optional)\s+)?(.+?)\s+([A-Za-z_]\w*)"
        r"\s*(?:=\s*[^,;()]+)?\s*(?:\([^)]*\)\s*)*[,;]?\s*$",
        raw_line.strip(),
    )
    if match is None:
        return None
    number, cardinality, type_repr, name = match.groups()
    return FieldDecl(
        path=path,
        container=container,
        container_kind=container_kind,
        number=int(number),
        name=name,
        type_repr=type_repr.strip(),
        cardinality=cardinality or "unlabeled",
        line=line_number,
    )


def _parse_or_raise_thrift_field(
    path: str,
    container: str,
    container_kind: str,
    raw_line: str,
    line_number: int,
) -> FieldDecl:
    field = _parse_thrift_field_line(path, container, container_kind, raw_line, line_number)
    if field is not None:
        return field
    if _looks_like_idless_thrift_field(raw_line):
        raise UnsupportedSyntaxError(
            path,
            f"thrift fields that omit field ids are not supported by the schema compatibility harness: "
            f"{container} line {line_number}: {raw_line.strip()}",
            kind="thrift_fields",
            scope=container,
            construct=raw_line.strip(),
        )
    raise UnsupportedSyntaxError(
        path,
        f"unsupported thrift field syntax in the schema compatibility harness: "
        f"{container} line {line_number}: {raw_line.strip()}",
        kind="thrift_fields",
        scope=container,
        construct=raw_line.strip(),
    )


def _looks_like_idless_thrift_field(raw_line: str) -> bool:
    return (
        re.match(
            r"^\s*(?:(required|optional)\s+)?(.+?)\s+([A-Za-z_]\w*)"
            r"\s*(?:=\s*[^,;()]+)?\s*(?:\([^)]*\)\s*)*[,;]?\s*$",
            raw_line.strip(),
        )
        is not None
    )


def _looks_like_idless_thrift_argument(argument: str) -> bool:
    return (
        re.match(
            r"^(?:(required|optional)\s+)?(.+?)\s+([A-Za-z_]\w*)\s*(?:=\s*[^,;]+)?\s*$",
            argument,
        )
        is not None
    )


def _parse_thrift_method_signature(
    path: str,
    service_name: str,
    signature: str,
    line_number: int,
    containers: dict[str, ContainerDecl],
    fields: dict[str, dict[int, FieldDecl]],
) -> None:
    match = re.match(
        r"^\s*(.+?)\s+([A-Za-z_]\w*)\s*\((.*?)\)\s*(?:throws\s*\((.*?)\))?\s*;?\s*$",
        signature,
    )
    if match is None:
        return
    _, method_name, params_text, throws_text = match.groups()
    params_container = f"{service_name}.{method_name}(params)"
    containers[params_container] = ContainerDecl(name=params_container, kind="service_params")
    for field in _parse_thrift_arguments(path, params_container, "service_params", params_text, line_number):
        fields[params_container][field.number] = field

    if throws_text is not None:
        throws_container = f"{service_name}.{method_name}(throws)"
        containers[throws_container] = ContainerDecl(name=throws_container, kind="service_throws")
        for field in _parse_thrift_arguments(path, throws_container, "service_throws", throws_text, line_number):
            fields[throws_container][field.number] = field


def _parse_thrift_arguments(
    path: str,
    container: str,
    container_kind: str,
    arguments: str,
    line_number: int,
) -> list[FieldDecl]:
    parsed = []
    for part in _split_top_level(arguments, ","):
        stripped = part.strip()
        if not stripped:
            continue
        field = _parse_thrift_field_line(path, container, container_kind, stripped, line_number)
        if field is not None:
            parsed.append(field)
            continue
        if _looks_like_idless_thrift_argument(stripped):
            raise UnsupportedSyntaxError(
                path,
                f"thrift service arguments that omit field ids are not supported by the schema compatibility harness: "
                f"{container} line {line_number}: {stripped}",
                kind="thrift_service_arguments",
                scope=container,
                construct=stripped,
            )
        raise UnsupportedSyntaxError(
            path,
            f"unsupported thrift service argument syntax in the schema compatibility harness: "
            f"{container} line {line_number}: {stripped}",
            kind="thrift_service_arguments",
            scope=container,
            construct=stripped,
        )
    return parsed


def _parse_proto_field_line(path: str, container: str, syntax: str | None, raw_line: str, line_number: int) -> FieldDecl | None:
    stripped = raw_line.strip()
    if not stripped or stripped.startswith(("option ", "reserved ", "extensions ", "extend ")):
        return None

    match = re.match(
        r"^\s*(?:(optional|required|repeated)\s+)?(.+?)\s+([A-Za-z_]\w*)\s*=\s*(\d+)\b",
        stripped,
    )
    if match is None:
        return None

    cardinality, type_repr, name, number = match.groups()
    type_repr = type_repr.strip()
    if cardinality is None:
        if type_repr.startswith("map<"):
            cardinality = "map"
        else:
            cardinality = "implicit"

    return FieldDecl(
        path=path,
        container=container,
        container_kind="message",
        number=int(number),
        name=name,
        type_repr=type_repr,
        cardinality=cardinality,
        line=line_number,
        syntax=syntax,
    )


def _new_field_rule(field: FieldDecl, container: ContainerDecl) -> str | None:
    if field.path.endswith(".thrift"):
        if field.cardinality != "optional":
            return "new_field_must_be_optional"
        return None

    syntax = container.syntax
    if syntax == "proto3":
        if field.cardinality not in {"optional", "repeated", "map"}:
            return "proto3_field_must_be_explicit_optional"
        return None

    if field.cardinality in {"optional", "repeated", "map"}:
        return None
    return "new_field_must_be_optional"


def _new_field_detail(field: FieldDecl, container: ContainerDecl) -> str:
    if field.path.endswith(".proto") and container.syntax == "proto3":
        return f"new proto3 field {field.name} must use explicit optional or repeated semantics"
    return f"new field {field.name} must be optional-compatible for forward and backward compatibility"


def _match_renumbered_fields(
    added_fields: dict[int, FieldDecl],
    deleted_fields: dict[int, FieldDecl],
) -> list[tuple[FieldDecl, FieldDecl]]:
    deleted_by_shape: dict[tuple[str, str, str], list[FieldDecl]] = defaultdict(list)
    added_by_shape: dict[tuple[str, str, str], list[FieldDecl]] = defaultdict(list)

    for field in deleted_fields.values():
        deleted_by_shape[(field.name, _normalize_type_repr(field.type_repr), field.cardinality)].append(field)
    for field in added_fields.values():
        added_by_shape[(field.name, _normalize_type_repr(field.type_repr), field.cardinality)].append(field)

    pairs: list[tuple[FieldDecl, FieldDecl]] = []
    for shape, deleted in deleted_by_shape.items():
        added = added_by_shape.get(shape, [])
        if len(deleted) != 1 or len(added) != 1:
            continue
        pairs.append((deleted[0], added[0]))
    return pairs


def _match_waiver(issue: Violation, waivers: list[Waiver]) -> Waiver | None:
    for waiver in waivers:
        if waiver.path != issue.path:
            continue
        if waiver.container_or_method != issue.container:
            continue
        if waiver.field_number != issue.field_number:
            continue
        if waiver.field_name != issue.field_name:
            continue
        if waiver.rule != issue.rule:
            continue
        if waiver.base_signature != issue.base_signature:
            continue
        return waiver
    return None


def _strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", lambda match: "\n" * match.group(0).count("\n"), text, flags=re.S)
    cleaned = []
    for line in text.splitlines():
        comment_index = line.find("//")
        if comment_index >= 0:
            line = line[:comment_index]
        cleaned.append(line)
    return "\n".join(cleaned)


def _split_top_level(text: str, delimiter: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    angle_depth = 0
    paren_depth = 0
    for char in text:
        if char == "<":
            angle_depth += 1
        elif char == ">" and angle_depth > 0:
            angle_depth -= 1
        elif char == "(":
            paren_depth += 1
        elif char == ")" and paren_depth > 0:
            paren_depth -= 1
        if char == delimiter and angle_depth == 0 and paren_depth == 0:
            item = "".join(current).strip()
            if item:
                items.append(item)
            current = []
            continue
        current.append(char)
    item = "".join(current).strip()
    if item:
        items.append(item)
    return items


def _has_balanced_type_delimiters(text: str) -> bool:
    angle_depth = 0
    paren_depth = 0
    for char in text:
        if char == "<":
            angle_depth += 1
        elif char == ">" and angle_depth > 0:
            angle_depth -= 1
        elif char == "(":
            paren_depth += 1
        elif char == ")" and paren_depth > 0:
            paren_depth -= 1
    return angle_depth == 0 and paren_depth == 0


def _is_proto_aggregate_option_line(line: str) -> bool:
    if not re.match(r"option\b", line):
        return False
    equals_index = line.find("=")
    brace_index = line.find("{")
    return equals_index >= 0 and brace_index > equals_index


def _is_schema_path(path: str) -> bool:
    if not any(path.startswith(prefix) for prefix in SCHEMA_PREFIXES):
        return False
    return path.endswith(".proto") or path.endswith(".thrift")


def _all_schema_paths(repo_root: Path, base: str) -> set[str]:
    current_paths = {
        path.relative_to(repo_root).as_posix()
        for root in (repo_root / "gensrc" / "proto", repo_root / "gensrc" / "thrift")
        if root.exists()
        for path in root.rglob("*")
        if path.is_file() and _is_schema_path(path.relative_to(repo_root).as_posix())
    }
    base_paths = set(_git_list_paths(repo_root, base, "gensrc/proto", "gensrc/thrift"))
    return current_paths | base_paths


def _git_changed_paths(repo_root: Path, base: str) -> set[str]:
    result = subprocess.run(
        ["git", "diff", "--name-status", "--find-renames", f"{base}...HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    changed_paths: set[str] = set()
    for raw_line in result.stdout.splitlines():
        if not raw_line.strip():
            continue
        parts = raw_line.split("\t")
        status = parts[0]
        if status.startswith(("R", "C")) and len(parts) >= 3:
            paths = parts[1:3]
        else:
            paths = parts[1:]
        changed_paths.update(path.strip() for path in paths if path.strip())
    return changed_paths


def _git_list_paths(repo_root: Path, ref: str, *prefixes: str) -> set[str]:
    command = ["git", "ls-tree", "-r", "--name-only", ref, "--", *prefixes]
    result = subprocess.run(command, cwd=repo_root, check=True, capture_output=True, text=True)
    return {line.strip() for line in result.stdout.splitlines() if _is_schema_path(line.strip())}


def _git_read_text(repo_root: Path, ref: str | None, repo_path: str) -> str | None:
    if ref is None:
        return None
    result = subprocess.run(
        ["git", "show", f"{ref}:{repo_path}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return result.stdout
    missing_markers = ("exists on disk, but not in", "does not exist in", "pathspec", "fatal: path")
    if any(marker in result.stderr for marker in missing_markers):
        return None
    result.check_returncode()
    return None


def _print_issues(issues: list[Violation]) -> None:
    for issue in issues:
        location = f"path={issue.path}"
        if issue.container:
            location += f" container={issue.container}"
        if issue.field_number is not None:
            location += f" field={issue.field_number}"
        if issue.field_name:
            location += f" name={issue.field_name}"
        print(f"[schema] rule={issue.rule} {location}")
        print(f"  {issue.detail}")
        print(f"  remediation: {issue.remediation}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check compatibility rules for gensrc thrift and proto schema changes.")
    parser.add_argument("--mode", choices=("full", "changed"), default="full")
    parser.add_argument("--base", help="Git base ref used for compatibility comparison")
    parser.add_argument("--waivers", default=DEFAULT_WAIVERS, help="Path to the schema compatibility waiver JSON")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    try:
        issues = check_repo(repo_root, mode=args.mode, base=args.base, waivers_path=repo_root / args.waivers)
    except ValueError as error:
        parser.error(str(error))
        return 2

    if issues:
        _print_issues(issues)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
