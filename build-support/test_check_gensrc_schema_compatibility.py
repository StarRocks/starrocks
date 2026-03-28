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

import importlib.util
import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "check_gensrc_schema_compatibility.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_gensrc_schema_compatibility", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CheckGensrcSchemaCompatibilityTest(unittest.TestCase):
    def test_changed_mode_rejects_new_thrift_required_field(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "sample.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TSample {
                      1: optional string existing_name
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TSample {
                      1: optional string existing_name
                      2: required string new_name
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["new_field_must_be_optional"], [issue.rule for issue in issues])
            self.assertEqual("gensrc/thrift/sample.thrift", issues[0].path)
            self.assertEqual("TSample", issues[0].container)
            self.assertEqual(2, issues[0].field_number)

    def test_changed_mode_rejects_new_unlabeled_thrift_rpc_param(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "service.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    service SampleService {
                      void ping(1: optional string request)
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    service SampleService {
                      void ping(1: optional string request, 2: string trace_id)
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["new_field_must_be_optional"], [issue.rule for issue in issues])
            self.assertEqual("SampleService.ping(params)", issues[0].container)
            self.assertEqual(2, issues[0].field_number)

    def test_changed_mode_rejects_idless_thrift_rpc_param_change(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "service.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    service SampleService {
                      void ping(string request)
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    service SampleService {
                      void ping(i64 request)
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["unsupported_syntax"], [issue.rule for issue in issues])
            self.assertEqual("gensrc/thrift/service.thrift", issues[0].path)
            self.assertIn("omit field ids", issues[0].detail)

    def test_changed_mode_checks_supported_fields_when_unsupported_syntax_is_unchanged(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "service.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    service SampleService {
                      void ping(string request)
                    }

                    struct TSample {
                      1: optional string existing_name
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    service SampleService {
                      void ping(string request)
                    }

                    struct TSample {
                      1: optional string existing_name
                      2: required string new_name
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["new_field_must_be_optional"], [issue.rule for issue in issues])
            self.assertEqual("TSample", issues[0].container)
            self.assertEqual(2, issues[0].field_number)

    def test_changed_mode_rejects_idless_thrift_struct_field(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "sample.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TSample {
                      1: optional string existing_name
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TSample {
                      1: optional string existing_name
                      string trace_id
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["unsupported_syntax"], [issue.rule for issue in issues])
            self.assertEqual("TSample", issues[0].container)
            self.assertIn("omit field ids", issues[0].detail)

    def test_changed_mode_checks_supported_fields_when_idless_struct_field_is_unchanged(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "sample.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TLegacy {
                      string trace_id
                    }

                    struct TSample {
                      1: optional string existing_name
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TLegacy {
                      string trace_id
                    }

                    struct TSample {
                      1: optional string existing_name
                      2: required string new_name
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["new_field_must_be_optional"], [issue.rule for issue in issues])
            self.assertEqual("TSample", issues[0].container)
            self.assertEqual(2, issues[0].field_number)

    def test_changed_mode_rejects_changed_non_last_unsupported_thrift_field(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "sample.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TLegacy {
                      string trace_id
                      string span_id
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TLegacy {
                      binary trace_id
                      string span_id
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["unsupported_syntax"], [issue.rule for issue in issues])
            self.assertEqual("TLegacy", issues[0].container)
            self.assertIn("binary trace_id", issues[0].detail)

    def test_changed_mode_ignores_line_drift_for_unchanged_unsupported_syntax(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "service.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    service SampleService {
                      void ping(string request)
                    }

                    struct TSample {
                      1: optional string existing_name
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    service SampleService {

                      void ping(string request)
                    }

                    struct TSample {
                      1: optional string existing_name
                      2: required string new_name
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["new_field_must_be_optional"], [issue.rule for issue in issues])
            self.assertEqual("TSample", issues[0].container)
            self.assertEqual(2, issues[0].field_number)

    def test_changed_mode_rejects_unsupported_thrift_union_change(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "legacy.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    union TLegacyUnion {
                      1: string name
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    union TLegacyUnion {
                      1: i64 name
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["unsupported_syntax"], [issue.rule for issue in issues])
            self.assertEqual("TLegacyUnion", issues[0].container)

    def test_changed_mode_rejects_unsupported_proto_oneof_change(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "sample.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto3";

                    message SamplePB {
                      oneof payload {
                        string name = 1;
                      }
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto3";

                    message SamplePB {
                      oneof payload {
                        int64 name = 1;
                      }
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["unsupported_syntax"], [issue.rule for issue in issues])
            self.assertEqual("SamplePB.payload", issues[0].container)

    def test_changed_mode_preserves_rename_source_paths(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "old.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      optional string name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            self._run_git(repo, "mv", "gensrc/proto/old.proto", "gensrc/proto/new.proto")
            renamed_path = repo / "gensrc" / "proto" / "new.proto"
            renamed_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      optional string name = 2;
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["field_deleted"], [issue.rule for issue in issues])
            self.assertEqual("gensrc/proto/old.proto", issues[0].path)

    def test_changed_mode_parses_multiline_thrift_fields(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "sample.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TSample {
                      1: optional map<
                          string,
                          string
                      > properties
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TSample {
                      1: optional map<
                          string,
                          i64
                      > properties
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["field_type_changed"], [issue.rule for issue in issues])
            self.assertEqual("TSample", issues[0].container)
            self.assertEqual(1, issues[0].field_number)

    def test_changed_mode_ignores_thrift_type_formatting_drift(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "sample.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TSample {
                      1: optional map<string,string> properties
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TSample {
                      1: optional map<string, string> properties
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual([], issues)

    def test_changed_mode_ignores_proto_type_formatting_drift(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "sample.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto3";

                    message SamplePB {
                      map<string,string> properties = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto3";

                    message SamplePB {
                      map<string, string> properties = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual([], issues)

    def test_changed_mode_allows_same_number_rename(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "rename.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TRename {
                      1: optional string old_name
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TRename {
                      1: optional string new_name
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual([], issues)

    def test_changed_mode_rejects_field_renumber(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "sample.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      optional string name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      optional string name = 2;
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["field_renumbered"], [issue.rule for issue in issues])
            self.assertEqual("SamplePB", issues[0].container)
            self.assertEqual(1, issues[0].field_number)

    def test_changed_mode_preserves_deep_proto_message_scope(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "nested.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message Outer {
                      message Inner {
                        message Leaf {
                          optional string name = 1;
                        }
                      }
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message Outer {
                      message Inner {
                        message Leaf {
                          optional int64 name = 1;
                        }
                      }
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["field_type_changed"], [issue.rule for issue in issues])
            self.assertEqual("Outer.Inner.Leaf", issues[0].container)
            self.assertEqual(1, issues[0].field_number)

    def test_changed_mode_rejects_proto3_bare_singular_addition(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "sample.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto3";

                    message SamplePB {
                      optional string existing_name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto3";

                    message SamplePB {
                      optional string existing_name = 1;
                      string new_name = 2;
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["proto3_field_must_be_explicit_optional"], [issue.rule for issue in issues])
            self.assertEqual("SamplePB", issues[0].container)
            self.assertEqual(2, issues[0].field_number)

    def test_changed_mode_tracks_proto_aggregate_option_braces(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "aggregate_option.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      option (sample.message) = {
                        enabled: true
                      };
                      optional string name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      option (sample.message) = {
                        enabled: true
                      };
                      optional int64 name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["field_type_changed"], [issue.rule for issue in issues])
            self.assertEqual("SamplePB", issues[0].container)
            self.assertEqual(1, issues[0].field_number)

    def test_deletion_requires_waiver(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "delete.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message DeletePB {
                      optional string existing_name = 1;
                      optional string removed_name = 2;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message DeletePB {
                      optional string existing_name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["field_deleted"], [issue.rule for issue in issues])
            self.assertEqual("DeletePB", issues[0].container)
            self.assertEqual(2, issues[0].field_number)
            self.assertEqual("optional string removed_name = 2", issues[0].base_signature)

    def test_matching_waiver_suppresses_deletion_violation(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "delete.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message DeletePB {
                      optional string existing_name = 1;
                      optional string removed_name = 2;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message DeletePB {
                      optional string existing_name = 1;
                    }
                    """
                )
            )
            waiver_path = repo / "build-support" / "schema_compatibility_waivers.json"
            waiver_path.write_text(
                json.dumps(
                    {
                        "waivers": [
                            {
                                "path": "gensrc/proto/delete.proto",
                                "container_or_method": "DeletePB",
                                "field_number": 2,
                                "field_name": "removed_name",
                                "rule": "field_deleted",
                                "base_signature": "optional string removed_name = 2",
                                "reason": "covered by an explicit compatibility migration",
                                "owner": "engprod",
                            }
                        ]
                    },
                    indent=2,
                )
                + "\n"
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual([], issues)

    def test_stale_waiver_is_reported_when_waiver_file_changes(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "sample.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      optional string existing_name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            waiver_path = repo / "build-support" / "schema_compatibility_waivers.json"
            waiver_path.write_text(
                json.dumps(
                    {
                        "waivers": [
                            {
                                "path": "gensrc/proto/sample.proto",
                                "container_or_method": "SamplePB",
                                "field_number": 9,
                                "field_name": "ghost_name",
                                "rule": "field_deleted",
                                "base_signature": "optional string ghost_name = 9",
                                "reason": "stale waiver coverage",
                                "owner": "engprod",
                            }
                        ]
                    },
                    indent=2,
                )
                + "\n"
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["stale_waiver"], [issue.rule for issue in issues])
            self.assertEqual("gensrc/proto/sample.proto", issues[0].path)

    def test_stale_waiver_is_reported_for_missing_schema_path(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "sample.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      optional string existing_name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            waiver_path = repo / "build-support" / "schema_compatibility_waivers.json"
            waiver_path.write_text(
                json.dumps(
                    {
                        "waivers": [
                            {
                                "path": "gensrc/proto/missing.proto",
                                "container_or_method": "MissingPB",
                                "field_number": 9,
                                "field_name": "ghost_name",
                                "rule": "field_deleted",
                                "base_signature": "optional string ghost_name = 9",
                                "reason": "stale waiver coverage",
                                "owner": "engprod",
                            }
                        ]
                    },
                    indent=2,
                )
                + "\n"
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["stale_waiver"], [issue.rule for issue in issues])
            self.assertEqual("gensrc/proto/missing.proto", issues[0].path)

    def test_full_mode_ignores_non_schema_files_under_gensrc(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            proto_path = repo / "gensrc" / "proto" / "sample.proto"
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message SamplePB {
                      optional string existing_name = 1;
                    }
                    """
                )
            )
            (repo / "gensrc" / "proto" / "Makefile").write_text("all:\n\t@true\n")
            self._commit_all(repo, "base")

            issues = module.check_repo(repo, mode="full", base="HEAD")

            self.assertEqual([], issues)

    def test_full_mode_includes_nested_schema_files(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            self._commit_all(repo, "base")

            proto_path = repo / "gensrc" / "proto" / "nested" / "sample.proto"
            proto_path.parent.mkdir(parents=True)
            proto_path.write_text(
                textwrap.dedent(
                    """\
                    syntax = "proto2";

                    message NestedPB {
                      required string new_name = 1;
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="full", base="HEAD~1")

            self.assertEqual(["new_field_must_be_optional"], [issue.rule for issue in issues])
            self.assertEqual("gensrc/proto/nested/sample.proto", issues[0].path)

    def test_full_mode_ignores_unchanged_unsupported_syntax(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "legacy.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    union TLegacyUnion {
                      1: string name
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            issues = module.check_repo(repo, mode="full", base="HEAD")

            self.assertEqual([], issues)

    def test_changed_mode_rejects_thrift_service_throws_type_change(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "service.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    exception TError {
                      1: optional string message
                    }

                    exception TOtherError {
                      1: optional string message
                    }

                    service SampleService {
                      void ping(1: optional string request) throws (1: optional TError err)
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    exception TError {
                      1: optional string message
                    }

                    exception TOtherError {
                      1: optional string message
                    }

                    service SampleService {
                      void ping(1: optional string request) throws (1: optional TOtherError err)
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["field_type_changed"], [issue.rule for issue in issues])
            self.assertEqual("SampleService.ping(throws)", issues[0].container)
            self.assertEqual(1, issues[0].field_number)

    def test_changed_mode_parses_thrift_field_annotations(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._init_repo(repo)
            thrift_path = repo / "gensrc" / "thrift" / "annotated.thrift"
            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TAnnotated {
                      1: optional i64 value (deprecated = "true")
                    }
                    """
                )
            )
            self._commit_all(repo, "base")

            thrift_path.write_text(
                textwrap.dedent(
                    """\
                    struct TAnnotated {
                      1: optional string value (deprecated = "true")
                    }
                    """
                )
            )
            self._commit_all(repo, "head")

            issues = module.check_repo(repo, mode="changed", base="HEAD~1")

            self.assertEqual(["field_type_changed"], [issue.rule for issue in issues])
            self.assertEqual("TAnnotated", issues[0].container)
            self.assertEqual(1, issues[0].field_number)

    def test_ci_pipeline_tracks_schema_checker_inputs(self) -> None:
        workflow_text = (Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline.yml").read_text()
        workflow_section = workflow_text.split("  schema-compatibility:\n", 1)[1].split("\n  thirdparty-info:\n", 1)[0]

        self.assertIn("name: Schema Compatibility", workflow_text)
        self.assertIn("- 'gensrc/proto/**'", workflow_text)
        self.assertIn("- 'gensrc/thrift/**'", workflow_text)
        self.assertIn("- 'build-support/check_gensrc_schema_compatibility.py'", workflow_text)
        self.assertIn("- 'build-support/test_check_gensrc_schema_compatibility.py'", workflow_text)
        self.assertIn("- 'build-support/schema_compatibility_waivers.json'", workflow_text)
        self.assertIn("git fetch origin ${{ github.base_ref }}\n", workflow_section)
        self.assertNotIn("git fetch origin ${{ github.base_ref }} --depth=1", workflow_section)

    def test_ci_pipeline_branch_tracks_schema_checker_inputs(self) -> None:
        workflow_text = (
            Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline-branch.yml"
        ).read_text()
        workflow_section = workflow_text.split("  schema-compatibility:\n", 1)[1].split("\n  be-checker:\n", 1)[0]

        self.assertIn("name: Schema Compatibility", workflow_text)
        self.assertIn("- 'gensrc/proto/**'", workflow_text)
        self.assertIn("- 'gensrc/thrift/**'", workflow_text)
        self.assertIn("- 'build-support/check_gensrc_schema_compatibility.py'", workflow_text)
        self.assertIn("- 'build-support/test_check_gensrc_schema_compatibility.py'", workflow_text)
        self.assertIn("- 'build-support/schema_compatibility_waivers.json'", workflow_text)
        self.assertIn("git fetch origin ${{ github.base_ref }}\n", workflow_section)
        self.assertNotIn("git fetch origin ${{ github.base_ref }} --depth=1", workflow_section)

    def _init_repo(self, repo: Path) -> None:
        (repo / "gensrc" / "proto").mkdir(parents=True)
        (repo / "gensrc" / "thrift").mkdir(parents=True)
        (repo / "build-support").mkdir()
        waiver_path = repo / "build-support" / "schema_compatibility_waivers.json"
        waiver_path.write_text('{"waivers": []}\n')

        self._run_git(repo, "init")
        self._run_git(repo, "config", "user.email", "test@example.com")
        self._run_git(repo, "config", "user.name", "Test User")

    def _commit_all(self, repo: Path, message: str) -> None:
        self._run_git(repo, "add", ".")
        self._run_git(repo, "commit", "-m", message)

    def _run_git(self, repo: Path, *args: str) -> None:
        subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True)


if __name__ == "__main__":
    unittest.main()
