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
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "gen_config_fwd_headers.py"
SPEC = importlib.util.spec_from_file_location("gen_config_fwd_headers", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"failed to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class GenConfigFwdHeadersTest(unittest.TestCase):
    def test_real_use_staros_config_remains_guarded(self) -> None:
        blocks = MODULE.collect_config_blocks(MODULE.COMMON_CONFIG)
        rendered = MODULE.render_header(["starlet_port"], blocks)

        self.assertIn("#ifdef USE_STAROS", rendered)
        self.assertIn('CONF_Int32(starlet_port, "9070");', rendered)
        self.assertIn("#endif", rendered)

    def test_else_branch_is_preserved_for_selected_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.h"
            config_path.write_text(
                textwrap.dedent(
                    """\
                    #pragma once

                    namespace starrocks::config {
                    #ifdef FEATURE_FLAG
                    CONF_Int32(primary_branch, "1");
                    #else
                    CONF_Int32(fallback_branch, "2");
                    #endif
                    } // namespace starrocks::config
                    """
                )
            )

            blocks = MODULE._extract_config_blocks(config_path)
            rendered = MODULE.render_header(["fallback_branch"], blocks)

        self.assertIn("#ifdef FEATURE_FLAG", rendered)
        self.assertIn("#else", rendered)
        self.assertIn('CONF_Int32(fallback_branch, "2");', rendered)
        self.assertIn("#endif", rendered)
        self.assertNotIn('CONF_Int32(primary_branch, "1");', rendered)


if __name__ == "__main__":
    unittest.main()
