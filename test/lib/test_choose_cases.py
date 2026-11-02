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
"""
Tests for ChooseCase.read_t_r_file.

Needs the packages run.py needs, but no cluster: ChooseCase is built without its __init__, which
connects to one and scans the whole case tree. Run from test/: python3 -m unittest lib.test_choose_cases
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import LOOP_FLAG  # noqa: E402
from lib.choose_cases import ChooseCase  # noqa: E402

TWO_LOOPS = """-- name: test_two_loops
LOOP {
  PROPERTY: {"timeout": 10, "interval": 1, "desc": "first"}
  result=SELECT 1;
  CHECK: int(${result}[0][0]) == 1
} END LOOP
LOOP {
  PROPERTY: {"timeout": 10, "interval": 1, "desc": "second"}
  result=SELECT 2;
  CHECK: int(${result}[0][0]) == 2
} END LOOP
"""


class TestReadLoops(unittest.TestCase):
    def read_loops(self, case_text):
        with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as case_file:
            case_file.write(case_text)
        self.addCleanup(os.remove, case_file.name)
        os.environ.setdefault("attr", "")
        chooser = ChooseCase.__new__(ChooseCase)
        chooser.case_list = []
        chooser.read_t_r_file(case_file.name, None)
        self.assertEqual(1, len(chooser.case_list))
        return [stat for stat in chooser.case_list[0].sql if isinstance(stat, dict) and stat["type"] == LOOP_FLAG]

    def test_each_loop_keeps_its_own_statements(self):
        loops = self.read_loops(TWO_LOOPS)

        self.assertEqual(["first", "second"], [loop["prop"]["desc"] for loop in loops])
        self.assertIn("SELECT 1;", "\n".join(loops[0]["stat"]))
        self.assertNotIn("SELECT 2;", "\n".join(loops[0]["stat"]))
        self.assertIn("SELECT 2;", "\n".join(loops[1]["stat"]))


if __name__ == "__main__":
    unittest.main()
