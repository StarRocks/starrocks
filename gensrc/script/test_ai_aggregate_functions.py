# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pathlib
import subprocess
import sys
import tempfile
import unittest


class AIAggregateFunctionsTest(unittest.TestCase):
    def test_single_canonical_aggregate_uses_named_json_controls(self):
        import functions
        aggregates = functions.ai_aggregate_functions
        reducers = [fn for fn in functions.vectorized_functions if fn[0] == 200155]
        self.assertEqual([200150], [fn[0] for fn in aggregates])
        self.assertEqual(1, len(reducers))
        for fn in aggregates + reducers:
            self.assertEqual(['VARCHAR'] * 3 + ['JSON'], fn[5][1:])
            self.assertEqual(['instruction', 'provider', 'model', 'options'],
                             [arg['name'] for arg in fn[7]['named_args']][1:])
            self.assertEqual('', fn[7]['named_args'][2]['default'])
            self.assertEqual('', fn[7]['named_args'][3]['default'])
            self.assertEqual({}, fn[7]['named_args'][4]['default'])

    def test_public_routing_and_hidden_execution_are_separate(self):
        with tempfile.TemporaryDirectory() as output:
            subprocess.run([sys.executable, str(pathlib.Path(__file__).with_name('gen_functions.py')),
                            '--java', output, '--cpp', output], check=True)
            java = pathlib.Path(output, 'com/starrocks/builtins/VectorizedBuiltinFunctions.java').read_text()
            cpp = pathlib.Path(output, 'opcode/AIFunctionDescriptors.inc').read_text()
            self.assertIn('AggregateFunction.createBuiltin("ai_agg"', java)
            self.assertIn('.put(200150L, new AIFunctionDescriptor(AICapability.CHAT, 3, 2))', java)
            self.assertIn('.put(200150L, new AIAggregateLoweringDescriptor', java)
            self.assertNotIn('AggregateFunction.createBuiltin("__ai_collect_text"', java)
            self.assertNotIn('ScalarFunction.createVectorizedBuiltin(200155L', java)
            self.assertNotIn('.fid = 200150,', cpp)
            for removed_id in (200151, 200152, 200153, 200156, 200157, 200158):
                self.assertNotRegex(java, rf'\b{removed_id}L?\b')
                self.assertNotRegex(cpp, rf'\b{removed_id}\b')
            self.assertIn('.fid = 200155,', cpp)
            self.assertIn('AIArgumentType::AGG_STATE', cpp)
            self.assertIn('AIExecutionKind::AGGREGATE_REDUCE', cpp)
            self.assertIn('AIExecutionKind::SINGLE_REQUEST', cpp)


class AIAggregateMetadataValidationTest(unittest.TestCase):
    def setUp(self):
        import gen_functions
        gen_functions.function_list.clear()
        gen_functions.function_set.clear()
        gen_functions.function_signature_set.clear()

    @staticmethod
    def add_reducers():
        import functions
        import gen_functions
        for fn in functions.vectorized_functions:
            gen_functions.add_function(fn)

    def test_reducers_require_typed_state_and_internal_execution(self):
        import copy
        import functions
        import gen_functions
        reducer = next(fn for fn in functions.vectorized_functions if fn[0] == 200155)
        for field, value in [('internal', False), ('execution_kind', 'SINGLE_REQUEST')]:
            invalid = copy.deepcopy(reducer)
            invalid[7]['ai'][field] = value
            with self.assertRaises(ValueError):
                gen_functions.validate_ai_metadata(invalid, invalid[7]['ai'], invalid[7]['named_args'])
        invalid = copy.deepcopy(reducer)
        invalid[5][0] = 'VARCHAR'
        with self.assertRaises(ValueError):
            gen_functions.validate_ai_metadata(invalid, invalid[7]['ai'], invalid[7]['named_args'])

    def test_public_aggregate_rejects_unknown_metadata(self):
        import copy
        import functions
        import gen_functions
        self.add_reducers()
        public = copy.deepcopy(functions.ai_aggregate_functions[0])
        public[7]['aggregate']['instruction_argument_typo'] = 1
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)

    def test_public_aggregate_rejects_missing_or_wrong_reducer(self):
        import copy
        import functions
        import gen_functions
        public = copy.deepcopy(functions.ai_aggregate_functions[0])
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)
        self.add_reducers()
        public[7]['aggregate']['reducer_function_id'] = 200156
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)
        public[7]['aggregate']['reducer_function_id'] = 200155
        public[7]['aggregate']['instruction'] = 'input'
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)

    def test_public_controls_must_match_reducer_contract(self):
        import copy
        import functions
        import gen_functions
        self.add_reducers()
        for index in (2, 3, 4):
            public = copy.deepcopy(functions.ai_aggregate_functions[0])
            public[5][index] = 'BIGINT'
            with self.assertRaises(ValueError):
                gen_functions.add_ai_aggregate_function(public)


if __name__ == '__main__':
    unittest.main()
