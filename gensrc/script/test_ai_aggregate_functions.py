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
    def test_aggregate_routes_only_through_default_chat_provider(self):
        import functions
        aggregates = functions.ai_aggregate_functions
        reducers = [fn for fn in functions.ai_vectorized_functions if fn[0] in (200155, 200156)]
        self.assertEqual([200150, 200151], [fn[0] for fn in aggregates])
        for fn in aggregates + reducers:
            self.assertEqual('PROVIDER', fn[7]['model_source'])
            self.assertEqual('CHAT', fn[7]['capability'])
            self.assertEqual(-1, fn[7]['provider_argument'])
            self.assertEqual(-1, fn[7]['model_argument'])
            self.assertIn(len(fn[5]), (2, 3))

    def test_public_routing_and_hidden_execution_are_separate(self):
        with tempfile.TemporaryDirectory() as output:
            subprocess.run([sys.executable, str(pathlib.Path(__file__).with_name('gen_functions.py')),
                            '--java', output, '--cpp', output], check=True)
            java = pathlib.Path(output, 'com/starrocks/builtins/VectorizedBuiltinFunctions.java').read_text()
            cpp = pathlib.Path(output, 'opcode/AIFunctionDescriptors.inc').read_text()
            self.assertIn('addVectorizedAIAggregateBuiltin(200150, "ai_agg"', java)
            self.assertIn('addVectorizedAIAggregateBuiltin(200151, "ai_agg"', java)
            self.assertIn('.put(200150L, new AIFunctionDescriptor', java)
            self.assertIn('.put(200150L, new AIAggregateLoweringDescriptor', java)
            for name in ('__ai_collect_text', '__ai_agg_reduce'):
                self.assertNotRegex(java, r'functionSet\.\w+\([^\n]*"' + name + '"')
            self.assertNotIn('.fid = 200150,', cpp)
            for removed_id in (200152, 200153, 200157, 200158):
                self.assertNotRegex(java, rf'\b{removed_id}L?\b')
                self.assertNotRegex(cpp, rf'\b{removed_id}\b')
            self.assertIn('.fid = 200155,', cpp)
            self.assertIn('.fid = 200156,', cpp)
            self.assertIn('AIArgumentType::AGG_STATE', cpp)
            self.assertIn('AIExecutionKind::AGGREGATE_REDUCE', cpp)
            self.assertIn('AIExecutionKind::SINGLE_REQUEST', cpp)

class AIAggregateMetadataValidationTest(unittest.TestCase):
    def test_reducers_require_typed_state_and_internal_execution(self):
        import copy
        import functions
        import gen_functions
        reducer = next(fn for fn in functions.ai_vectorized_functions if fn[0] == 200155)
        for field, value in [('internal', False), ('execution_kind', 'SINGLE_REQUEST')]:
            invalid = copy.deepcopy(reducer)
            invalid[7][field] = value
            with self.assertRaises(ValueError):
                gen_functions.validate_ai_metadata(invalid)
        invalid = copy.deepcopy(reducer)
        invalid[5][0] = 'VARCHAR'
        with self.assertRaises(ValueError):
            gen_functions.validate_ai_metadata(invalid)

    def test_public_aggregate_rejects_unknown_metadata(self):
        import copy
        import functions
        import gen_functions
        gen_functions.function_list.clear()
        gen_functions.function_set.clear()
        gen_functions.function_signature_set.clear()
        for fn in functions.ai_vectorized_functions:
            gen_functions.add_ai_function(fn)
        public = copy.deepcopy(functions.ai_aggregate_functions[0])
        public[7]['instruction_argument_typo'] = 1
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)

    def test_public_aggregate_rejects_missing_or_wrong_reducer(self):
        import copy
        import functions
        import gen_functions
        gen_functions.function_list.clear()
        gen_functions.function_set.clear()
        gen_functions.function_signature_set.clear()
        public = copy.deepcopy(functions.ai_aggregate_functions[0])
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)
        for fn in functions.ai_vectorized_functions:
            gen_functions.add_ai_function(fn)
        public[7]['reducer_function_id'] = 200156
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)
        public[7]['reducer_function_id'] = 200155
        public[7]['instruction_argument'] = 0
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)

    def test_public_aggregate_rejects_explicit_provider_overload(self):
        import copy
        import functions
        import gen_functions
        gen_functions.function_list.clear()
        gen_functions.function_set.clear()
        gen_functions.function_signature_set.clear()
        reducer = copy.deepcopy(next(fn for fn in functions.ai_vectorized_functions if fn[0] == 200155))
        reducer[5].insert(1, 'VARCHAR')
        reducer[7].update(model_source='PROVIDER', provider_argument=1, input_arguments=[0, 2])
        gen_functions.add_ai_function(reducer)
        public = copy.deepcopy(functions.ai_aggregate_functions[0])
        public[5].insert(0, 'VARCHAR')
        public[7].update(model_source='PROVIDER', provider_argument=0, text_argument=1, instruction_argument=2)
        with self.assertRaises(ValueError):
            gen_functions.add_ai_aggregate_function(public)


if __name__ == '__main__':
    unittest.main()
