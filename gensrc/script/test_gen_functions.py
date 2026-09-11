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

import copy
import pathlib
import tempfile
import unittest

import functions
import gen_functions


class AIFunctionGenerationTest(unittest.TestCase):
    def setUp(self):
        gen_functions.function_list.clear()
        gen_functions.function_set.clear()
        gen_functions.function_signature_set.clear()

    def register(self):
        for row in functions.vectorized_functions:
            gen_functions.add_function(row)
        for row in functions.ai_vectorized_functions:
            gen_functions.add_ai_function(row)

    def generate(self, path):
        gen_functions.generate_fe(str(path / 'VectorizedBuiltinFunctions.java'))
        gen_functions.generate_cpp(str(path) + '/')
        return {file.name: file.read_text() for file in path.iterdir()}

    def test_existing_ids_and_semantic_positions(self):
        self.register()
        entries = {fn['id']: fn for fn in gen_functions.function_list if fn.get('binary_type') == 'AI'}
        expected_ids = set(range(200100, 200104)) | set(range(200110, 200128))
        expected_ids |= set(range(200130, 200134)) | set(range(200140, 200144))
        self.assertEqual(expected_ids, entries.keys())
        explicit_ids = {200102, 200103, 200111, 200113, 200115, 200117, 200119, 200121,
                        200123, 200125, 200127, 200132, 200133}
        self.assertEqual(explicit_ids, {fid for fid, fn in entries.items() if fn['ai']['model_argument'] >= 0})
        for fid in range(200140, 200144):
            metadata = entries[fid]['ai']
            self.assertEqual('AI_MODEL', metadata['model_source'])
            self.assertEqual(0, metadata['ai_model_argument'])
            self.assertNotIn('resource_argument', metadata)
            self.assertEqual([1], metadata['input_arguments'])
        for fid, source, target in ((200120, 1, 2), (200121, 2, 3)):
            metadata = entries[fid]['ai']
            self.assertEqual([source], metadata['null_as_empty_arguments'])
            self.assertEqual([target], metadata['empty_as_null_arguments'])
        self.assertEqual('TEXT_EMBEDDING', entries[200130]['ai']['capability'])
        self.assertEqual('CHAT', entries[200122]['ai']['capability'])
        self.assertEqual('SIMILARITY', entries[200122]['ai']['result_kind'])

    def test_generated_descriptors_and_dispatch_are_consistent(self):
        self.register()
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            outputs = self.generate(path)
            self.assertEqual(outputs, self.generate(path), 'Generation must be deterministic')
        java = outputs['VectorizedBuiltinFunctions.java']
        cpp = outputs['AIFunctionDescriptors.inc']
        for row in functions.ai_vectorized_functions:
            fid, name = row[:2]
            self.assertIn(f'addVectorizedAIScalarBuiltin({fid}, "{name}"', java)
            self.assertIn(f'.put({fid}L, new AIFunctionDescriptor(', java)
            self.assertIn(f'.fid = {fid},', cpp)
            self.assertNotIn(f'uint64_t>({fid})', outputs['AiFunctions.inc'])
        self.assertIn('uint64_t>(200000)', outputs['AiFunctions.inc'])
        self.assertIn('ai_query', outputs['AiFunctions.inc'])
        self.assertIn('std::array<AIArgumentType, 4>', cpp)
        self.assertIn('std::array<int, 3>', cpp)
        self.assertTrue('int aiModelArgument)' in java, 'FE descriptor must name the AI model selector explicitly')
        self.assertNotIn('resourceArgument', java)
        self.assertIn('.model_source = TAIModelSource::AI_MODEL,', cpp)
        self.assertIn('.ai_model_argument = 0,', cpp)
        self.assertNotIn('resource_argument', cpp)
        self.assertNotIn('TAIModelSource::RESOURCE', cpp)

    def test_nonconsecutive_fid_has_declared_semantics(self):
        row = copy.deepcopy(next(row for row in functions.ai_vectorized_functions if row[0] == 200121))
        row[0], row[1] = 900007, 'future_translate'
        gen_functions.add_ai_function(row)
        with tempfile.TemporaryDirectory() as directory:
            outputs = self.generate(pathlib.Path(directory))
        java = outputs['VectorizedBuiltinFunctions.java']
        cpp = outputs['AIFunctionDescriptors.inc']
        self.assertIn('.put(900007L, new AIFunctionDescriptor(', java)
        self.assertIn('.fid = 900007,', cpp)
        self.assertIn('.prompt_kind = AIPromptKind::TRANSLATE,', cpp)
        self.assertIn('.model_argument = 0,', cpp)
        self.assertIn('.input_arguments = {1, 2, 3},', cpp)
        self.assertIn('.null_as_empty_arguments = {false, false, true, false},', cpp)

    def test_invalid_semantics_are_rejected_before_registration(self):
        prototype = next(row for row in functions.ai_vectorized_functions if row[0] == 200121)
        invalid = [
            ('capability', 'UNKNOWN'), ('result_kind', 'EMBEDDING'), ('prompt_kind', 'UNKNOWN'),
            ('model_source', 'UNKNOWN'), ('model_argument', 4), ('ai_model_argument', 0),
            ('input_arguments', [1, 2]), ('input_arguments', [1, 1, 3]),
            ('null_as_empty_arguments', [9]), ('empty_as_null_arguments', [0]),
            ('options_argument', 3),
        ]
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                row = copy.deepcopy(prototype)
                row[7][field] = value
                with self.assertRaises(ValueError):
                    gen_functions.add_ai_function(row)
                self.assertEqual([], gen_functions.function_list)
        for field in ('capability', 'result_kind', 'prompt_kind', 'input_arguments'):
            with self.subTest(missing=field):
                row = copy.deepcopy(prototype)
                del row[7][field]
                with self.assertRaises(ValueError):
                    gen_functions.add_ai_function(row)
                self.assertEqual([], gen_functions.function_list)

    def test_legacy_resource_source_is_not_registered(self):
        row = copy.deepcopy(next(row for row in functions.ai_vectorized_functions if row[0] == 200140))
        row[7]['model_source'] = 'RESOURCE'
        with self.assertRaisesRegex(ValueError, 'unsupported model source'):
            gen_functions.add_ai_function(row)
        self.assertEqual([], gen_functions.function_list)

    def test_ai_model_selector_cannot_override_the_provider_model(self):
        prototype = next(row for row in functions.ai_vectorized_functions if row[0] == 200140)
        invalid = [
            {'model_source': 'SYSTEM'},
            {'ai_model_argument': -1},
            {'model_argument': 0},
            {'resource_argument': 0},
        ]
        for changes in invalid:
            with self.subTest(changes=changes):
                row = copy.deepcopy(prototype)
                row[7].update(changes)
                with self.assertRaises(ValueError):
                    gen_functions.add_ai_function(row)
                self.assertEqual([], gen_functions.function_list)

    def test_options_must_follow_the_existing_sql_signature_convention(self):
        row = copy.deepcopy(next(row for row in functions.ai_vectorized_functions if row[0] == 200101))
        row[5] = ['ANY_MAP', 'VARCHAR']
        row[7]['input_arguments'] = [1]
        with self.assertRaisesRegex(ValueError, 'options.*last'):
            gen_functions.add_ai_function(row)
        self.assertEqual([], gen_functions.function_list)


if __name__ == '__main__':
    unittest.main()
