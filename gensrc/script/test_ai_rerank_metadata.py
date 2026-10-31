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
import tempfile
import unittest
from pathlib import Path

import functions
import gen_functions


class RerankMetadataTest(unittest.TestCase):
    def test_single_canonical_signature_and_unique_id(self):
        declarations = [f for f in functions.vectorized_functions if f[1] == 'ai_rerank']
        self.assertEqual([200170], [f[0] for f in declarations])
        self.assertEqual(1, sum(f[0] == 200170 for f in functions.vectorized_functions))
        self.assertEqual(['VARCHAR', 'ARRAY_VARCHAR', 'VARCHAR', 'VARCHAR', 'JSON'], declarations[0][5])

    def declaration(self):
        return [200170, 'ai_rerank', True, False,
                ('ARRAY', ('STRUCT', (('index', 'INT'), ('relevance_score', 'DOUBLE')))),
                ['VARCHAR', 'ARRAY_VARCHAR', 'VARCHAR', 'VARCHAR', 'JSON'], 'nullptr', {
                    'named_args': [{'name': 'query'}, {'name': 'documents'},
                                   {'name': 'provider', 'default': ''}, {'name': 'model', 'default': ''},
                                   {'name': 'options', 'default': {}}],
                    'ai': functions.ai_metadata('RERANK', 'NONE', 'RERANK', inputs=('query', 'documents')),
                }]

    def validate(self, declaration):
        return gen_functions.validate_ai_metadata(declaration, declaration[-1]['ai'], declaration[-1]['named_args'])

    def test_typed_contract_and_defaults(self):
        declaration = self.declaration()
        metadata = self.validate(declaration)
        self.assertEqual([0, 1], metadata['input_arguments'])
        self.assertEqual(2, metadata['provider_argument'])
        self.assertEqual(3, metadata['model_argument'])
        self.assertEqual([], metadata['null_as_empty_arguments'])
        self.assertEqual([], metadata['blank_as_null_arguments'])
        self.assertEqual(declaration, next(f for f in functions.vectorized_functions if f[1] == 'ai_rerank'))

    def test_incompatible_metadata_rejected(self):
        for key, value in [('capability', 'CHAT'), ('prompt_kind', 'PASSTHROUGH'), ('result_kind', 'JSON'),
                           ('inputs', ('documents', 'query')), ('null_as_empty', ('query',)),
                           ('blank_as_null', ('query',))]:
            declaration = copy.deepcopy(self.declaration())
            declaration[-1]['ai'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate(declaration)
        declaration = self.declaration()
        declaration[5][1] = 'FILE'
        with self.assertRaises(ValueError):
            self.validate(declaration)

    def test_existing_metadata_remains_valid(self):
        for declaration in functions.vectorized_functions:
            if isinstance(declaration[-1], dict) and 'ai' in declaration[-1]:
                self.validate(declaration)

    def test_fixed_complex_types_are_generic_fresh_java_expressions(self):
        expected = ('new ArrayType(new StructType(Lists.newArrayList('
                    'new StructField("index", INT), new StructField("relevance_score", DOUBLE)), true))')
        self.assertEqual(expected, gen_functions.java_type(self.declaration()[4]))
        self.assertEqual('INT', gen_functions.java_type('INT'))
        self.assertEqual('new ArrayType(new ArrayType(VARCHAR))',
                         gen_functions.java_type(('ARRAY', ('ARRAY', 'VARCHAR'))))
        for malformed in [('ARRAY',), ('STRUCT', ()), ('STRUCT', (('x', 'INT'), ('x', 'DOUBLE'))),
                          ('STRUCT', (('bad-name', 'INT'),)), ('MAP', 'INT', 'VARCHAR')]:
            with self.subTest(malformed=malformed), self.assertRaises(ValueError):
                gen_functions.java_type(malformed)

    def test_both_registration_templates_and_cpp_descriptor(self):
        saved = (list(gen_functions.function_list), set(gen_functions.function_set),
                 set(gen_functions.function_signature_set))
        gen_functions.function_list.clear()
        gen_functions.function_set.clear()
        gen_functions.function_signature_set.clear()
        try:
            declaration = self.declaration()
            gen_functions.add_function(declaration)
            gen_functions.add_function([200171, 'ordinary_complex', True, False, declaration[4], [], 'nullptr'])
            with tempfile.TemporaryDirectory() as directory:
                java = Path(directory) / 'functions.java'
                cpp = Path(directory) / 'descriptors.inc'
                gen_functions.generate_fe(str(java))
                gen_functions.generate_ai_cpp(str(cpp))
                self.assertEqual(2, java.read_text().count(gen_functions.java_type(declaration[4])))
                self.assertIn('AICapability.RERANK, 3, 2', java.read_text())
                self.assertIn('AIFunctionResultKind::RERANK', cpp.read_text())
                self.assertIn('AIArgumentType::VARCHAR, AIArgumentType::STRING_ARRAY', cpp.read_text())
        finally:
            gen_functions.function_list[:] = saved[0]
            gen_functions.function_set.clear()
            gen_functions.function_set.update(saved[1])
            gen_functions.function_signature_set.clear()
            gen_functions.function_signature_set.update(saved[2])


if __name__ == '__main__':
    unittest.main()
