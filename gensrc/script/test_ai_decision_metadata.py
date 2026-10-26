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


class DecisionMetadataTest(unittest.TestCase):
    def decision(self, state='VARCHAR'):
        return [200160, 'ai_decide', True, False, 'JSON', [state, 'JSON', 'VARCHAR', 'VARCHAR'],
                'AIFunctions::ai_decide', {
                    'named_args': [{'name': 'state'}, {'name': 'questions'},
                                   {'name': 'provider', 'default': ''}, {'name': 'model', 'default': ''}],
                    'ai': functions.ai_metadata('DECISION', 'NONE', 'JSON', inputs=('state', 'questions')),
                }]

    def validate(self, declaration):
        return gen_functions.validate_ai_metadata(declaration, declaration[7]['ai'], declaration[7]['named_args'])

    def test_two_canonical_typed_signatures(self):
        declarations = [f for f in functions.vectorized_functions if f[1] == 'ai_decide']
        self.assertEqual([200160, 200161], [f[0] for f in declarations])
        self.assertEqual([['VARCHAR', 'JSON', 'VARCHAR', 'VARCHAR'], ['JSON', 'JSON', 'VARCHAR', 'VARCHAR']],
                         [f[5] for f in declarations])
        for declaration in declarations:
            metadata = self.validate(declaration)
            self.assertEqual(2, metadata['provider_argument'])
            self.assertEqual(3, metadata['model_argument'])
            self.assertEqual([0, 1], metadata['input_arguments'])
            self.assertEqual('DECISION', metadata['capability'])
            self.assertEqual('NONE', metadata['prompt_kind'])
            self.assertEqual([{'name': 'state'}, {'name': 'questions'},
                              {'name': 'provider', 'default': ''}, {'name': 'model', 'default': ''}],
                             declaration[7]['named_args'])

    def test_decision_accepts_typed_inputs_without_options(self):
        for state in ('VARCHAR', 'JSON'):
            self.validate(self.decision(state))

    def test_decision_rejects_incompatible_metadata(self):
        mutations = [('capability', 'CHAT'), ('prompt_kind', 'PASSTHROUGH'), ('result_kind', 'STRING'),
                     ('inputs', ['questions', 'state']), ('null_as_empty', ['state']), ('blank_as_null', ['state'])]
        for key, value in mutations:
            declaration = copy.deepcopy(self.decision())
            declaration[7]['ai'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate(declaration)

    def test_existing_registrations_remain_valid(self):
        for declaration in functions.vectorized_functions:
            if len(declaration) == 8 and isinstance(declaration[7], dict) and 'ai' in declaration[7]:
                self.validate(declaration)

    def test_json_input_and_options_have_distinct_runtime_roles(self):
        saved = list(gen_functions.function_list)
        saved_ids = set(gen_functions.function_set)
        saved_signatures = set(gen_functions.function_signature_set)
        gen_functions.function_list.clear()
        gen_functions.function_set.clear()
        gen_functions.function_signature_set.clear()
        try:
            decision = self.decision('JSON')
            gen_functions.add_function(decision)
            chat = next(f for f in functions.vectorized_functions if f[0] == 200001)
            gen_functions.add_function(chat)
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / 'descriptors.inc'
                gen_functions.generate_ai_cpp(str(output))
                generated = output.read_text()
                self.assertIn('AIArgumentType::JSON, AIArgumentType::JSON', generated)
                self.assertEqual(1, generated.count('AIArgumentType::OPTIONS'))
        finally:
            gen_functions.function_list[:] = saved
            gen_functions.function_set.clear()
            gen_functions.function_set.update(saved_ids)
            gen_functions.function_signature_set.clear()
            gen_functions.function_signature_set.update(saved_signatures)


if __name__ == '__main__':
    unittest.main()
