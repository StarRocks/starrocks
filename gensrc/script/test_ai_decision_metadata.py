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
import unittest

import functions
import gen_functions


class DecisionMetadataTest(unittest.TestCase):
    def decision(self, state='VARCHAR', explicit_provider=False):
        arguments = ['VARCHAR', state, 'JSON'] if explicit_provider else [state, 'JSON']
        inputs = [1, 2] if explicit_provider else [0, 1]
        return [200160, 'ai_decide', True, False, 'JSON', arguments, 'AiFunctions::ai_decide',
                functions.ai_metadata('DECISION', 'NONE', 'JSON', inputs,
                                      provider_argument=0 if explicit_provider else -1, model_source='PROVIDER')]

    def test_four_typed_provider_signatures(self):
        declarations = [f for f in functions.ai_vectorized_functions if f[1] == 'ai_decide']
        self.assertEqual([200160, 200161, 200162, 200163], [f[0] for f in declarations])
        self.assertEqual([['VARCHAR', 'JSON'], ['JSON', 'JSON'],
                          ['VARCHAR', 'VARCHAR', 'JSON'], ['VARCHAR', 'JSON', 'JSON']],
                         [f[5] for f in declarations])
        for declaration in declarations:
            metadata = gen_functions.validate_ai_metadata(declaration)
            self.assertEqual('PROVIDER', metadata['model_source'])
            self.assertEqual(-1, metadata['model_argument'])
            self.assertEqual(0 if declaration[0] >= 200162 else -1, metadata['provider_argument'])
            self.assertEqual('DECISION', metadata['capability'])
            self.assertEqual('NONE', metadata['prompt_kind'])

    def test_decision_accepts_typed_inputs_without_chat_prompt(self):
        for state in ('VARCHAR', 'JSON'):
            for explicit_provider in (False, True):
                gen_functions.validate_ai_metadata(self.decision(state, explicit_provider))

    def test_decision_rejects_incompatible_metadata(self):
        original = self.decision()
        mutations = [('capability', 'CHAT'), ('prompt_kind', 'PASSTHROUGH'),
                     ('result_kind', 'STRING'), ('model_source', 'SYSTEM'), ('model_argument', 0),
                     ('input_arguments', [1, 0]), ('null_as_empty_arguments', [0]),
                     ('blank_as_null_arguments', [0])]
        for key, value in mutations:
            declaration = copy.deepcopy(original)
            declaration[7][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                gen_functions.validate_ai_metadata(declaration)

    def test_existing_registrations_remain_valid(self):
        for declaration in functions.ai_vectorized_functions:
            gen_functions.validate_ai_metadata(declaration)

    def test_default_provider_routing_is_capability_driven(self):
        declaration = copy.deepcopy(next(f for f in functions.ai_vectorized_functions if f[0] == 200100))
        declaration[7]['model_source'] = 'PROVIDER'
        metadata = gen_functions.validate_ai_metadata(declaration)
        self.assertEqual(-1, metadata['provider_argument'])
        self.assertEqual('CHAT', metadata['capability'])
        declaration[7]['model_argument'] = 0
        with self.assertRaises(ValueError):
            gen_functions.validate_ai_metadata(declaration)


if __name__ == '__main__':
    unittest.main()
