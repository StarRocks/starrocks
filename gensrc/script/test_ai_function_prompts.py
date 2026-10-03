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

import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

import functions
import gen_functions


SCRIPT_DIR = Path(__file__).resolve().parent


class AIFunctionPromptsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.output = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.output.cleanup)
        cls.path = Path(cls.output.name)
        subprocess.run([sys.executable, str(SCRIPT_DIR / 'gen_functions.py'),
                        '--cpp', str(cls.path / 'cpp'), '--java', str(cls.path / 'java')], check=True)
        cls.java = (cls.path / 'java/com/starrocks/builtins/VectorizedBuiltinFunctions.java').read_text()
        cls.cpp = (cls.path / 'cpp/opcode/AIFunctionDescriptors.inc').read_text()

    def prompts(self):
        spec = importlib.util.find_spec('ai_function_prompts')
        self.assertIsNotNone(spec, 'Shared BE prompt templates are required')
        return __import__('ai_function_prompts')

    def test_generated_fixed_bytes_for_every_kind(self):
        expected = {'PASSTHROUGH': (0, 0), 'SENTIMENT': (187, 187), 'CLASSIFY': (289, 289),
                    'EXTRACT': (307, 307), 'FIX_GRAMMAR': (145, 145), 'REDACT': (333, 333),
                    'TRANSLATE': (136, 163), 'SIMILARITY': (225, 225), 'SUMMARIZE': (112, 112),
                    'FILTER': (143, 143)}
        for kind, (fixed, auto_detect) in expected.items():
            with self.subTest(kind=kind):
                self.assertIn(f'{kind}({fixed}, {auto_detect})', self.java)
        self.assertEqual(set(expected), set(gen_functions.AI_PROMPT_INPUT_TYPES))
        self.assertIn('public int fixedBytes()', self.java)
        self.assertIn('public int autoDetectFixedBytes()', self.java)

    def test_shared_templates_preserve_current_be_prompt_bytes(self):
        prompts = self.prompts()
        # SHA-256 of the existing BE literals before extraction, including placeholders.
        expected = {
            'SENTIMENT': 'b42b4fcb7c65d9498994c30b4ddd5c7256798979660f161289bcddc5a8a55a7d',
            'CLASSIFY': '3b0f06c77c4e6d37046616cc4bb50f85ef713dcbb8d3502d4100392091c8ea98',
            'EXTRACT': 'd6d65da983835f6af5bbb12eb77da92cdc7faa40d57751f18b6ce2672b5fe7af',
            'FIX_GRAMMAR': 'c8504d05525325685f92ca6b884e8047742ec8b9074595b69883fc9a374bbc69',
            'REDACT': '3e6532474c9b0465aaa003d6cbd664aad3660caaad34f8c2d2a246165c026756',
            'TRANSLATE': '59b37ef3637c14b3e5e0bf073470b3b29485b3f25f07b859c6afa50cdf87bc9d',
            'SIMILARITY': '0753b0d87e2a937f8e0169903e1ce85e434fa90db2fd2c86f025faea915938cf',
            'SUMMARIZE': '6015bd440478606766d61b689838692da08810cf99bf26888ec840838bfd6bb9',
            'FILTER': '7f2b2d6cec5f0df85ec0a14f2554cec379bfefda83c5445e0fc3be6bd3735ab4',
        }
        self.assertEqual(set(expected) | {'PASSTHROUGH'}, set(prompts.AI_PROMPT_TEMPLATES))
        self.assertEqual('$0', prompts.AI_PROMPT_TEMPLATES['PASSTHROUGH'])
        for kind, digest in expected.items():
            with self.subTest(kind=kind):
                self.assertEqual(digest, hashlib.sha256(prompts.AI_PROMPT_TEMPLATES[kind].encode()).hexdigest())
        self.assertEqual('36a314f5b721062e924a13ac57aae7757b579a7b1479780f2795cc940afb49f7',
                         hashlib.sha256(prompts.AI_TRANSLATE_AUTO_DETECT_TEMPLATE.encode()).hexdigest())

    def test_generated_be_literals_match_shared_templates(self):
        prompts = self.prompts()
        for kind, template in prompts.AI_PROMPT_TEMPLATES.items():
            if kind == 'PASSTHROUGH':
                continue
            name = 'kAI' + kind.title().replace('_', '') + 'Prompt'
            match = re.search(r'static constexpr char ' + name + r'\[\] = (".*");', self.cpp)
            self.assertIsNotNone(match, name)
            self.assertEqual(template, json.loads(match[1]))
        self.assertIn(json.dumps(prompts.AI_TRANSLATE_AUTO_DETECT_TEMPLATE), self.cpp)

    def test_placeholder_validation_and_utf8_fixed_bytes(self):
        prompts = self.prompts()
        self.assertEqual(4, prompts.prompt_fixed_bytes('你 $0', 1))
        self.assertEqual(0, prompts.prompt_fixed_bytes('$1$0', 2))
        for template in ('$0 $0', '$1', '$0 $2', '$0 $', '$0 $x', '$0 $$', '$10'):
            with self.subTest(template=template), self.assertRaises(ValueError):
                prompts.prompt_fixed_bytes(template, 1)
        self.assertEqual(163, prompts.prompt_fixed_bytes(prompts.AI_TRANSLATE_AUTO_DETECT_TEMPLATE, 2))
        with self.assertRaises(ValueError):
            prompts.prompt_fixed_bytes(prompts.AI_TRANSLATE_AUTO_DETECT_TEMPLATE, 3)

    def test_be_uses_generated_literals_with_existing_substitution_order(self):
        source = (SCRIPT_DIR.parents[1] / 'be/src/exprs/ai/ai_function_call_expr.cpp').read_text()
        body = source.split('std::string build_prompt(', 1)[1].split('} // namespace', 1)[0]
        calls = re.findall(r'strings::Substitute\((.*?)\)', body, re.S)
        self.assertEqual([
            'kAISentimentPrompt, text', 'kAIClassifyPrompt, second, text',
            'kAIExtractPrompt, second, text', 'kAIFixGrammarPrompt, text',
            'kAIRedactPrompt, second, text', 'kAITranslateAutoDetectPrompt, third, text',
            'kAITranslatePrompt, second, third, text', 'kAISimilarityPrompt, text, second',
            'kAISummarizePrompt, text', 'kAIFilterPrompt, text, second',
        ], [' '.join(call.split()) for call in calls])
        self.assertIn('if (second.empty())', body)

    def test_template_validation_rejects_missing_or_extra_kinds(self):
        prompts = self.prompts()
        prompts.validate_ai_prompt_templates()
        for kind in ('SENTIMENT', 'UNKNOWN'):
            templates = dict(prompts.AI_PROMPT_TEMPLATES)
            if kind in templates:
                del templates[kind]
            else:
                templates[kind] = '$0'
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                prompts.validate_ai_prompt_templates(templates)

    def test_fe_argument_policies_come_from_existing_metadata(self):
        self.assertIn('List<Integer> nullAsEmptyArguments', self.java)
        self.assertIn('List<Integer> blankAsNullArguments', self.java)
        self.assertIn('nullAsEmptyArguments = List.copyOf(nullAsEmptyArguments)', self.java)
        self.assertIn('blankAsNullArguments = List.copyOf(blankAsNullArguments)', self.java)
        for function in functions.ai_vectorized_functions:
            metadata = function[-1]
            lists = [', '.join(map(str, metadata[key]))
                     for key in ('input_arguments', 'null_as_empty_arguments', 'blank_as_null_arguments')]
            expected = f'AIPromptKind.{metadata["prompt_kind"]}, ' + ', '.join(f'List.of({v})' for v in lists)
            descriptor = re.search(r'\.put\(' + str(function[0]) + r'L,.*?\)\)\)', self.java, re.S)
            self.assertIsNotNone(descriptor)
            self.assertIn(expected, descriptor[0])

    def test_ordinary_builtin_generation_is_independent_of_ai_metadata(self):
        for field, empty in (('function_list', []), ('function_set', set()), ('function_signature_set', set())):
            self.addCleanup(setattr, gen_functions, field, getattr(gen_functions, field))
            setattr(gen_functions, field, empty)
        for function in functions.vectorized_functions:
            gen_functions.add_function(function)
        ordinary = self.path / 'ordinary'
        ordinary.mkdir(exist_ok=True)
        gen_functions.generate_cpp(str(ordinary) + '/')
        for path in (self.path / 'cpp/opcode').glob('*.inc'):
            if path.name != 'AIFunctionDescriptors.inc':
                self.assertEqual(path.read_bytes(), (ordinary / path.name).read_bytes(), path.name)
        gen_functions.generate_fe(ordinary / 'Functions.java')
        full_builtins = self.java.split('public static void initBuiltins', 1)[1].splitlines()
        ordinary_builtins = (ordinary / 'Functions.java').read_text().split('public static void initBuiltins', 1)[1]
        self.assertEqual(ordinary_builtins.splitlines(),
                         [line for line in full_builtins if 'addVectorizedAIScalarBuiltin' not in line])


if __name__ == '__main__':
    unittest.main()
