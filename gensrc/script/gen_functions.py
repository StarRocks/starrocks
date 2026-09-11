#!/usr/bin/env python
# encoding: utf-8

"""
  Copyright 2021-present StarRocks, Inc. All rights reserved.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""

import argparse
import os
import sys

from string import Template

import functions

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

license_string = """
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This is a generated file, DO NOT EDIT.
// To add new functions, see the generator at
// gensrc/script/gen_functions.py or the function list at
// gensrc/script/functions.py.
"""

java_template = Template(
    """
${license}

package com.starrocks.builtins;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.common.Pair;
import com.starrocks.thrift.TAIModelSource;
import com.starrocks.type.Type;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import static com.starrocks.type.AnyArrayType.ANY_ARRAY;
import static com.starrocks.type.AnyElementType.ANY_ELEMENT;
import static com.starrocks.type.AnyMapType.ANY_MAP;
import static com.starrocks.type.AnyStructType.ANY_STRUCT;
import static com.starrocks.type.ArrayType.ARRAY_BIGINT;
import static com.starrocks.type.ArrayType.ARRAY_BOOLEAN;
import static com.starrocks.type.ArrayType.ARRAY_DATE;
import static com.starrocks.type.ArrayType.ARRAY_DATETIME;
import static com.starrocks.type.ArrayType.ARRAY_DECIMAL128;
import static com.starrocks.type.ArrayType.ARRAY_DECIMAL32;
import static com.starrocks.type.ArrayType.ARRAY_DECIMAL64;
import static com.starrocks.type.ArrayType.ARRAY_DECIMALV2;
import static com.starrocks.type.ArrayType.ARRAY_DOUBLE;
import static com.starrocks.type.ArrayType.ARRAY_FLOAT;
import static com.starrocks.type.ArrayType.ARRAY_INT;
import static com.starrocks.type.ArrayType.ARRAY_JSON;
import static com.starrocks.type.ArrayType.ARRAY_LARGEINT;
import static com.starrocks.type.ArrayType.ARRAY_SMALLINT;
import static com.starrocks.type.ArrayType.ARRAY_TINYINT;
import static com.starrocks.type.ArrayType.ARRAY_VARCHAR;
import static com.starrocks.type.BitmapType.BITMAP;
import static com.starrocks.type.BooleanType.BOOLEAN;
import static com.starrocks.type.DateType.DATE;
import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.DateType.TIME;
import static com.starrocks.type.DecimalType.DECIMAL128;
import static com.starrocks.type.DecimalType.DECIMAL256;
import static com.starrocks.type.DecimalType.DECIMAL32;
import static com.starrocks.type.DecimalType.DECIMAL64;
import static com.starrocks.type.DecimalType.DECIMALV2;
import static com.starrocks.type.FloatType.DOUBLE;
import static com.starrocks.type.FloatType.FLOAT;
import static com.starrocks.type.FunctionType.FUNCTION;
import static com.starrocks.type.HLLType.HLL;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.IntegerType.LARGEINT;
import static com.starrocks.type.IntegerType.SMALLINT;
import static com.starrocks.type.IntegerType.TINYINT;
import static com.starrocks.type.JsonType.JSON;
import static com.starrocks.type.MapType.MAP_VARCHAR_VARCHAR;
import static com.starrocks.type.PercentileType.PERCENTILE;
import static com.starrocks.type.VarbinaryType.VARBINARY;
import static com.starrocks.type.VarcharType.VARCHAR;
import static com.starrocks.type.VariantType.VARIANT;

public class VectorizedBuiltinFunctions {
    public static final Set<String> AI_FUNCTION_NAMES = ImmutableSet.of(${ai_function_names});

    public enum AICapability {
        CHAT, TEXT_EMBEDDING
    }

    public record AIFunctionDescriptor(AICapability capability, int modelArgument, int aiModelArgument) {
    }

    private static final Map<Long, AIFunctionDescriptor> AI_FUNCTION_DESCRIPTORS =
            ImmutableMap.<Long, AIFunctionDescriptor>builder()
${ai_descriptors}
                    .build();

    public static AIFunctionDescriptor getAIFunctionDescriptor(long functionId) {
        return AI_FUNCTION_DESCRIPTORS.get(functionId);
    }

    public static void initBuiltins(FunctionSet functionSet) {
        ${functions}
  }
}

"""
)

cpp_template = """
#include "exprs/builtin_functions.h"
namespace starrocks {{
void __attribute__((constructor)) {module}_initialize() {{
{content}
}}
}}
"""

function_list = list()
function_set = set()
function_signature_set = set()
FE_HIDDEN_FUNCTIONS = {'dict_encode'}

def add_function(fn_data):
    entry = dict()
    if fn_data[0] in function_set:
        print("=================================================================")
        print("Duplicated function id: " + str(fn_data))
        print("=================================================================")
        exit(1)
    function_set.add(fn_data[0])

    entry["id"] = fn_data[0]
    entry["name"] = fn_data[1]
    entry["exception_safe"] = str(fn_data[2]).lower()
    entry["check_overflow"] = str(fn_data[3]).lower()
    entry["ret"] = fn_data[4]
    entry["args"] = fn_data[5]

    function_signature = "%s#%s#(%s)" % (
        entry["ret"],
        entry["name"],
        ", ".join(entry["args"]),
    )

    if function_signature in function_signature_set:
        print("=================================================================")
        print("Duplicated function signature: " + function_signature)
        print("=================================================================")
        exit(1)
    function_signature_set.add(function_signature)

    if "..." in fn_data[5]:
        assert 2 <= len(fn_data[5]), "Invalid arguments in functions.py:\n\t" + repr(
            fn_data
        )
        assert (
            "..." == fn_data[5][-1]
        ), "variadic parameter must at the end:\n\t" + repr(fn_data)

        entry["args_nums"] = len(fn_data[5]) - 1
    else:
        entry["args_nums"] = len(fn_data[5])

    entry["fn"] = "&" + fn_data[6] if fn_data[6] != "nullptr" else "nullptr"

    if len(fn_data) >= 9:
        entry["prepare"] = "&" + fn_data[7] if fn_data[7] != "nullptr" else "nullptr"
        entry["close"] = "&" + fn_data[8] if fn_data[8] != "nullptr" else "nullptr"

    # Named Arguments metadata: check if last element is a dict with 'named_args' key
    entry["named_args"] = []
    if fn_data and isinstance(fn_data[-1], dict) and 'named_args' in fn_data[-1]:
        entry["named_args"] = fn_data[-1]['named_args']

    function_list.append(entry)
    return entry


# These are the input/output contracts of the BE prompt builders and result decoders,
# not lists of SQL function names or FIDs. Overloads bind argument positions in functions.py.
AI_PROMPT_INPUT_TYPES = {
    'PASSTHROUGH': ['VARCHAR'],
    'SENTIMENT': ['VARCHAR'],
    'CLASSIFY': ['VARCHAR', 'ARRAY_VARCHAR'],
    'EXTRACT': ['VARCHAR', 'ARRAY_VARCHAR'],
    'FIX_GRAMMAR': ['VARCHAR'],
    'REDACT': ['VARCHAR', 'ARRAY_VARCHAR'],
    'TRANSLATE': ['VARCHAR', 'VARCHAR', 'VARCHAR'],
    'SIMILARITY': ['VARCHAR', 'VARCHAR'],
    'SUMMARIZE': ['VARCHAR'],
    'FILTER': ['VARCHAR', 'VARCHAR'],
}
AI_RESULT_TYPES = {
    'STRING': 'VARCHAR', 'SENTIMENT': 'VARCHAR', 'JSON': 'JSON',
    'SIMILARITY': 'FLOAT', 'BOOLEAN': 'BOOLEAN', 'EMBEDDING': 'ARRAY_FLOAT',
}
AI_ARGUMENT_TYPES = {'VARCHAR': 'VARCHAR', 'ANY_MAP': 'OPTIONS', 'ARRAY_VARCHAR': 'STRING_ARRAY'}


def validate_ai_metadata(fn_data):
    def invalid(message):
        raise ValueError(f"Invalid AI function {fn_data[0]}: {message}")

    if len(fn_data) != 8 or not isinstance(fn_data[7], dict):
        invalid('semantic metadata is required')
    metadata = dict(fn_data[7])
    required = {'model_source', 'capability', 'prompt_kind', 'result_kind', 'input_arguments'}
    optional = {'model_argument', 'ai_model_argument', 'null_as_empty_arguments', 'empty_as_null_arguments'}
    if not required <= metadata.keys() or metadata.keys() - required - optional:
        invalid('missing or unknown semantic metadata fields')
    if metadata['model_source'] not in ('SYSTEM', 'AI_MODEL'):
        invalid('unsupported model source')
    if metadata['capability'] not in ('CHAT', 'TEXT_EMBEDDING'):
        invalid('unsupported capability')
    if metadata['prompt_kind'] not in AI_PROMPT_INPUT_TYPES:
        invalid('unsupported prompt kind')
    if AI_RESULT_TYPES.get(metadata['result_kind']) != fn_data[4]:
        invalid('result decoder must match SQL return type')
    if (metadata['capability'] == 'TEXT_EMBEDDING') != (metadata['result_kind'] == 'EMBEDDING'):
        invalid('embedding capability and result decoder must agree')
    if metadata['capability'] == 'TEXT_EMBEDDING' and metadata['prompt_kind'] != 'PASSTHROUGH':
        invalid('embedding input must not use a chat prompt builder')

    args = fn_data[5]
    if any(arg not in AI_ARGUMENT_TYPES for arg in args):
        invalid('unsupported AI argument type')
    options = [index for index, arg in enumerate(args) if arg == 'ANY_MAP']
    if len(options) > 1:
        invalid('only one options map is supported')
    # Match the SQL analyzer's optional trailing-options contract.
    if options and options[0] != len(args) - 1:
        invalid('options must be the last SQL argument')
    selectors = []
    for field in ('model_argument', 'ai_model_argument'):
        index = metadata.setdefault(field, -1)
        if type(index) is not int or index < -1 or index >= len(args):
            invalid(f'{field} is out of range')
        if index >= 0:
            if args[index] != 'VARCHAR':
                invalid(f'{field} must refer to a VARCHAR')
            selectors.append(index)
    if (metadata['model_source'] == 'AI_MODEL') != (metadata['ai_model_argument'] >= 0):
        invalid('AI model source must have an AI model selector')
    if metadata['ai_model_argument'] >= 0 and metadata['model_argument'] >= 0:
        invalid('AI model selector is not an explicit provider model')

    inputs = metadata['input_arguments']
    if not isinstance(inputs, list) or any(type(i) is not int or i < 0 or i >= len(args) for i in inputs):
        invalid('invalid input argument positions')
    if [args[i] for i in inputs] != AI_PROMPT_INPUT_TYPES[metadata['prompt_kind']]:
        invalid('input arguments must match the prompt builder contract')
    roles = selectors + options + inputs
    if len(roles) != len(args) or sorted(roles) != list(range(len(args))):
        invalid('every SQL argument must have exactly one semantic role')
    for field in ('null_as_empty_arguments', 'empty_as_null_arguments'):
        indices = metadata.setdefault(field, [])
        if not isinstance(indices, list) or any(type(i) is not int or i not in inputs or args[i] != 'VARCHAR'
                                                for i in indices):
            invalid(f'{field} must refer to VARCHAR input arguments')
        if len(set(indices)) != len(indices):
            invalid(f'duplicate positions in {field}')
    if set(metadata['null_as_empty_arguments']) & set(metadata['empty_as_null_arguments']):
        invalid('conflicting NULL and empty policies')
    return metadata


def add_ai_function(fn_data):
    metadata = validate_ai_metadata(fn_data)

    entry = add_function(fn_data[:7])
    entry["binary_type"] = "AI"
    entry["model_source"] = metadata['model_source']
    entry['ai'] = metadata


def generate_default_value(param, fn_id):
    """Convert Python value to Java Expr code for Named Arguments default values"""
    default = param.get('default')
    name = param['name']

    if 'default' not in param:
        return None  # required parameter, no default value
    elif isinstance(default, bool):
        return f'            defaults{fn_id}.add(new Pair<>("{name}", new BoolLiteral({str(default).lower()})));'
    elif isinstance(default, int):
        return f'            defaults{fn_id}.add(new Pair<>("{name}", new IntLiteral({default})));'
    elif isinstance(default, str):
        # Escape all special characters for Java string literals
        escaped = (default
            .replace('\\', '\\\\')  # backslash first
            .replace('"', '\\"')    # double quote
            .replace('\n', '\\n')   # newline
            .replace('\r', '\\r')   # carriage return
            .replace('\t', '\\t'))  # tab
        return f'            defaults{fn_id}.add(new Pair<>("{name}", new StringLiteral("{escaped}")));'
    else:
        print(f"WARNING: Unsupported default value type '{type(default).__name__}' "
              f"for parameter '{name}' in function {fn_id}. "
              f"Parameter will be treated as required.")
        return None


def generate_fe(path):
    fn_template = Template(
        'functionSet.addVectorizedScalarBuiltin(${id}, "${name}", ${has_vargs}, ${ret}${args_types});'
    )
    ai_fn_template = Template(
        'functionSet.addVectorizedAIScalarBuiltin(${id}, "${name}", ${has_vargs}, '
        'TAIModelSource.${model_source}, ${ret}${args_types});'
    )

    fn_named_template = Template('''{
            List<Type> argTypes${id} = Lists.newArrayList(${args_types_list});
            Function fn${id} = ScalarFunction.createVectorizedBuiltin(${id}L, "${name}", argTypes${id}, ${has_vargs}, ${ret});
            fn${id}.setArgNames(Lists.newArrayList(${arg_names}));
            Vector<Pair<String, Expr>> defaults${id} = new Vector<>();
${default_values}
            fn${id}.setDefaultNamedArgs(defaults${id});
            functionSet.addBuiltin(fn${id});
        }''')

    def gen_fe_fn(fnm):
        fnm["args_types"] = ", " if len(fnm["args"]) > 0 else ""
        fnm["args_types"] = fnm["args_types"] + ", ".join(
            [i for i in fnm["args"] if i != "..."]
        )
        fnm["has_vargs"] = "true" if "..." in fnm["args"] else "false"

        if fnm.get("binary_type") == "AI":
            return ai_fn_template.substitute(fnm)

        # Check if function has named arguments
        if fnm.get("named_args"):
            named_args = fnm["named_args"]
            arg_names = ', '.join([f'"{p["name"]}"' for p in named_args])
            default_lines = [generate_default_value(p, fnm["id"]) for p in named_args]
            default_values = '\n'.join([d for d in default_lines if d])
            # List of argument types for List<Type> constructor
            args_types_list = ", ".join([i for i in fnm["args"] if i != "..."])

            return fn_named_template.substitute(
                id=fnm["id"],
                name=fnm["name"],
                has_vargs=fnm["has_vargs"],
                ret=fnm["ret"],
                args_types_list=args_types_list,
                arg_names=arg_names,
                default_values=default_values
            )
        else:
            return fn_template.substitute(fnm)

    value = dict()
    value["license"] = license_string
    value["functions"] = "\n        ".join([gen_fe_fn(i) for i in function_list if i['name'] not in FE_HIDDEN_FUNCTIONS])
    ai_function_names = sorted({fn["name"] for fn in function_list if fn.get("binary_type") == "AI"})
    value["ai_function_names"] = ", ".join('"%s"' % name for name in ai_function_names)
    value['ai_descriptors'] = '\n'.join(
        '                    .put(%dL, new AIFunctionDescriptor(AICapability.%s, %d, %d))' % (
            fn['id'], fn['ai']['capability'], fn['ai']['model_argument'], fn['ai']['ai_model_argument'])
        for fn in function_list if fn.get('binary_type') == 'AI')

    content = java_template.substitute(value)

    with open(path, mode="w+") as f:
        f.write(content)


def generate_ai_cpp(path):
    ai_functions = [fn for fn in function_list if fn.get('binary_type') == 'AI']
    max_arguments = max((len(fn['args']) for fn in ai_functions), default=0)
    max_inputs = max((len(fn['ai']['input_arguments']) for fn in ai_functions), default=0)
    content = license_string + '''
// Included inside namespace starrocks, after the prompt/argument kind declarations.
struct AIFunctionDescriptor {
    int64_t fid;
    const char* name;
    TAIModelSource::type model_source;
    AICapability capability;
    AIFunctionResultKind result_kind;
    AIPromptKind prompt_kind;
    size_t argument_count;
    std::array<AIArgumentType, %d> argument_types;
    int model_argument;
    int ai_model_argument;
    std::array<int, %d> input_arguments;
    size_t input_count;
    std::array<bool, %d> null_as_empty_arguments;
    std::array<bool, %d> empty_as_null_arguments;
};

static constexpr std::array<AIFunctionDescriptor, %d> kAIFunctionDescriptors = {{
''' % (max_arguments, max_inputs, max_arguments, max_arguments, len(ai_functions))
    for fn in ai_functions:
        metadata = fn['ai']
        arg_types = ['AIArgumentType::' + AI_ARGUMENT_TYPES[arg] for arg in fn['args']]
        arg_types += ['AIArgumentType::VARCHAR'] * (max_arguments - len(arg_types))
        inputs = metadata['input_arguments'] + [-1] * (max_inputs - len(metadata['input_arguments']))
        fields = [
            ('fid', str(fn['id'])), ('name', '"' + fn['name'] + '"'),
            ('model_source', 'TAIModelSource::' + metadata['model_source']),
            ('capability', 'AICapability::' + metadata['capability']),
            ('result_kind', 'AIFunctionResultKind::' + metadata['result_kind']),
            ('prompt_kind', 'AIPromptKind::' + metadata['prompt_kind']),
            ('argument_count', str(len(fn['args']))),
            ('argument_types', '{' + ', '.join(arg_types) + '}'),
            ('model_argument', str(metadata['model_argument'])),
            ('ai_model_argument', str(metadata['ai_model_argument'])),
            ('input_arguments', '{' + ', '.join(str(i) for i in inputs) + '}'),
            ('input_count', str(len(metadata['input_arguments']))),
        ]
        for policy in ('null_as_empty_arguments', 'empty_as_null_arguments'):
            flags = (str(i in metadata[policy]).lower() for i in range(max_arguments))
            fields.append((policy, '{' + ', '.join(flags) + '}'))
        content += '    {\n' + ''.join('        .%s = %s,\n' % field for field in fields) + '    },\n'
    content += '}};\n'
    with open(path, mode='w+') as output:
        output.write(content)


def generate_cpp(path):
    def gen_be_fn(fnm):
        res = ""
        if "prepare" in fnm:
            res = '{%d, {"%s", %d, %s, %s, %s, %s, %s, "%s", {%s} }}' % (
                fnm["id"],
                fnm["name"],
                fnm["args_nums"],
                fnm["fn"],
                fnm["prepare"],
                fnm["close"],
                fnm["exception_safe"],
                fnm["check_overflow"],
                fnm['ret'], 
                ", ".join(['"%s"' % arg for arg in fnm['args']]),
            )
        else:
            res = '{%d, {"%s", %d, %s, %s, %s, "%s", {%s} }}' % (
                fnm["id"],
                fnm["name"],
                fnm["args_nums"],
                fnm["fn"],
                fnm["exception_safe"],
                fnm["check_overflow"],
                fnm['ret'], 
                ", ".join(['"%s"' % arg for arg in fnm['args']]),
            )

        return res

    value = dict()
    value["license"] = license_string
    builtin_functions = [fn for fn in function_list if fn.get("binary_type") != "AI"]
    value["functions"] = ", \n        ".join([gen_be_fn(i) for i in builtin_functions])

    modules = [
        "MathFunctions",
        "StringFunctions",
        "LikePredicate",
        "BinaryFunctions",
        "BitFunctions",
        "TimeFunctions",
        "ConditionFunctions",
        "HyperloglogFunctions",
        "BitmapFunctions",
        "DsThetaFunctions",
        "HashFunctions",
        "GroupingSetsFunctions",
        "StructFunctions",
        "UtilityFunctions",
        "JsonFunctions",
        "VariantFunctions",
        "EncryptionFunctions",
        "ESFunctions",
        "GeoFunctions",
        "PercentileFunctions",
        "ArrayFunctions",
        "MapFunctions",
        "GinFunctions",
        "AiFunctions",
        "HttpRequestFunctions",
        "DictFunctions",
    ]

    modules_contents = dict()
    for module in modules:
        modules_contents[module] = ""

    for fnm in builtin_functions:
        target = "Unknown"
        if fnm["fn"] == "nullptr":
            continue
        for module in modules:
            if module in fnm["fn"]:
                target = module
                break
        if target == "Unknown":
            print("fnm:" + fnm["fn"] + str(fnm))

        if "prepare" in fnm:
            modules_contents[target] = modules_contents[
                target
            ] + '\tBuiltinFunctions::emplace_builtin_function(static_cast<uint64_t>(%d), "%s", %d, %s, %s, %s, %s, %s, "%s", std::vector<const char*>{%s});\n' % (
                fnm["id"],
                fnm["name"],
                fnm["args_nums"],
                fnm["fn"],
                fnm["prepare"],
                fnm["close"],
                fnm["exception_safe"],
                fnm["check_overflow"],
                fnm['ret'], 
                ", ".join(['"%s"' % arg for arg in fnm['args']]),
            )
        else:
            modules_contents[target] = modules_contents[
                target
            ] + '\tBuiltinFunctions::emplace_builtin_function(static_cast<uint64_t>(%d), "%s", %d, %s, %s, %s, "%s", std::vector<const char*>{%s});\n' % (
                fnm["id"],
                fnm["name"],
                fnm["args_nums"],
                fnm["fn"],
                fnm["exception_safe"],
                fnm["check_overflow"],
                fnm['ret'], 
                ", ".join(['"%s"' % arg for arg in fnm['args']]),
            )

    for module in modules:
        with open(path + module + ".inc", mode="w+") as f:
            content = cpp_template.format(
                module=module, content=modules_contents[module]
            )
            f.write(content)

    generate_ai_cpp(path + 'AIFunctionDescriptors.inc')


if __name__ == "__main__":
    FE_PATH = "../../fe/fe-core/target/generated-sources/build"
    BE_PATH = "../build/gen_cpp"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cpp",
        dest="cpp_path",
        default=BE_PATH,
        help="Path of generated cpp file",
        type=str,
    )
    parser.add_argument(
        "--java",
        dest="java_path",
        default=FE_PATH,
        help="Path of generated java file",
        type=str,
    )
    args = parser.parse_args()

    be_functions_dir = args.cpp_path + "/opcode"
    os.makedirs(be_functions_dir, exist_ok=True)

    fe_functions_dir = args.java_path + "/com/starrocks/builtins"
    os.makedirs(fe_functions_dir, exist_ok=True)

    # Read the function metadata inputs
    for function in functions.vectorized_functions:
        add_function(function)

    for function in functions.ai_vectorized_functions:
        add_ai_function(function)

    generate_fe(fe_functions_dir + "/VectorizedBuiltinFunctions.java")
    generate_cpp(be_functions_dir + "/")
