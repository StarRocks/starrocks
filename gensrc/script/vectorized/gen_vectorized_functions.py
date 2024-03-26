#!/usr/bin/env python
# encoding: utf-8

"""
 This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 """

import os
import sys
from string import Template

import vectorized_functions

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

license_string = """
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

// This is a generated file, DO NOT EDIT.
// To add new functions, see the generator at
// common/function-registry/gen_builtins_catalog.py or the function list at
// common/function-registry/starrocks_builtins_functions.py.
"""

java_template = Template("""
${license}

package com.starrocks.builtins;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;

public class VectorizedBuiltinFunctions {
    public static void initBuiltins(FunctionSet functionSet) {
        ${functions}
  }
}

""")

cpp_template = Template("""
${license}

#include "exprs/vectorized/array_functions.h"
#include "exprs/vectorized/builtin_functions.h"
#include "exprs/vectorized/map_functions.h"
#include "exprs/vectorized/math_functions.h"
#include "exprs/vectorized/bit_functions.h"
#include "exprs/vectorized/string_functions.h"
#include "exprs/vectorized/time_functions.h"
#include "exprs/vectorized/like_predicate.h"
#include "exprs/vectorized/is_null_predicate.h"
#include "exprs/vectorized/hyperloglog_functions.h"
#include "exprs/vectorized/bitmap_functions.h"
#include "exprs/vectorized/json_functions.h"
#include "exprs/vectorized/hash_functions.h"
#include "exprs/vectorized/encryption_functions.h"
#include "exprs/vectorized/geo_functions.h"
#include "exprs/vectorized/percentile_functions.h"
#include "exprs/vectorized/grouping_sets_functions.h"
#include "exprs/vectorized/es_functions.h"
#include "exprs/vectorized/utility_functions.h"

namespace starrocks {
namespace vectorized {

BuiltinFunctions::FunctionTables BuiltinFunctions::_fn_tables = {
        ${functions}
};

}
}
""")

function_list = list()
function_set = set()


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

<<<<<<< HEAD:gensrc/script/vectorized/gen_vectorized_functions.py
    if "..." in fn_data[3]:
        assert 2 <= len(fn_data[3]), "Invalid arguments in vectorized_functions.py:\n\t" + repr(fn_data)
        assert "..." == fn_data[3][-1], "variadic parameter must at the end:\n\t" + repr(fn_data)
=======
    function_signature = "%s#%s#(%s)" % (entry["ret"], entry["name"], ", ".join(entry["args"]))

    if function_signature in function_signature_set:
        print("=================================================================")
        print("Duplicated function signature: " + function_signature)
        print("=================================================================")
        exit(1)
    function_signature_set.add(function_signature)

    if "..." in fn_data[5]:
        assert 2 <= len(fn_data[5]), "Invalid arguments in functions.py:\n\t" + repr(fn_data)
        assert "..." == fn_data[5][-1], "variadic parameter must at the end:\n\t" + repr(fn_data)
>>>>>>> 3d935f4706 ([BugFix] Function framework support checkout overflow (#43065)):gensrc/script/gen_functions.py

        entry["args_nums"] = len(fn_data[5]) - 1
    else:
        entry["args_nums"] = len(fn_data[5])

    entry["fn"] = "&" + fn_data[6] if fn_data[6] != "nullptr" else "nullptr"

    if len(fn_data) >= 9:
        entry["prepare"] = "&" + fn_data[7] if fn_data[7] != "nullptr" else "nullptr"
        entry["close"] = "&" + fn_data[8] if fn_data[8] != "nullptr" else "nullptr"

    function_list.append(entry)


def generate_fe(path):
    fn_template = Template(
        'functionSet.addVectorizedScalarBuiltin(${id}, "${name}", ${has_vargs}, Type.${ret}${args_types});')

    def gen_fe_fn(fnm):
        fnm["args_types"] = ", " if len(fnm["args"]) > 0 else ""
        fnm["args_types"] = fnm["args_types"] + ", ".join(["Type." + i for i in fnm["args"] if i != "..."])
        fnm["has_vargs"] = "true" if "..." in fnm["args"] else "false"

        return fn_template.substitute(fnm)

    value = dict()
    value["license"] = license_string
    value["functions"] = "\n        ".join([gen_fe_fn(i) for i in function_list])

    content = java_template.substitute(value)

    with open(path, mode="w+") as f:
        f.write(content)


def generate_cpp(path):
    def gen_be_fn(fnm):
        res = ""
        if "prepare" in fnm:
            res = '{%d, {"%s", %d, %s, %s, %s, %s, %s' % (
                fnm["id"], fnm["name"], fnm["args_nums"], fnm["fn"], fnm["prepare"], fnm["close"],
                fnm["exception_safe"], fnm["check_overflow"])
        else:
            res = '{%d, {"%s", %d, %s, %s, %s' % (
                fnm["id"], fnm["name"], fnm["args_nums"], fnm["fn"], fnm["exception_safe"],
                fnm["check_overflow"])

        return res + "}}"

    value = dict()
    value["license"] = license_string
    value["functions"] = ", \n        ".join([gen_be_fn(i) for i in function_list])

    content = cpp_template.substitute(value)

    with open(path, mode="w+") as f:
        f.write(content)


if __name__ == '__main__':
    # Read the function metadata inputs
    for function in vectorized_functions.vectorized_functions:
        add_function(function)

    FE_PATH = "../../../../fe/fe-core/target/generated-sources/build/com/starrocks/builtins/"
    BE_PATH = "../../gen_cpp/opcode/"

    if not os.path.exists(FE_PATH):
        os.makedirs(FE_PATH)

<<<<<<< HEAD:gensrc/script/vectorized/gen_vectorized_functions.py
    if not os.path.exists(BE_PATH):
        os.makedirs(BE_PATH)

    generate_fe(FE_PATH + "VectorizedBuiltinFunctions.java")
    generate_cpp(BE_PATH + "builtin_functions.cpp")
=======
    generate_fe(fe_functions_dir + "/VectorizedBuiltinFunctions.java")
    generate_cpp(be_functions_dir + "/builtin_functions.cpp")
>>>>>>> 3d935f4706 ([BugFix] Function framework support checkout overflow (#43065)):gensrc/script/gen_functions.py
