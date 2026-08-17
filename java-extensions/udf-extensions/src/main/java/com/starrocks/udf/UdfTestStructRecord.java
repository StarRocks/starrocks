// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.udf;

/**
 * Test scaffold record used exclusively by the BE-side unit tests of
 * the STRUCT UDF input-boxing / output-writing paths in
 * {@code java_data_converter.cpp} and {@code java_udf.cpp}.
 *
 * <p>Production code does not reference this class. It exists in the
 * runtime jar so the BE C++ tests can resolve a concrete record via
 * {@code FindClass("com/starrocks/udf/UdfTestStructRecord")} without
 * having to load a user-supplied UDF jar.
 *
 * <p>Field shape mirrors the SQL signature
 * {@code STRUCT<key INT, value VARCHAR>} which is the canonical example
 * used in {@code test/sql/test_udf/test_jvm_struct_udf}.
 */
public record UdfTestStructRecord(Integer key, String value) {
}
