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

import java.util.List;
import java.util.Map;

/**
 * Test scaffold used exclusively by BE-side unit tests of the STRUCT UDF
 * input/output paths.
 *
 * <p>Each method exists only so the BE can fetch a concretely-parameterized
 * {@code java.lang.reflect.Type} via {@code Method.getGenericReturnType()}
 * (and for UDAF / UDTF shapes, the parameter types via
 * {@code Method.getGenericParameterTypes()}), which is what
 * {@code build_udf_type_desc} / {@code build_method_udf_type_descs} consume
 * when building the {@link UdfTypeDesc} tree for ARRAY / MAP / STRUCT slots.
 * The method bodies are never invoked.
 *
 * <p>Production code does not reference this class.
 */
public final class UdfTestSupport {
    /** Standalone scratch state class used by the UDAF method shapes below. */
    public static final class State {
    }

    private UdfTestSupport() {
    }

    public static UdfTestStructRecord structReturn() {
        return null;
    }

    public static List<UdfTestStructRecord> arrayOfStruct() {
        return null;
    }

    public static Map<String, UdfTestStructRecord> mapOfStruct() {
        return null;
    }

    public static List<List<UdfTestStructRecord>> arrayOfArrayOfStruct() {
        return null;
    }

    // --- UDAF method shapes ---------------------------------------------
    //
    // `update(State, args...)` lets BE tests exercise build_method_udf_type_descs
    // with state_offset=1 and a STRUCT-bearing SQL argument. The method's own
    // return is void, mirroring the production UDAF update contract.
    public static void udafUpdateWithStruct(State state, UdfTestStructRecord record) {
    }

    // `finalize(State)` returning List<Record> exercises the UDAF finalize path
    // where the SQL return is array<struct<...>> while the Java return is the
    // parameterized List itself (no array unwrap needed for UDAF).
    public static List<UdfTestStructRecord> udafFinalizeArrayOfStruct(State state) {
        return null;
    }

    // --- UDTF method shapes ---------------------------------------------
    //
    // UDTF's process(...) returns `T[]` where T is the per-row SQL return
    // element type. Used to verify the unwrap_return_array_layer path in
    // build_method_udf_type_descs (Class<Record[]> -> Class<Record>).
    public static UdfTestStructRecord[] udtfProcessReturnsRecordArray(UdfTestStructRecord record) {
        return null;
    }
}
