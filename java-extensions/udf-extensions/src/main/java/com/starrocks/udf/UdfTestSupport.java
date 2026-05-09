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
 * {@code java.lang.reflect.Type} via {@code Method.getGenericReturnType()},
 * which is what {@code build_udf_type_desc} consumes when building the
 * {@link UdfTypeDesc} tree for ARRAY / MAP / STRUCT slots. The method bodies
 * are never invoked.
 *
 * <p>Production code does not reference this class.
 */
public final class UdfTestSupport {
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
}
