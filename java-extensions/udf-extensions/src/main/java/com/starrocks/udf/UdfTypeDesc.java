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
 * Java-side mirror of the SQL TypeDescriptor used by scalar UDF boxing/unboxing.
 *
 * Built once per UDF argument and per UDF return type at CREATE FUNCTION /
 * function-context construction time on the BE; passed to the unified
 * UDFHelper.boxColumn / UDFHelper.writeResult entry points so the recursion
 * happens entirely on the Java side. This keeps the BE call sites flat
 * (one helper invocation per arg / return) and removes the parallel C++
 * type-info tree.
 *
 * Slot conventions:
 *   - SCALAR / DECIMAL: leaf, children == null. DECIMAL slots additionally
 *     carry precision and scale.
 *   - ARRAY: children = { elementType }.
 *   - MAP: children = { keyType, valueType }.
 *   - STRUCT: children = { field0Type, field1Type, ... } and recordClass
 *     holds the formal Java record class bound to the SQL STRUCT.
 */
public final class UdfTypeDesc {
    public final int logicalType;
    public final UdfTypeDesc[] children;
    public final int precision;
    public final int scale;
    public final Class<?> recordClass;

    public UdfTypeDesc(int logicalType, UdfTypeDesc[] children, int precision, int scale, Class<?> recordClass) {
        this.logicalType = logicalType;
        this.children = children;
        this.precision = precision;
        this.scale = scale;
        this.recordClass = recordClass;
    }
}
