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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;

import java.util.List;

/**
 * Shared test fixtures for the presplit package. Centralizes the
 * cross-test-class helpers so the contract changes in one place when the
 * underlying types evolve (e.g., when {@link ScanContext} stops being an
 * empty marker).
 */
final class PresplitTestSupport {

    static final ScanContext DUMMY_CONTEXT = new ScanContext() { };

    private PresplitTestSupport() {
    }

    static Column bigintColumn(String name) {
        return new Column(name, IntegerType.BIGINT);
    }

    static Column varcharColumn(String name) {
        return new Column(name, VarcharType.VARCHAR);
    }

    static Tuple bigintTuple(long value) {
        return new Tuple(List.of(Variant.of(IntegerType.BIGINT, Long.toString(value))));
    }

    static List<Variant> bigintRow(long value) {
        return List.of(Variant.of(IntegerType.BIGINT, Long.toString(value)));
    }

    static Tuple compositeTuple(String tenant, long position) {
        return new Tuple(List.of(
                Variant.of(VarcharType.VARCHAR, tenant),
                Variant.of(IntegerType.BIGINT, Long.toString(position))));
    }

    static List<Variant> compositeRow(String tenant, long position) {
        return List.of(
                Variant.of(VarcharType.VARCHAR, tenant),
                Variant.of(IntegerType.BIGINT, Long.toString(position)));
    }
}
