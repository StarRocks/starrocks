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

package com.starrocks.connector.paimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class RowDataConverter {
    private final InternalRow.FieldGetter[] fieldGetters;

    public RowDataConverter(RowType rowType) {
        this.fieldGetters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                i ->
                                        InternalRowUtils.createNullCheckingFieldGetter(
                                                rowType.getTypeAt(i), i))
                        .toArray(InternalRow.FieldGetter[]::new);
    }

    public List<String> convert(InternalRow rowData) {
        List<String> result = new ArrayList<>(fieldGetters.length);
        for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
            Object o = fieldGetter.getFieldOrNull(rowData);
            String value = o == null ? "null" : o.toString();
            result.add(value);
        }
        return result;
    }
}
