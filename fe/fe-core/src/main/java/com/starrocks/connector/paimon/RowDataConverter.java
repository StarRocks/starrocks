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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowDataConverter {
    private final Map<String, InternalRow.FieldGetter> fieldGetters;
    private final Map<String, DataType> dataTypes;

    public RowDataConverter(RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        this.fieldGetters = new HashMap<>(fieldCount);
        this.dataTypes = new HashMap<>();

        for (int i = 0; i < fieldCount; i++) {
            this.fieldGetters.put(rowType.getFields().get(i).name(),
                    InternalRowUtils.createNullCheckingFieldGetter(rowType.getTypeAt(i), i));
            this.dataTypes.put(rowType.getFieldNames().get(i), rowType.getFieldTypes().get(i));
        }
    }

    public List<String> convert(InternalRow rowData, List<String> requiredNames) {
        List<String> result = new ArrayList<>(requiredNames.size());
        for (String name : requiredNames) {
            InternalRow.FieldGetter fieldGetter = this.fieldGetters.get(name);
            Object o = fieldGetter.getFieldOrNull(rowData);
            DataType dataType = dataTypes.get(name);
            CastExecutor<Object, Object> executor = (CastExecutor<Object, Object>) CastExecutors
                    .resolve(dataType, DataTypes.STRING());
            String value;
            if (o == null) {
                value = "null";
            } else {
                if (executor != null) {
                    BinaryString casted = (BinaryString) executor.cast(o);
                    value = casted.toString();
                } else {
                    value = String.valueOf(o);
                }
            }
            result.add(value);
        }
        return result;
    }
}
