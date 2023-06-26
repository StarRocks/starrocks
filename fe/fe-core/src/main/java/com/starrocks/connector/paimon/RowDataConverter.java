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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowDataConverter {
    private final Map<String, InternalRow.FieldGetter> fieldGetters;

    public RowDataConverter(RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        this.fieldGetters = new HashMap<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            this.fieldGetters.put(rowType.getFields().get(i).name(),
                    InternalRowUtils.createNullCheckingFieldGetter(rowType.getTypeAt(i), i));
        }
    }

    public List<String> convert(InternalRow rowData, List<String> requiredNames) {
        List<String> result = new ArrayList<>(requiredNames.size());
        for (String name : requiredNames) {
            InternalRow.FieldGetter fieldGetter = this.fieldGetters.get(name);
            Object o = fieldGetter.getFieldOrNull(rowData);
            String value = o == null ? "null" : o.toString();
            result.add(value);
        }
        return result;
    }
}
