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

package com.starrocks.connector.share.iceberg;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class IcebergPartitionUtils {
    public static List<PartitionField> getAllPartitionFields(Table icebergTable) {
        Set<Integer> existingColumnsIds = icebergTable.schema()
                .columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(Collectors.toSet());

        List<PartitionField> visiblePartitionFields = icebergTable.specs()
                .values().stream()
                .flatMap(partitionSpec -> partitionSpec.fields().stream())
                .filter(partitionField -> existingColumnsIds.contains(partitionField.sourceId()))
                .collect(Collectors.toList());

        return filterDuplicates(visiblePartitionFields);
    }

    public static List<PartitionField> filterDuplicates(List<PartitionField> visiblePartitionFields) {
        Set<Integer> existingFieldIds = new HashSet<>();
        List<PartitionField> result = new ArrayList<>();
        for (PartitionField partitionField : visiblePartitionFields) {
            if (!existingFieldIds.contains(partitionField.fieldId())) {
                existingFieldIds.add(partitionField.fieldId());
                result.add(partitionField);
            }
        }
        return result;
    }
}
