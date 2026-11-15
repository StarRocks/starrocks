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

package com.starrocks.catalog.system.information;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

class BeCloudNativeCompactionsSystemTableTest {
    @Test
    void testColumnContract() {
        SystemTable table = BeCloudNativeCompactionsSystemTable.create();
        List<String> columns = table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());

        Assertions.assertEquals(List.of("BE_ID", "TXN_ID", "TABLET_ID", "VERSION", "SKIPPED", "RUNS",
                "START_TIME", "FINISH_TIME", "PROGRESS", "STATUS", "PROFILE", "SUBTASK_ID"), columns);
        Assertions.assertEquals(Table.TableType.SCHEMA, table.getType());
    }
}
