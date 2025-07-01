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

package com.starrocks.persist;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class TableAddOrDropColumnsInfoTest {
    @Test
    public void test() {
        TableAddOrDropColumnsInfo info = new TableAddOrDropColumnsInfo(1, 1,
                Collections.emptyMap(), Collections.emptyList(), 0, 1, Collections.emptyMap());

        Assertions.assertEquals(1, info.getDbId());
        Assertions.assertEquals(1, info.getTableId());
        Assertions.assertEquals(0, info.getIndexes().size());
        Assertions.assertEquals(0, info.getIndexSchemaMap().size());
        Assertions.assertEquals(0, info.getJobId());
        Assertions.assertEquals(1, info.getTxnId());
        Assertions.assertEquals(0, info.getIndexToNewSchemaId().size());

        TableAddOrDropColumnsInfo info2 = new TableAddOrDropColumnsInfo(1, 1,
                Collections.emptyMap(), Collections.emptyList(), 0, 1, Collections.emptyMap());

        Assertions.assertEquals(info.hashCode(), info2.hashCode());
        Assertions.assertEquals(info, info2);
        Assertions.assertNotNull(info.toString());

    }

}
