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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TabletGroupListTest {

    @Test
    public void testToStringAndGetters() {
        List<List<Long>> groups = List.of(List.of(1L, 2L), List.of(3L, 4L));
        TabletGroupList list = new TabletGroupList(groups, NodePosition.ZERO);
        Assertions.assertEquals(groups, list.getTabletIdGroups());
        Assertions.assertNotNull(list.getPos());
        Assertions.assertEquals("TABLETS (1, 2) (3, 4)", list.toString());

        TabletGroupList single = new TabletGroupList(List.of(List.of(1L, 2L)), NodePosition.ZERO);
        Assertions.assertEquals("TABLETS (1, 2)", single.toString());
    }
}
