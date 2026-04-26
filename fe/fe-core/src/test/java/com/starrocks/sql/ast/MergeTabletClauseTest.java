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

import com.starrocks.common.Config;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class MergeTabletClauseTest {

    @Test
    public void testDefaultConstructorAndGetters() {
        MergeTabletClause clause = new MergeTabletClause();
        Assertions.assertNull(clause.getPartitionNames());
        Assertions.assertNull(clause.getTabletGroupList());
        Assertions.assertNull(clause.getProperties());
        Assertions.assertEquals(Config.tablet_reshard_target_size, clause.getTabletReshardTargetSize());
        Assertions.assertTrue(clause.toString().contains("MERGE TABLET"));

        clause.setTabletReshardTargetSize(1024L);
        Assertions.assertEquals(1024L, clause.getTabletReshardTargetSize());
    }

    @Test
    public void testConstructorAndAccept() {
        TabletGroupList tabletGroupList = new TabletGroupList(List.of(List.of(1L, 2L)), NodePosition.ZERO);
        Map<String, String> properties = Map.of("tablet_reshard_target_size", "1024");
        MergeTabletClause clause = new MergeTabletClause(null, tabletGroupList, properties);

        Assertions.assertNull(clause.getPartitionNames());
        Assertions.assertEquals(tabletGroupList, clause.getTabletGroupList());
        Assertions.assertEquals(properties, clause.getProperties());
        Assertions.assertTrue(clause.toString().contains("MERGE TABLETS (1, 2)"));

        PartitionRef partitionRef = new PartitionRef(List.of("p1"), false, NodePosition.ZERO);
        MergeTabletClause partitionClause = new MergeTabletClause(partitionRef, null, properties);
        Assertions.assertTrue(partitionClause.toString().contains("MERGE TABLET PARTITIONS"));

        AstVisitorExtendInterface<String, Void> visitor = new AstVisitorExtendInterface<>() {
            @Override
            public String visitNode(ParseNode node, Void context) {
                return "visitNode";
            }
        };
        Assertions.assertEquals("visitNode", clause.accept(visitor, null));
    }
}
