// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.jmockit.Deencapsulation;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class OlapScanNodeTest {
    // columnA in (1) hashmode=3
    @Test
    public void testHashDistributionOneUser() throws AnalysisException {

        List<Long> partitions = new ArrayList<>();
        partitions.add(new Long(0));
        partitions.add(new Long(1));
        partitions.add(new Long(2));

        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", Type.BIGINT));

        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));

        Expr compareExpr = new SlotRef(new TableName("db", "tableName"), "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
        filterMap.put("columnA", columnFilter);

        DistributionPruner partitionPruner = new HashDistributionPruner(
                partitions,
                columns,
                filterMap,
                3);

        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 1);

        for (Long id : ids) {
            Assert.assertEquals((1 & 0xffffffff) % 3, id.intValue());
        }
    }

    // columnA in (1, 2 ,3, 4, 5, 6) hashmode=3
    @Test
    public void testHashPartitionManyUser() throws AnalysisException {

        List<Long> partitions = new ArrayList<>();
        partitions.add(new Long(0));
        partitions.add(new Long(1));
        partitions.add(new Long(2));

        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", Type.BIGINT));

        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));
        inList.add(new IntLiteral(2));
        inList.add(new IntLiteral(3));
        inList.add(new IntLiteral(4));
        inList.add(new IntLiteral(5));
        inList.add(new IntLiteral(6));

        Expr compareExpr = new SlotRef(new TableName("db", "tableName"), "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
        filterMap.put("columnA", columnFilter);

        DistributionPruner partitionPruner = new HashDistributionPruner(
                partitions,
                columns,
                filterMap,
                3);

        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 3);
    }

    @Test
    public void testHashForIntLiteral() {
        {
            HashDistributionKey hashKey = new HashDistributionKey();
            hashKey.pushColumn(new IntLiteral(1), Type.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        }
        {
            HashDistributionKey hashKey = new HashDistributionKey();
            hashKey.pushColumn(new IntLiteral(2), Type.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        }
        {
            HashDistributionKey hashKey = new HashDistributionKey();
            hashKey.pushColumn(new IntLiteral(3), Type.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        }
        {
            HashDistributionKey hashKey = new HashDistributionKey();
            hashKey.pushColumn(new IntLiteral(4), Type.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        }
        {
            HashDistributionKey hashKey = new HashDistributionKey();
            hashKey.pushColumn(new IntLiteral(5), Type.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        }
        {
            HashDistributionKey hashKey = new HashDistributionKey();
            hashKey.pushColumn(new IntLiteral(6), Type.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        }
    }

    @Test
    public void testAssertFragmentWithUnpartitionInput(@Injectable AssertNumRowsNode assertNumRowsNode,
                                                       @Injectable PlanFragment inputFragment,
                                                       @Mocked PlannerContext plannerContext) {
        DistributedPlanner distributedPlanner = new DistributedPlanner(plannerContext);

        PlanFragment assertFragment = Deencapsulation.invoke(distributedPlanner, "createAssertFragment",
                assertNumRowsNode, inputFragment);
        Assert.assertSame(assertFragment, inputFragment);
        Assert.assertTrue(assertFragment.getPlanRoot() instanceof AssertNumRowsNode);
    }
}
