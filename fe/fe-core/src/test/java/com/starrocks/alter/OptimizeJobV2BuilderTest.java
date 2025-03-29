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

package com.starrocks.alter;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.PartitionDesc;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

public class OptimizeJobV2BuilderTest {
    @Test
    public void testBuildWithOptimizeClause() throws StarRocksException {
        // Create a mock OlapTable
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(123L);
        Mockito.when(table.getName()).thenReturn("myTable");

        // Create a mock OptimizeClause
        OptimizeClause optimizeClause = Mockito.mock(OptimizeClause.class);
        Mockito.when(optimizeClause.getKeysDesc()).thenReturn(new KeysDesc());
        Mockito.when(optimizeClause.getPartitionDesc()).thenReturn(null);
        Mockito.when(optimizeClause.getSortKeys()).thenReturn(new ArrayList<String>());

        // Create an instance of OptimizeJobV2Builder
        OptimizeJobV2Builder builder = new OptimizeJobV2Builder(table)
                .withOptimizeClause(optimizeClause);

        // Call the build() method
        AlterJobV2 job = builder.build();

        // Assert that the returned job is an instance of OptimizeJobV2
        Assert.assertTrue(job instanceof OptimizeJobV2);

        // Assert that the job has the correct properties
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) job;
        Assert.assertEquals(123L, optimizeJob.getTableId());
        Assert.assertEquals("myTable", optimizeJob.getTableName());
    }

    @Test
    public void testBuildMergePartitionsJob() throws StarRocksException {
        // Create a mock OlapTable
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(123L);
        Mockito.when(table.getName()).thenReturn("myTable");

        // Create a mock OptimizeClause
        OptimizeClause optimizeClause = Mockito.mock(OptimizeClause.class);
        Mockito.when(optimizeClause.getKeysDesc()).thenReturn(new KeysDesc());
        Mockito.when(optimizeClause.getPartitionDesc()).thenReturn(new PartitionDesc());
        Mockito.when(optimizeClause.getSortKeys()).thenReturn(new ArrayList<String>());

        // Create an instance of OptimizeJobV2Builder
        OptimizeJobV2Builder builder = new OptimizeJobV2Builder(table)
                .withOptimizeClause(optimizeClause);

        // Call the build() method
        AlterJobV2 job = builder.build();

        // Assert that the returned job is an instance of MergePartitionJob
        Assert.assertTrue(job instanceof MergePartitionJob);

        // Assert that the job has the correct properties
        MergePartitionJob optimizeJob = (MergePartitionJob) job;
        Assert.assertEquals(123L, optimizeJob.getTableId());
        Assert.assertEquals("myTable", optimizeJob.getTableName());
    }

    @Test
    public void testBuildWithoutOptimizeClause() throws StarRocksException {
        // Create a mock OlapTable
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(123L);
        Mockito.when(table.getName()).thenReturn("myTable");
        Mockito.when(table.getStorageType()).thenReturn(null);
        Mockito.when(table.enableReplicatedStorage()).thenReturn(true);

        // Create an instance of OptimizeJobV2Builder without an optimize clause
        OptimizeJobV2Builder builder = new OptimizeJobV2Builder(table);
        builder.withOptimizeClause(new OptimizeClause(null, null, null, null, null, null));

        // Call the build() method
        AlterJobV2 job = builder.build();

        // Assert that the returned job is an instance of OnlineOptimizeJobV2
        Assert.assertTrue(job instanceof OnlineOptimizeJobV2);

        // Assert that the job has the correct properties
        OnlineOptimizeJobV2 onlineOptimizeJob = (OnlineOptimizeJobV2) job;
        Assert.assertEquals(123L, onlineOptimizeJob.getTableId());
        Assert.assertEquals("myTable", onlineOptimizeJob.getTableName());
    }
}