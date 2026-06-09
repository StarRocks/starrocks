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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/MaterializedIndexMetaTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.thrift.TStorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class MaterializedIndexMetaTest {

    private static String fileName = "./MaterializedIndexMetaSerializeTest";

    @AfterEach
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSetDefineExprCaseInsensitive() {
        List<Column> schema = Lists.newArrayList();
        Column column = new Column("UPPER", Type.ARRAY_VARCHAR);
        schema.add(column);
        MaterializedIndexMeta meta = new MaterializedIndexMeta(0, schema, 0, 0,
                (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS, null);

        Map<String, Expr> columnNameToDefineExpr = Maps.newHashMap();
        columnNameToDefineExpr.put("upper", new StringLiteral());
        meta.setColumnsDefineExpr(columnNameToDefineExpr);
        Assertions.assertNotNull(column.getDefineExpr());
    }

    /**
     * Regression test for the data race on updateSchemaBackendId. The send path (reserve) and the
     * task-finish path (remove) both run under a shared READ lock, so they mutate the set
     * concurrently. With a plain HashSet this races and can throw or corrupt the set during resize;
     * with a concurrent set plus an atomic reserve-then-act it must be safe and consistent.
     *
     * <p>This is a probabilistic race test, not a deterministic one: it hammers the set hard enough
     * that the old buggy implementation would very likely throw, but a passing run does not prove
     * the absence of a race on every reintroduced bug.
     */
    @Test
    public void testUpdateSchemaBackendConcurrency() throws InterruptedException {
        List<Column> schema = Lists.newArrayList(new Column("c0", ArrayType.ARRAY_VARCHAR));
        MaterializedIndexMeta meta = new MaterializedIndexMeta(0, schema, 0, 0,
                (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS, null);

        final int threadCount = 16;
        final int iterations = 2000;
        final int backendCount = 200;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int t = 0; t < threadCount; t++) {
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterations; i++) {
                        Long backendId = (long) (i % backendCount);
                        // Mirror the production check-then-act: only the sender that won the
                        // reservation removes it, just like only a real task gets finished.
                        if (meta.addUpdateSchemaBackendIfAbsent(backendId)) {
                            meta.removeUpdateSchemaBackend(backendId);
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
            thread.start();
        }

        startLatch.countDown();
        doneLatch.await();

        Assertions.assertNull(failure.get(), "concurrent add/remove threw: " + failure.get());

        // Every reservation was paired with a removal, so the set must be empty: a fresh reserve
        // for any backend must succeed (return true), proving no entry leaked.
        for (long backendId = 0; backendId < backendCount; backendId++) {
            Assertions.assertTrue(meta.addUpdateSchemaBackendIfAbsent(backendId),
                    "backend " + backendId + " was still reserved; an entry leaked");
        }
    }
}
