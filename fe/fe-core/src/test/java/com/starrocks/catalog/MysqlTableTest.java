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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/MysqlTableTest.java

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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.Type;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class MysqlTableTest {
    private List<Column> columns;
    private Map<String, String> properties;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @BeforeEach
    public void setUp() {
        columns = Lists.newArrayList();
        Column column = new Column("col1", Type.BIGINT);
        column.setIsKey(true);
        columns.add(column);

        properties = Maps.newHashMap();
        properties.put("host", "127.0.0.1");
        properties.put("port", "3306");
        properties.put("user", "root");
        properties.put("password", "root");
        properties.put("database", "db");
        properties.put("table", "tbl");

        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testNoHost() {
        assertThrows(DdlException.class, () -> {
            Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
                @Override
                public boolean apply(String s) {
                    if (s.equalsIgnoreCase("host")) {
                        return false;
                    } else {
                        return true;
                    }
                }
            });
            new MysqlTable(1000, "mysqlTable", columns, pro);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testNoPort() {
        assertThrows(DdlException.class, () -> {
            Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
                @Override
                public boolean apply(String s) {
                    if (s.equalsIgnoreCase("port")) {
                        return false;
                    } else {
                        return true;
                    }
                }
            });
            new MysqlTable(1000, "mysqlTable", columns, pro);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testPortNotNumber() {
        assertThrows(DdlException.class, () -> {
            Map<String, String> pro = Maps.transformEntries(properties,
                    new Maps.EntryTransformer<String, String, String>() {
                        @Override
                        public String transformEntry(String s, String s2) {
                            if (s.equalsIgnoreCase("port")) {
                                return "abc";
                            }
                            return s2;
                        }
                    });
            new MysqlTable(1000, "mysqlTable", columns, pro);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testNoUser() {
        assertThrows(DdlException.class, () -> {
            Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
                @Override
                public boolean apply(String s) {
                    if (s.equalsIgnoreCase("user")) {
                        return false;
                    } else {
                        return true;
                    }
                }
            });
            new MysqlTable(1000, "mysqlTable", columns, pro);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testNoPass() {
        assertThrows(DdlException.class, () -> {
            Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
                @Override
                public boolean apply(String s) {
                    if (s.equalsIgnoreCase("password")) {
                        return false;
                    } else {
                        return true;
                    }
                }
            });
            new MysqlTable(1000, "mysqlTable", columns, pro);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testNoDb() {
        assertThrows(DdlException.class, () -> {
            Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
                @Override
                public boolean apply(String s) {
                    if (s.equalsIgnoreCase("database")) {
                        return false;
                    } else {
                        return true;
                    }
                }
            });
            new MysqlTable(1000, "mysqlTable", columns, pro);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testNoTbl() {
        assertThrows(DdlException.class, () -> {
            Map<String, String> pro = Maps.filterKeys(properties, new Predicate<String>() {
                @Override
                public boolean apply(String s) {
                    if (s.equalsIgnoreCase("table")) {
                        return false;
                    } else {
                        return true;
                    }
                }
            });
            new MysqlTable(1000, "mysqlTable", columns, pro);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testNoPro() {
        assertThrows(DdlException.class, () -> {
            new MysqlTable(1000, "mysqlTable", columns, null);
            Assertions.fail("No exception throws.");
        });
    }
}
