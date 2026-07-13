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

package com.starrocks.analysis;

import com.google.common.collect.Maps;
import com.starrocks.common.StarRocksException;
import com.starrocks.plugin.DynamicPluginLoader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class InstallPluginStmtTest {
    @Test
    public void testNormal() throws StarRocksException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("md5sum", "7529db41471ec72e165f96fe9fb92742");
        InstallPluginStmt stmt = new InstallPluginStmt("http://test/test.zip", properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
        Assertions.assertEquals("7529db41471ec72e165f96fe9fb92742",
                stmt.getProperties().get(DynamicPluginLoader.MD5SUM_KEY));
        Assertions.assertEquals("http://test/test.zip", stmt.getPluginPath());
    }

    @Test
    public void testInstallPluginIfNotExistsParsedTrue() {
        String sql = "INSTALL PLUGIN IF NOT EXISTS FROM '/path/to/plugin.zip'";
        List<StatementBase> stmts = SqlParser.parse(sql, 32);
        InstallPluginStmt stmt = (InstallPluginStmt) stmts.get(0);
        Assertions.assertTrue(stmt.isIfNotExists());
    }

    @Test
    public void testInstallPluginWithoutIfNotExistsDefaultsFalse() {
        String sql = "INSTALL PLUGIN FROM '/path/to/plugin.zip'";
        List<StatementBase> stmts = SqlParser.parse(sql, 32);
        InstallPluginStmt stmt = (InstallPluginStmt) stmts.get(0);
        Assertions.assertFalse(stmt.isIfNotExists());
    }

    @Test
    public void testUninstallPluginIfExistsParsedTrue() {
        String sql = "UNINSTALL PLUGIN IF EXISTS my_plugin";
        List<StatementBase> stmts = SqlParser.parse(sql, 32);
        UninstallPluginStmt stmt = (UninstallPluginStmt) stmts.get(0);
        Assertions.assertTrue(stmt.isIfExists());
    }

    @Test
    public void testUninstallPluginWithoutIfExistsDefaultsFalse() {
        String sql = "UNINSTALL PLUGIN my_plugin";
        List<StatementBase> stmts = SqlParser.parse(sql, 32);
        UninstallPluginStmt stmt = (UninstallPluginStmt) stmts.get(0);
        Assertions.assertFalse(stmt.isIfExists());
    }

    @Test
    public void testInstallPluginConstructorDefaultsIfNotExistsFalse() {
        Map<String, String> props = Maps.newHashMap();
        InstallPluginStmt stmt = new InstallPluginStmt("http://test/plugin.zip", props);
        Assertions.assertFalse(stmt.isIfNotExists());
        Assertions.assertEquals("http://test/plugin.zip", stmt.getPluginPath());
    }

    @Test
    public void testUninstallPluginConstructorDefaultsIfExistsFalse() {
        UninstallPluginStmt stmt = new UninstallPluginStmt("my_plugin");
        Assertions.assertFalse(stmt.isIfExists());
        Assertions.assertEquals("my_plugin", stmt.getPluginName());
    }
}
