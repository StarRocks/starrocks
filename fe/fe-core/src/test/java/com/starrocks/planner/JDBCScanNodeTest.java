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

package com.starrocks.planner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JDBCScanNodeTest {
    private static ConnectContext connectContext;
    private String mysqlJdbcUri = "jdbc:mysql://localhost:9030/db";
    private JDBCScanNode jdbcScanNode;

    @BeforeClass
    public static void beforeClass() {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    private Resource getMockedJDBCResource(String name, String jdbcUri, String databaseType) throws Exception {
        FeConstants.runningUnitTest = true;
        Map<String, String> resourceConfigs = Maps.newHashMap();
        resourceConfigs.put("jdbc_uri", jdbcUri);
        resourceConfigs.put("user", "user0");
        resourceConfigs.put("password", "password0");
        resourceConfigs.put("driver_url", "driver_url");
        resourceConfigs.put("driver_class", "driver_class");
        resourceConfigs.put("database_type", databaseType);
        FeConstants.runningUnitTest = false;
        return new JDBCResource(name, resourceConfigs);
    }

    private JDBCTable getMockedJDBCTable()
            throws DdlException {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", Type.INT));
        Map<String, String> properties = ImmutableMap.of(
                "driver_class", "com.mysql.cj.jdbc.Driver",
                "driver_url", "driver_url",
                "type", "jdbc",
                "user", "mysql",
                "password", "mysql",
                "jdbc_uri", "jdbc:mysql://127.0.0.1:3306",
                "checksum", "mychecksum12345",
                "resource", "jdbc0",
                "table", "table0"
        );
        return new JDBCTable(1, "jdbc0", schema, properties);
    }

    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr,
                         @Mocked ResourceMgr resourceMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", mysqlJdbcUri, "mysql");
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
    }

    @Test
    public void testSessionVariablesAreSetForMysqlProtocol(@Mocked GlobalStateMgr globalStateMgr,
                                                           @Mocked ResourceMgr resourceMgr) throws Exception {
        connectContext.getSessionVariable().setJdbcExternalTableSessionVariables(
                "session_variable_one='val',session_variable_two=5"
        );

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", mysqlJdbcUri, null);
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        jdbcScanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
        jdbcScanNode.computeColumnsAndFiltersAndSessionVariables();
        TPlanNode tPlanNode = new TPlanNode();
        jdbcScanNode.toThrift(tPlanNode);
        Assert.assertEquals(
                Arrays.asList("/*+ SET_VAR(session_variable_one='val') */\n/*+ SET_VAR(session_variable_two=5) */\n"),
                tPlanNode.getJdbc_scan_node().getSession_variable_hints()
        );
    }

    @Test
    public void testSessionVariablesAreSetForMysqlProtocolForStarRocksDb(@Mocked GlobalStateMgr globalStateMgr,
                                                           @Mocked ResourceMgr resourceMgr) throws Exception {
        connectContext.getSessionVariable().setJdbcExternalTableSessionVariables(
                "session_variable_one='val',session_variable_two=5"
        );

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", mysqlJdbcUri, "starrocks");
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        jdbcScanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
        jdbcScanNode.computeColumnsAndFiltersAndSessionVariables();
        TPlanNode tPlanNode = new TPlanNode();
        jdbcScanNode.toThrift(tPlanNode);
        Assert.assertEquals(
                Arrays.asList(
                        "/*+ SET_VAR\n  (\n  session_variable_one='val',\n  session_variable_two=5\n  ) */\n"
                ),
                tPlanNode.getJdbc_scan_node().getSession_variable_hints()
        );
    }

    @Test
    public void testSessionVariablesAndUserDefinedVariablesAreSetForMysqlProtocolForStarRocksDb(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ResourceMgr resourceMgr) throws Exception {
        connectContext.getSessionVariable().setJdbcExternalTableSessionVariables(
                "@user_defined_var=5,@user_defined_var_2='five',session_variable_one='val', session_variable_two=5," +
                        "session_variable_three=\"5\""
        );

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", mysqlJdbcUri, "starrocks");
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        jdbcScanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
        jdbcScanNode.computeColumnsAndFiltersAndSessionVariables();
        TPlanNode tPlanNode = new TPlanNode();
        jdbcScanNode.toThrift(tPlanNode);
        Assert.assertEquals(
                Arrays.asList(
                        "/*+ SET_VAR\n  (\n  session_variable_one='val',\n  session_variable_two=5,\n  "
                        + "session_variable_three=\"5\"\n  ) */\n",
                        "/*+ SET_USER_VARIABLE(@user_defined_var=5, @user_defined_var_2='five') */\n"
                ),
                tPlanNode.getJdbc_scan_node().getSession_variable_hints()
        );
    }

    @Test
    public void testSessionVariablesAndUserDefinedVariablesAreSetWithTrailingComma(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ResourceMgr resourceMgr) throws Exception {
        connectContext.getSessionVariable().setJdbcExternalTableSessionVariables(
                "@user_defined_var=5,@user_defined_var_2='five',session_variable_one='val',session_variable_two=5,"
        );

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", mysqlJdbcUri, "starrocks");
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        jdbcScanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
        jdbcScanNode.computeColumnsAndFiltersAndSessionVariables();
        TPlanNode tPlanNode = new TPlanNode();
        jdbcScanNode.toThrift(tPlanNode);
        Assert.assertEquals(
                Arrays.asList(
                        "/*+ SET_VAR\n  (\n  session_variable_one='val',\n  session_variable_two=5\n  ) */\n",
                        "/*+ SET_USER_VARIABLE(@user_defined_var=5, @user_defined_var_2='five') */\n"
                ),
                tPlanNode.getJdbc_scan_node().getSession_variable_hints()
        );
    }

    @Test
    public void testSessionVariablesThrowsUnsuppportedOperationExceptionForNonMysqlProtocol(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ResourceMgr resourceMgr) throws Exception {
        connectContext.getSessionVariable().setJdbcExternalTableSessionVariables(
                "@user_defined_var=5,@user_defined_var_2='five',session_variable_one='val',session_variable_two=5"
        );

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", "jdbc:postgresql://127.0.0.1:5432/db", null);
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        jdbcScanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
        UnsupportedOperationException exceptionThrown =Assert.assertThrows(
                UnsupportedOperationException.class, () ->
                jdbcScanNode.computeColumnsAndFiltersAndSessionVariables()
        );

        Assert.assertEquals(
                "Sending session variable to JDBC external table is only supported for MYSQL protocol",
                exceptionThrown.getMessage()
        );
    }

    @Test
    public void testSessionVariablesForUserDefinedVariablesThrowsUnsuppportedOperationExceptionForNonStarRocksDbType(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ResourceMgr resourceMgr) throws Exception {
        connectContext.getSessionVariable().setJdbcExternalTableSessionVariables(
                "@user_defined_var=5,@user_defined_var_2='five',session_variable_one='val',session_variable_two=5"
        );

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", mysqlJdbcUri, null);
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        jdbcScanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
        UnsupportedOperationException exceptionThrown =Assert.assertThrows(
                UnsupportedOperationException.class, () ->
                        jdbcScanNode.computeColumnsAndFiltersAndSessionVariables()
        );

        Assert.assertEquals(
                "Sending user defined variables to JDBC external table is only supported for " +
                        "\"database_type\" = 'starrocks'. " + "Invalid assignment: @user_defined_var=5. " +
                        "Please add \"database_type\" = \"starrocks\" to the JDBC resource properties on creation " +
                        "if you intend to propagate user defined variables to an external StarRocks cluster.",
                exceptionThrown.getMessage()
        );
    }

    @Test
    public void testSessionVariablesForThrowsIllegalArgumentExceptionForBadAssignment(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ResourceMgr resourceMgr) throws Exception {
        connectContext.getSessionVariable().setJdbcExternalTableSessionVariables(
                "session_variable_valid_num=5,session_variable_valid_str='five',session_variable_invalid=five,"
        );

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", mysqlJdbcUri, null);
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        jdbcScanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
        IllegalArgumentException exceptionThrown =Assert.assertThrows(
                IllegalArgumentException.class, () ->
                        jdbcScanNode.computeColumnsAndFiltersAndSessionVariables()
        );

        Assert.assertEquals(
                "Invalid session variable format for " +
                        "jdbc_external_table_session_variables. Invalid assignment: session_variable_invalid=five. " +
                        "Supports MySQL system variables or StarRocks user defined variables. " +
                        "Values can be a quoted string or a numeric value. For example: " +
                        "@my_var='some_value' or my_var=123. The entire string should be a comma separated string " +
                        "of variables e.g. \"@my_var='some_value',my_var2=123\"",
                exceptionThrown.getMessage()
        );
    }

    @Test
    public void testSessionVariablesForThrowsIllegalArgumentExceptionForMissedComma(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ResourceMgr resourceMgr) throws Exception {
        connectContext.getSessionVariable().setJdbcExternalTableSessionVariables(
                "session_variable_valid_num=5,session_variable_valid_str='five'session_var=five"
        );

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource("jdbc0", mysqlJdbcUri, null);
            }
        };
        JDBCTable table = getMockedJDBCTable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        jdbcScanNode = new JDBCScanNode(new PlanNodeId(0), desc, table);
        IllegalArgumentException exceptionThrown =Assert.assertThrows(
                IllegalArgumentException.class, () ->
                        jdbcScanNode.computeColumnsAndFiltersAndSessionVariables()
        );

        Assert.assertEquals(
                "Invalid session variable format for " +
                        "jdbc_external_table_session_variables. Invalid assignment: " +
                        "session_variable_valid_str='five'session_var=five. Supports MySQL system variables or " +
                        "StarRocks user defined variables. Values can be a quoted string or a numeric value. For " +
                        "example: @my_var='some_value' or my_var=123. The entire string should be a comma separated " +
                        "string of variables e.g. \"@my_var='some_value',my_var2=123\"",
                exceptionThrown.getMessage()
        );
    }
}

