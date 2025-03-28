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
package com.starrocks.common.util;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

public class SqlUtilsTest {

    @Test
    public void testIsPreQuerySQL() {
        Assert.assertTrue(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("select @@query_timeout", SqlModeHelper.MODE_DEFAULT)));

        Assert.assertTrue(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("SET NAMES utf8mb4", SqlModeHelper.MODE_DEFAULT)));

        Assert.assertTrue(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                        SqlModeHelper.MODE_DEFAULT)));

        Assert.assertFalse(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("SET password = 'xxx'", SqlModeHelper.MODE_DEFAULT)));

        Assert.assertFalse(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("SET @ a = 1", SqlModeHelper.MODE_DEFAULT)));

        Assert.assertFalse(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("SET property for 'root' \"max_user_connections\"=\"1000\"",
                        SqlModeHelper.MODE_DEFAULT)));

        Assert.assertTrue(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("set query_timeout=600", SqlModeHelper.MODE_DEFAULT)));

        Assert.assertFalse(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("select sleep(10)", SqlModeHelper.MODE_DEFAULT)));

        Assert.assertTrue(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("select connection_id()", SqlModeHelper.MODE_DEFAULT)));

        Assert.assertTrue(SqlUtils.isPreQuerySQL(
                SqlParser.parseSingleStatement("select session_id()", SqlModeHelper.MODE_DEFAULT)));
    }
}
