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

package com.starrocks.connector.parser.trino;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrinoDialectDowngradeTest extends TrinoTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @Test
    public void testTrinoDialectDowngrade() throws Exception {
        String querySql = "select date_add('2010-11-30 23:59:59', INTERVAL 3 DAY);";
        try {
            connectContext.getSessionVariable().setEnableDialectDowngrade(true);
            connectContext.getSessionVariable().setSqlDialect("trino");
            analyzeSuccess(querySql);
            connectContext.getSessionVariable().setEnableDialectDowngrade(false);
            analyzeFail(querySql, "mismatched input '3'. Expecting: '+', '-', <string>");
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
        }
    }
}
