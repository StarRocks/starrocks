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

import com.starrocks.sql.ast.StatementBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TrinoDialectDowngradeTest extends TrinoTestBase {
    @BeforeAll
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

    @Test
    public void testRelationAliasCaseInsensitiveResetOnDowngrade() {
        // Test that relationAliasCaseInsensitive is reset to false when rollback to StarRocks parser
        String querySql = "select date_add('2010-11-30 23:59:59', INTERVAL 3 DAY);";
        try {
            connectContext.getSessionVariable().setEnableDialectDowngrade(true);
            connectContext.getSessionVariable().setSqlDialect("trino");

            // Before parsing, relationAliasCaseInsensitive should be false
            Assertions.assertFalse(connectContext.isRelationAliasCaseInsensitive(),
                    "relationAliasCaseInsensitive should be false before parsing");

            // This SQL will fail in Trino parser but succeed in StarRocks parser after downgrade
            StatementBase stmt = analyzeSuccess(querySql);
            Assertions.assertNotNull(stmt);

            // After rollback to StarRocks parser, relationAliasCaseInsensitive should be reset to false
            Assertions.assertFalse(connectContext.isRelationAliasCaseInsensitive(),
                    "relationAliasCaseInsensitive should be reset to false after downgrade");
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
            connectContext.setRelationAliasCaseInSensitive(false);
        }
    }

    @Test
    public void testRelationAliasCaseInsensitiveWithTrinoUnsupportedSyntax() {
        // Test with SQL that uses Trino unsupported syntax, triggering TrinoParserUnsupportedException
        // This SQL uses StarRocks-specific syntax that Trino parser doesn't support
        String querySql = "select * from t0 order by 1 limit 10 offset 5";
        try {
            connectContext.getSessionVariable().setEnableDialectDowngrade(true);
            connectContext.getSessionVariable().setSqlDialect("trino");

            // Before parsing
            Assertions.assertFalse(connectContext.isRelationAliasCaseInsensitive());

            // This should trigger downgrade and reset the flag
            StatementBase stmt = analyzeSuccess(querySql);
            Assertions.assertNotNull(stmt);

            // After rollback, flag should be false
            Assertions.assertFalse(connectContext.isRelationAliasCaseInsensitive(),
                    "relationAliasCaseInsensitive should be reset to false after Trino unsupported syntax downgrade");
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
            connectContext.setRelationAliasCaseInSensitive(false);
        }
    }

    @Test
    public void testRelationAliasCaseInsensitiveWithDowngradeDisabled() {
        // Test that relationAliasCaseInsensitive is reset even when downgrade is disabled
        // This SQL uses StarRocks-specific interval syntax which Trino parser cannot handle
        String querySql = "select date_add('2010-11-30 23:59:59', INTERVAL 3 DAY)";
        try {
            connectContext.getSessionVariable().setEnableDialectDowngrade(false);
            connectContext.getSessionVariable().setSqlDialect("trino");

            // Before parsing
            Assertions.assertFalse(connectContext.isRelationAliasCaseInsensitive());

            analyzeFail(querySql);

            // Even when exception is thrown, the flag should be false
            Assertions.assertFalse(connectContext.isRelationAliasCaseInsensitive(),
                    "relationAliasCaseInsensitive should be reset to false even when downgrade is disabled");
        } finally {
            connectContext.getSessionVariable().setSqlDialect("trino");
            connectContext.getSessionVariable().setEnableDialectDowngrade(true);
            connectContext.setRelationAliasCaseInSensitive(false);
        }
    }
}
