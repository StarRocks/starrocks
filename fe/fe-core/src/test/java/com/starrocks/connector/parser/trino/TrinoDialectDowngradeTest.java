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

import com.starrocks.common.Config;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

    // Falling back from the Trino parser logs the statement and the parsers' messages, which quote it; under
    // enable_sql_desensitize_in_log neither may reach the log.
    @Test
    public void testFallbackLogsAreDesensitized() {
        String downgraded = "select date_add('SECRET_DOWNGRADE', INTERVAL 3 DAY)";
        String unparsable = "select 'SECRET_UNPARSABLE' from t0 where";
        List<String> lines = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("trino-desensitize-capture-" + UUID.randomUUID(), null, null) {
            @Override
            public void append(LogEvent event) {
                StringBuilder text = new StringBuilder(event.getMessage().getFormattedMessage());
                for (Throwable t = event.getThrown(); t != null; t = t.getCause()) {
                    text.append(' ').append(t.getMessage());
                }
                lines.add(text.toString());
            }
        };
        appender.start();
        Logger logger = (Logger) LogManager.getLogger(SqlParser.class);
        Level prevLevel = logger.getLevel();
        logger.addAppender(appender);
        logger.setLevel(Level.TRACE);
        boolean prevDesensitize = Config.enable_sql_desensitize_in_log;
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        try {
            sessionVariable.setEnableDialectDowngrade(true);
            sessionVariable.setSqlDialect("trino");

            // the fallback is logged, unchanged when desensitize is off
            Config.enable_sql_desensitize_in_log = false;
            SqlParser.parse(downgraded, sessionVariable);
            Assertions.assertTrue(lines.stream().anyMatch(l -> l.contains("SECRET_DOWNGRADE")), lines.toString());

            lines.clear();
            Config.enable_sql_desensitize_in_log = true;
            SqlParser.parse(downgraded, sessionVariable);
            Assertions.assertThrows(Exception.class, () -> SqlParser.parse(unparsable, sessionVariable));
            // the failure type stays
            Assertions.assertTrue(lines.stream().anyMatch(l -> l.equals("Trino parse sql [<desensitized>] error, "
                    + "cause by io.trino.sql.parser.ParsingException: <desensitized>")), lines.toString());
            Assertions.assertTrue(lines.stream().anyMatch(l -> l.startsWith("StarRocks parse sql [<desensitized>]")),
                    lines.toString());
            for (String line : lines) {
                Assertions.assertFalse(line.contains("SECRET"), line);
            }
        } finally {
            Config.enable_sql_desensitize_in_log = prevDesensitize;
            sessionVariable.setSqlDialect("trino");
            logger.removeAppender(appender);
            logger.setLevel(prevLevel);
        }
    }
}
