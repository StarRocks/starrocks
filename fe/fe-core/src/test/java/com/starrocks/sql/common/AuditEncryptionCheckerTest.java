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

package com.starrocks.sql.common;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuditEncryptionCheckerTest {

    @Test
    public void testProviderDdlKeepsRawSqlFormatting() {
        // Provider DDL has no AST SQL formatter; keep the existing raw-SQL redaction path.
        for (String sql : new String[] {
                "CREATE AI PROVIDER p TYPE chat PROPERTIES ('endpoint'='https://models.example.test/chat', "
                        + "'model'='chat', 'api_key'='audit-test-secret')",
                "ALTER AI PROVIDER p SET ('api_key'='audit-test-secret')"
        }) {
            StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertFalse(AuditEncryptionChecker.needEncrypt(stmt));
        }
    }

    @Test
    public void testProviderFunctionDoesNotContainCredentials() {
        StatementBase stmt = SqlParser.parseSingleStatement(
                "SELECT ai_custom_query('p', 'prompt')", SqlModeHelper.MODE_DEFAULT);
        Assertions.assertFalse(AuditEncryptionChecker.needEncrypt(stmt));
    }

    @Test
    public void testNeedEncryptInsertSelectFromFiles() {
        StatementBase stmt = SqlParser.parseSingleStatement(
                "INSERT INTO t0 SELECT * FROM FILES(" +
                        "\"path\"=\"s3://bucket/data.parquet\", " +
                        "\"format\"=\"parquet\", " +
                        "\"aws.s3.access_key\"=\"AKIA_SOURCE\", " +
                        "\"aws.s3.secret_key\"=\"SOURCE_SECRET\")",
                SqlModeHelper.MODE_DEFAULT);
        Assertions.assertTrue(AuditEncryptionChecker.needEncrypt(stmt));
    }

    @Test
    public void testNeedEncryptInsertIntoFiles() {
        StatementBase stmt = SqlParser.parseSingleStatement(
                "INSERT INTO FILES(" +
                        "\"path\"=\"s3://bucket/output/\", " +
                        "\"format\"=\"parquet\", " +
                        "\"aws.s3.access_key\"=\"AKIA_TARGET\", " +
                        "\"aws.s3.secret_key\"=\"TARGET_SECRET\") " +
                        "SELECT 1",
                SqlModeHelper.MODE_DEFAULT);
        Assertions.assertTrue(AuditEncryptionChecker.needEncrypt(stmt));
    }
}
