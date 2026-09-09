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

package com.starrocks.plugin;

import com.starrocks.common.Config;
import com.starrocks.server.RunMode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class AuditEventTest {
    @Test
    public void testAuditEvent() {
        AuditEvent.AuditEventBuilder builder = new AuditEvent.AuditEventBuilder()
                .setEventType(AuditEvent.EventType.CONNECTION)
                .setUser("user")
                .setAuthorizedUser("authorizedUser")
                .setClientIp("clientIp")
                .setFeIp("feIp")
                .setDb("db")
                .setState("state")
                .setBigQueryLogCPUSecondThreshold(1)
                .setCatalog("catalog")
                .setQueryId("queryId")
                .setWriteClientTimeMs(100)
                .setStmtId(123)
                .setStmt("stmt")
                .setDigest("digest")
                .setErrorCode("errorCode")
                .setIsQuery(true)
                .setWarehouse("wh")
                .setSessionId("sessionId")
                .setCustomQueryId("customQueryId")
                .setCustomSessionName("customSessionName")
                .setCNGroup("test_cngroup")
                .setQueriedRelations(Arrays.asList("default_catalog.db.tbl", "default_catalog.db.view1"))
                .addReadLocalCnt(100)
                .addReadRemoteCnt(100);

        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedNothingMode() {
                return false;
            }
        };
        AuditEvent event = builder.build();

        Assertions.assertEquals(AuditEvent.EventType.CONNECTION, event.type);
        Assertions.assertEquals("user", event.user);
        Assertions.assertEquals("authorizedUser", event.authorizedUser);
        Assertions.assertEquals("clientIp", event.clientIp);
        Assertions.assertEquals("feIp", event.feIp);
        Assertions.assertEquals("db", event.db);
        Assertions.assertEquals("state", event.state);
        Assertions.assertEquals(1, event.bigQueryLogCPUSecondThreshold);
        Assertions.assertEquals("catalog", event.catalog);
        Assertions.assertEquals("queryId", event.queryId);
        Assertions.assertEquals(123, event.stmtId);
        Assertions.assertEquals("stmt", event.stmt);
        Assertions.assertEquals("digest", event.digest);
        Assertions.assertEquals("errorCode", event.errorCode);
        Assertions.assertEquals(true, event.isQuery);
        Assertions.assertEquals("wh", event.warehouse);
        Assertions.assertEquals("sessionId", event.sessionId);
        Assertions.assertEquals("customQueryId", event.customQueryId);
        Assertions.assertEquals("customSessionName", event.customSessionName);
        Assertions.assertEquals("test_cngroup", event.cnGroup);
        Assertions.assertEquals(Arrays.asList("default_catalog.db.tbl", "default_catalog.db.view1"),
                event.queriedRelations);
        Assertions.assertEquals("50.0%", event.cacheHitRatio);
        Assertions.assertEquals(100, event.writeClientTimeMs);
        Assertions.assertEquals((float) 50, event.getCacheMissRatio());
    }

    @Test
    public void testNormalizeErrorMessage() {
        Assertions.assertEquals("", AuditEvent.normalizeErrorMessage(null));
        Assertions.assertEquals("", AuditEvent.normalizeErrorMessage(""));
        Assertions.assertEquals("Unknown table 'db.tbl'.",
                AuditEvent.normalizeErrorMessage("Unknown table 'db.tbl'."));
    }

    // The audit log keeps one record per line with '|' between the fields.
    @Test
    public void testNormalizeErrorMessageCollapsesSeparators() {
        Assertions.assertEquals("failed on BE 127.0.0.1 out of memory",
                AuditEvent.normalizeErrorMessage("failed on BE 127.0.0.1\n\tout of memory"));
        Assertions.assertEquals("forged State=OK",
                AuditEvent.normalizeErrorMessage("forged|State=OK"));
    }

    // \p{Cntrl} is US-ASCII only, so the Unicode line breaks have to be named explicitly. A reader
    // that honours them (java.util.Scanner) would otherwise see one record as two.
    @Test
    public void testNormalizeErrorMessageCollapsesUnicodeLineBreaks() {
        Assertions.assertEquals("before after",
                AuditEvent.normalizeErrorMessage("before\u0085after"));
        Assertions.assertEquals("before after",
                AuditEvent.normalizeErrorMessage("before\u2028after"));
        Assertions.assertEquals("before after",
                AuditEvent.normalizeErrorMessage("before\u2029after"));
        Assertions.assertEquals("Unknown table 'a b'.",
                AuditEvent.normalizeErrorMessage("Unknown table 'a\u2028b'."));
    }

    // The collapsed set has to stay complete: these seven code points are exactly what the JDK
    // itself calls a line break (the \R construct), and a message keeping any of them can be read
    // as two audit records. Both entry points must strip them.
    @Test
    public void testCollapsesEveryJdkLineBreak() {
        for (int codePoint : new int[] {0x0A, 0x0B, 0x0C, 0x0D, 0x0085, 0x2028, 0x2029}) {
            String message = "a" + new String(Character.toChars(codePoint)) + "b";
            String name = "U+" + Integer.toHexString(codePoint);
            Assertions.assertEquals("a b", AuditEvent.normalizeErrorMessage(message), name);
            Assertions.assertEquals("a b", AuditEvent.collapseSeparators(message), name);
        }
    }

    // Format characters are not line breaks, and stripping them would corrupt legitimate text
    // (U+200D joins emoji sequences and Indic clusters), so they are deliberately left alone.
    @Test
    public void testKeepsFormatCharacters() {
        String joined = "a\u200db";
        Assertions.assertEquals(joined, AuditEvent.normalizeErrorMessage(joined));
    }

    @Test
    public void testNormalizeErrorMessageTruncates() {
        int original = Config.audit_log_error_message_max_length;
        try {
            Config.audit_log_error_message_max_length = 10;
            Assertions.assertEquals("0123456789... /* truncated, audit_log_error_message_max_length=10 */",
                    AuditEvent.normalizeErrorMessage("0123456789abcdef"));
            Assertions.assertEquals("0123456789", AuditEvent.normalizeErrorMessage("0123456789"));

            // Truncating must not cut a surrogate pair in half.
            Config.audit_log_error_message_max_length = 4;
            Assertions.assertEquals("abc... /* truncated, audit_log_error_message_max_length=4 */",
                    AuditEvent.normalizeErrorMessage("abc😀def"));

            // 0 turns the field off entirely; combined with ignore_empty the key disappears.
            Config.audit_log_error_message_max_length = 0;
            Assertions.assertEquals("", AuditEvent.normalizeErrorMessage("some failure"));
        } finally {
            Config.audit_log_error_message_max_length = original;
        }
    }

    @Test
    public void testNormalizeErrorMessageRedactsCredentials() {
        String redacted = AuditEvent.normalizeErrorMessage(
                "failed to access \"aws.s3.secret_key\" = \"AKIAsecret\" bucket");
        Assertions.assertFalse(redacted.contains("AKIAsecret"), redacted);
    }

    // The message quotes the very literals that desensitizing the statement takes out.
    @Test
    public void testNormalizeErrorMessageSuppressedWhenDesensitizing() {
        boolean original = Config.enable_sql_desensitize_in_log;
        try {
            Config.enable_sql_desensitize_in_log = true;
            Assertions.assertEquals("", AuditEvent.normalizeErrorMessage("Duplicate partition value 'secret'"));
        } finally {
            Config.enable_sql_desensitize_in_log = original;
        }
    }

    // The connection audit reports the same message in the legacy ErrorCode field.
    @Test
    public void testCollapseSeparators() {
        Assertions.assertEquals("", AuditEvent.collapseSeparators(null));
        Assertions.assertEquals("", AuditEvent.collapseSeparators(""));
        Assertions.assertEquals("Access denied for user 'u'",
                AuditEvent.collapseSeparators("Access denied for user 'u'"));
        Assertions.assertEquals("denied forged State=OK",
                AuditEvent.collapseSeparators("denied\n|forged|State=OK"));
    }

    @Test
    public void testBuilderNormalizesErrorMessage() {
        AuditEvent event = new AuditEvent.AuditEventBuilder()
                .setErrorMessage("line one\nline two")
                .build();
        Assertions.assertEquals("line one line two", event.errorMessage);
    }
}
