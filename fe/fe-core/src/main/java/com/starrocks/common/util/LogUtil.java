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

import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlAuthPacket;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
import com.starrocks.server.GlobalStateMgr;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LogUtil {

    public static void logConnectionInfoToAuditLogAndQueryQueue(ConnectContext ctx, MysqlAuthPacket authPacket) {
        boolean enableConnectionLog = false;
        if (Config.audit_log_modules != null) {
            for (String module : Config.audit_log_modules) {
                if ("connection".equals(module)) {
                    enableConnectionLog = true;
                    break;
                }
            }
        }
        if (!enableConnectionLog) {
            return;
        }
        AuditEvent.AuditEventBuilder builder = new AuditEvent.AuditEventBuilder()
                .setEventType(AuditEvent.EventType.CONNECTION)
                .setUser(authPacket == null ? "null" : authPacket.getUser())
                .setAuthorizedUser(ctx.getCurrentUserIdentity() == null
                        ? "null" : ctx.getCurrentUserIdentity().toString())
                .setClientIp(ctx.getMysqlChannel().getRemoteHostPortString())
                .setDb(authPacket == null ? "null" : authPacket.getDb())
                .setState(ctx.getState().toString())
                .setErrorCode(ctx.getState().getErrorMessage());
        GlobalStateMgr.getCurrentAuditEventProcessor().handleAuditEvent(builder.build());

        QueryDetail queryDetail = new QueryDetail();
        queryDetail.setQueryId(DebugUtil.printId(UUIDUtil.genUUID()));
        queryDetail.setState(ctx.getState().isError() ?
                QueryDetail.QueryMemState.FAILED : QueryDetail.QueryMemState.FINISHED);
        queryDetail.setUser(authPacket == null ? "null" : authPacket.getUser());
        queryDetail.setRemoteIP(ctx.getRemoteIP());
        queryDetail.setDatabase(authPacket == null ? "null" : authPacket.getDb());
        queryDetail.setErrorMessage(ctx.getState().getErrorMessage());
        QueryDetailQueue.addAndRemoveTimeoutQueryDetail(queryDetail);
    }

    public static List<String> getCurrentStackTraceToList() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .map(StackTraceElement::toString)
                .collect(Collectors.toList());
    }

    public static String getCurrentStackTrace() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .map(stack -> "        " + stack.toString())
                .collect(Collectors.joining(System.lineSeparator(), System.lineSeparator(), ""));
    }

    // just remove redundant spaces, tabs and line separators
    public static String removeLineSeparator(String origStmt) {
        char inStringStart = '-';

        StringBuilder sb = new StringBuilder();

        int idx = 0;
        int length = origStmt.length();
        while (idx < length) {
            char character = origStmt.charAt(idx);

            if (character == '\"' || character == '\'' || character == '`') {
                // process quote string
                inStringStart = character;
                appendChar(sb, inStringStart);
                idx++;
                while (idx < length && ((origStmt.charAt(idx) != inStringStart) || origStmt.charAt(idx - 1) == '\\')) {
                    sb.append(origStmt.charAt(idx));
                    ++idx;
                }
                sb.append(inStringStart);
            } else if ((character == '-' && idx != length - 1 && origStmt.charAt(idx + 1) == '-') ||
                    character == '#') {
                // process comment style like '-- comment' or '# comment'
                while (idx < length - 1 && origStmt.charAt(idx) != '\n') {
                    appendChar(sb, origStmt.charAt(idx));
                    ++idx;
                }
                appendChar(sb, '\n');
            } else if (character == '/' && idx != origStmt.length() - 1 && origStmt.charAt(idx + 1) == '*') {
                //  process comment style like '/* comment */' or hint
                while (idx < length - 1 && (origStmt.charAt(idx) != '*' || origStmt.charAt(idx + 1) != '/')) {
                    appendChar(sb, origStmt.charAt(idx));
                    idx++;
                }
                appendChar(sb, '*');
                appendChar(sb, '/');
                idx++;
            } else if (character == '\t' || character == '\r' || character == '\n') {
                // replace line separator
                appendChar(sb, ' ');
            } else {
                // append normal character
                appendChar(sb, character);
            }

            idx++;
        }
        return sb.toString();
    }

    private static void appendChar(StringBuilder sb, char character) {
        if (character != ' ') {
            sb.append(character);
        } else if (sb.length() > 0 && sb.charAt(sb.length() - 1) != ' ') {
            sb.append(" ");
        }
    }
}
