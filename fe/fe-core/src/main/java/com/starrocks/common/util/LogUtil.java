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

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlAuthPacket;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;

import java.lang.management.ThreadInfo;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
                .setFeIp(FrontendOptions.getLocalHostAddress())
                .setDb(authPacket == null ? "null" : authPacket.getDb())
                .setState(ctx.getState().toString())
                .setErrorCode(ctx.getState().getErrorMessage());
        GlobalStateMgr.getCurrentState().getAuditEventProcessor().handleAuditEvent(builder.build());

        QueryDetail queryDetail = new QueryDetail();
        queryDetail.setQueryId(DebugUtil.printId(UUIDUtil.genUUID()));
        queryDetail.setState(ctx.getState().isError() ?
                QueryDetail.QueryMemState.FAILED : QueryDetail.QueryMemState.FINISHED);
        queryDetail.setUser(authPacket == null ? "null" : authPacket.getUser());
        queryDetail.setRemoteIP(ctx.getRemoteIP());
        queryDetail.setDatabase(authPacket == null ? "null" : authPacket.getDb());
        queryDetail.setErrorMessage(ctx.getState().getErrorMessage());
        queryDetail.setCatalog(ctx.getCurrentCatalog());
        QueryDetailQueue.addQueryDetail(queryDetail);
    }

    public static List<String> getCurrentStackTraceToList(int trimHeadLevels, int reserveLevels) {
        return getStackTraceToList(Thread.currentThread(), trimHeadLevels, reserveLevels);
    }

    public static JsonArray getCurrentStackTraceToJsonArray(int trimHeadLevels, int reserveLevels) {
        return strListToJsonArray(getCurrentStackTraceToList(trimHeadLevels, reserveLevels));
    }

    public static List<String> getCurrentStackTraceToList() {
        // no trim, reserve all levels
        return getStackTraceToList(Thread.currentThread(), 0, 100);
    }

    public static JsonArray getCurrentStackTraceToJsonArray() {
        return strListToJsonArray(getCurrentStackTraceToList());
    }

    // Trim `trimHeadLevels` stack frames from head,
    // then reserve `reserveLevels` of the remained stack frames.
    public static List<String> getStackTraceToList(Thread thread, int trimHeadLevels, int reserveLevels) {
        return getStackTraceToList(thread.getStackTrace(), trimHeadLevels, reserveLevels);
    }

    public static JsonArray getStackTraceToJsonArray(Thread thread, int trimHeadLevels, int reserveLevels) {
        return strListToJsonArray(getStackTraceToList(thread, trimHeadLevels, reserveLevels));
    }

    public static JsonArray getStackTraceToJsonArray(ThreadInfo threadInfo, int trimHeadLevels, int reserveLevels) {
        return strListToJsonArray(getStackTraceToList(threadInfo.getStackTrace(), trimHeadLevels, reserveLevels));
    }

    public static String getCurrentStackTrace() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .map(stack -> "        " + stack.toString())
                .collect(Collectors.joining(System.lineSeparator(), System.lineSeparator(), ""));
    }

    private static List<String> getStackTraceToList(StackTraceElement[] stackTraceElements,
                                                    int trimHeadLevels,
                                                    int reserveLevels) {
        Preconditions.checkState(trimHeadLevels >= 0);
        Preconditions.checkState(reserveLevels > 0);
        List<String> stackTrace = Arrays.stream(stackTraceElements)
                .map(StackTraceElement::toString)
                .collect(Collectors.toList());
        if (stackTrace.size() <= trimHeadLevels) {
            return Collections.singletonList("all stack frames trimmed, trimHeadLevels: " + trimHeadLevels);
        } else {
            int toIndex = Math.min(trimHeadLevels + reserveLevels, stackTrace.size());
            // slice list
            return stackTrace.subList(trimHeadLevels, toIndex);
        }
    }

    private static JsonArray strListToJsonArray(List<String> stackTrace) {
        JsonArray jsonArray = new JsonArray();
        stackTrace.forEach(jsonArray::add);
        return jsonArray;
    }

    public static String dumpThread(Thread t, int reserveLevels) {
        JsonObject info = new JsonObject();
        info.addProperty("id", t.getId());
        info.addProperty("name", t.getName());
        info.add("stack", getStackTraceToJsonArray(t, 0, reserveLevels));

        return info.toString();
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

    public static List<Throwable> unwindException(Throwable e, int maxDepth) {
        List<Throwable> result = new ArrayList<>();
        for (int i = 0; i < maxDepth; i++) {
            if (e == null) {
                break;
            }
            boolean skip = true;
            Throwable parent = null;
            if (e instanceof InvocationTargetException) {
                parent = ((InvocationTargetException) e).getTargetException();
            } else {
                parent = e.getCause();
                // A layer contributes nothing beyond its cause when either:
                //  - it has no message of its own (e.g. `new Foo(null, cause)`), or
                //  - its message is just the JDK default `Throwable(Throwable cause)` ctor's
                //    `cause.toString()` (e.g. ErrorReport.wrapWithRuntimeException's bare
                //    `new RuntimeException(ddlException)`, used purely to smuggle a checked
                //    exception past a signature that can't declare one).
                // Unpeel such layers instead of showing the same text twice.
                skip = parent != null && (e.getMessage() == null || Objects.equals(e.getMessage(), parent.toString()));
            }
            if (!skip) {
                result.add(e);
            }
            if (parent == e) {
                break;
            }
            e = parent;
        }
        return result;
    }

    public static String getUnwoundExceptionMessage(Throwable e) {
        final int maxDepth = 20;
        List<Throwable> unwound = unwindException(e, maxDepth);
        // Some call sites re-wrap an exception into a different type purely for classification
        // purposes (e.g. `new AnalysisException(semanticException.getMessage(), semanticException)`),
        // carrying the same message forward unchanged. Collapse those adjacent duplicates so they
        // don't show up twice, and so a message that was never really "unwound" doesn't get a
        // class-name prefix it never had before.
        List<Throwable> result = new ArrayList<>();
        String prevMsg = null;
        for (Throwable t : unwound) {
            String msg = t.getMessage();
            if (result.isEmpty() || !Objects.equals(msg, prevMsg)) {
                result.add(t);
            }
            prevMsg = msg;
        }
        if (result.isEmpty()) {
            return e.getMessage();
        }
        if (result.size() == 1) {
            // e itself may have been unpeeled away as a content-free wrapper; report the surviving
            // layer's own message rather than the original (possibly now-irrelevant) top-level message.
            return result.get(0).getMessage();
        }
        // The outermost layer is usually a generic internal wrapper (e.g. StarRocksConnectorException,
        // AnalysisException) whose class name carries no value to the user; only deeper layers reveal
        // which underlying system actually failed, so only those get a class-name prefix.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < result.size(); i++) {
            Throwable t = result.get(i);
            String msg = t.getMessage();
            if (i > 0) {
                sb.append(t.getClass().getSimpleName());
                if (msg != null && !msg.isEmpty()) {
                    sb.append(": ");
                }
            }
            if (msg != null) {
                sb.append(msg);
            }
            sb.append("\n");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
