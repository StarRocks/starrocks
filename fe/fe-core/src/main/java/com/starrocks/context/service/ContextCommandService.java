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

package com.starrocks.context.service;

import com.google.common.base.Strings;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.ContextWriteExecutor;
import com.starrocks.context.markdown.MarkdownExtractor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared write-side contract service for REST and future SQL CRUD surfaces.
 */
public class ContextCommandService {

    private final ContextReadExecutor reader;
    private final ContextWriteExecutor writer;

    public ContextCommandService(ContextReadExecutor reader, ContextWriteExecutor writer) {
        this.reader = reader;
        this.writer = writer;
    }

    public ContextWriteExecutor.UpsertResult write(ContextCollectionName collection, Long entityId, String entityKey,
                                                   String body, String writeOptions,
                                                   String title, String preview, Double confidence,
                                                   Map<String, Expr> options) {
        ContextReadExecutor.VersionRow row = resolveRow(collection, entityId, entityKey);
        if (row == null) {
            throw new IllegalStateException("entity not found");
        }
        String currentBody = row.body == null ? "" : row.body;
        String updatedBody = applyWriteOptions(currentBody, body, writeOptions);
        String updatedMarkdown = MarkdownExtractor.canonicalizeRawMarkdown(
                null, updatedBody, row.frontmatterJson, row.sourceJson);
        Map<String, Expr> entityArgs = new LinkedHashMap<>();
        entityArgs.put("id", new IntLiteral(row.entityId));
        if (!Strings.isNullOrEmpty(row.entityKey)) {
            entityArgs.put("entity_key", new StringLiteral(row.entityKey));
        }
        entityArgs.put("entity_type", new StringLiteral(row.entityType));
        entityArgs.put("title", new StringLiteral(title == null ? nullToEmpty(row.title) : title));
        if (preview != null) {
            entityArgs.put("preview", new StringLiteral(preview));
        } else if (!Strings.isNullOrEmpty(row.preview)) {
            entityArgs.put("preview", new StringLiteral(row.preview));
        }
        if (confidence != null) {
            entityArgs.put("confidence", new FloatLiteral(confidence));
        } else {
            entityArgs.put("confidence", new FloatLiteral(row.confidence));
        }
        entityArgs.put("content", new StringLiteral(updatedMarkdown));
        return writer.upsert(collection, entityArgs, options);
    }

    public ContextWriteExecutor.UpsertResult deprecate(ContextCollectionName collection, Long entityId, String entityKey,
                                                       Map<String, Expr> options) {
        return write(collection, entityId, entityKey, null, null, null, null, 0.0, options);
    }

    public ContextWriteExecutor.UpsertResult delete(ContextCollectionName collection, Long entityId, String entityKey,
                                                    boolean hardDelete, Map<String, Expr> options) {
        ContextReadExecutor.VersionRow row = resolveRow(collection, entityId, entityKey);
        if (row == null) {
            throw new IllegalStateException("entity not found");
        }
        if (hardDelete) {
            return writer.hardDelete(collection, row.entityId, row.entityKey, options);
        }
        return writer.tombstone(collection, row.entityId, row.entityKey, options);
    }

    private ContextReadExecutor.VersionRow resolveRow(ContextCollectionName collection, Long entityId, String entityKey) {
        ContextMgr.CollectionMeta meta = resolveCollectionMeta(collection);
        ContextReadExecutor.VersionRow row;
        if (entityId != null && entityId > 0) {
            row = reader.loadCurrentVersionRow(entityId);
        } else {
            long resolved = resolveIdByKey(collection, entityKey);
            row = resolved > 0 ? reader.loadCurrentVersionRow(resolved) : null;
        }
        if (row == null) {
            return null;
        }
        if (meta != null && (row.contextBaseId != meta.getContextBaseId() || row.collectionId != meta.getId())) {
            return null;
        }
        return row;
    }

    private long resolveIdByKey(ContextCollectionName collection, String entityKey) {
        if (Strings.isNullOrEmpty(entityKey)) {
            return -1L;
        }
        ContextMgr.CollectionMeta meta = resolveCollectionMeta(collection);
        if (meta == null) {
            return -1L;
        }
        return reader.resolveEntityIdByKey(entityKey, meta.getContextBaseId(), meta.getId());
    }

    private ContextMgr.CollectionMeta resolveCollectionMeta(ContextCollectionName collection) {
        return GlobalStateMgr.getCurrentState().getContextMgr()
                .getCollection(collection.getContextBase(), collection.getCollection());
    }

    static String applyWriteOptions(String currentMarkdown, String body, String writeOptions) {
        String base = currentMarkdown == null ? "" : currentMarkdown;
        if (Strings.isNullOrEmpty(writeOptions)) {
            return body == null ? base : body;
        }
        if ("-a".equals(writeOptions)) {
            if (Strings.isNullOrEmpty(body)) {
                return base;
            }
            if (base.isEmpty()) {
                return body;
            }
            return base.endsWith("\n") ? base + body : base + "\n" + body;
        }
        if (writeOptions.startsWith("-L") && writeOptions.endsWith("i")) {
            int line = parsePositiveInt(writeOptions.substring(2, writeOptions.length() - 1));
            return insertBeforeLine(base, body, line);
        }
        if (writeOptions.startsWith("-L")) {
            String range = writeOptions.substring(2);
            String[] parts = range.split("-", 2);
            int start = parsePositiveInt(parts[0]);
            int end = parts.length > 1 ? parsePositiveInt(parts[1]) : start;
            return replaceLines(base, body, start, end);
        }
        return body == null ? base : body;
    }

    private static int parsePositiveInt(String raw) {
        try {
            return Math.max(1, Integer.parseInt(raw));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid line selector: " + raw);
        }
    }

    private static String insertBeforeLine(String markdown, String body, int lineNumber) {
        String[] lines = markdown.split("\n", -1);
        StringBuilder out = new StringBuilder();
        int insertIndex = Math.min(Math.max(1, lineNumber), lines.length + 1);
        for (int i = 1; i <= lines.length + 1; i++) {
            if (i == insertIndex && !Strings.isNullOrEmpty(body)) {
                out.append(body);
                if (!body.endsWith("\n")) {
                    out.append('\n');
                }
            }
            if (i <= lines.length) {
                out.append(lines[i - 1]);
                if (i < lines.length || i < lines.length + 1) {
                    out.append('\n');
                }
            }
        }
        return trimTrailingSingleNewline(out.toString());
    }

    private static String replaceLines(String markdown, String body, int startLine, int endLine) {
        String[] lines = markdown.split("\n", -1);
        int start = Math.max(1, startLine);
        int end = Math.max(start, endLine);
        StringBuilder out = new StringBuilder();
        for (int i = 1; i <= lines.length; i++) {
            if (i == start && !Strings.isNullOrEmpty(body)) {
                out.append(body);
                if (!body.endsWith("\n")) {
                    out.append('\n');
                }
            }
            if (i < start || i > end) {
                out.append(lines[i - 1]);
                if (i < lines.length) {
                    out.append('\n');
                }
            }
        }
        if (lines.length == 0 && !Strings.isNullOrEmpty(body)) {
            out.append(body);
        }
        return trimTrailingSingleNewline(out.toString());
    }

    private static String trimTrailingSingleNewline(String value) {
        if (value.endsWith("\n")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
