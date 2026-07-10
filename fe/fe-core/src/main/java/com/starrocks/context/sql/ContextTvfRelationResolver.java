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

package com.starrocks.context.sql;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.context.ContextInternalTables;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.ContextReadExecutor;
import com.starrocks.context.ContextSqlSupport;
import com.starrocks.context.SnapshotResolver;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import com.starrocks.context.retrieval.ContextPacker;
import com.starrocks.context.retrieval.ContextScopeResolver;
import com.starrocks.context.retrieval.ContextSearchExecutor;
import com.starrocks.context.retrieval.ReferenceExpander;
import com.starrocks.context.retrieval.TextSearchExecutor;
import com.starrocks.context.retrieval.VectorSearchExecutor;
import com.starrocks.context.service.ContextQueryService;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Resolves semantic-context TVFs into analyzer-time relations. Read/dump style TVFs still
 * materialize to ValuesRelation, while text_search/vector_search rewrite to FE-built subqueries so
 * the outer statement plans and executes in one pass.
 */
public class ContextTvfRelationResolver {

    private static final Gson GSON = new Gson();
    private static final Pattern TEXT_SEARCH_MATCH_FRIENDLY = Pattern.compile("^[A-Za-z0-9_\\-]+$");
    private static final Pattern TEXT_TOKEN_DELIMITER = Pattern.compile("[\\s\\p{Punct}]+");
    // Cap synthesized SQL preview length on parse-failure logs so a runaway input doesn't bloat
    // the logfile. Successful synth-SQL emits at DEBUG so production traffic is silent.
    private static final int SYNTH_SQL_LOG_PREVIEW_CHARS = 256;

    private static final List<String> CONTEXT_GET_COLUMNS = Arrays.asList(
            "id", "entity_key", "entity_type", "title", "body", "preview", "raw_markdown",
            "version", "updated_time", "created_time", "snapshot_version",
            "source", "deleted");
    private static final List<Type> CONTEXT_GET_TYPES = Arrays.asList(
            IntegerType.BIGINT, StringType.STRING, StringType.STRING, StringType.STRING,
            StringType.STRING, StringType.STRING, StringType.STRING, IntegerType.BIGINT,
            StringType.STRING, StringType.STRING, IntegerType.BIGINT,
            StringType.STRING, BooleanType.BOOLEAN);

    private static final List<String> ENTITY_HISTORY_COLUMNS = Arrays.asList(
            "id", "version", "snapshot_version", "updated_time", "deleted", "preview", "confidence");
    private static final List<Type> ENTITY_HISTORY_TYPES = Arrays.asList(
            IntegerType.BIGINT, IntegerType.BIGINT, IntegerType.BIGINT, StringType.STRING,
            BooleanType.BOOLEAN, StringType.STRING, FloatType.DOUBLE);

    private static final List<String> READ_COLLECTION_COLUMNS = Arrays.asList(
            "id", "version", "entity_key", "entity_type", "contextbase_id", "collection_id",
            "title", "preview", "body", "raw_markdown", "frontmatter_json", "source",
            "confidence", "created_time", "updated_time", "commit_time", "snapshot_version",
            "deleted");
    private static final List<Type> READ_COLLECTION_TYPES = Arrays.asList(
            IntegerType.BIGINT, IntegerType.BIGINT, StringType.STRING, StringType.STRING,
            IntegerType.BIGINT, IntegerType.BIGINT, StringType.STRING, StringType.STRING,
            StringType.STRING, StringType.STRING, StringType.STRING, StringType.STRING,
            FloatType.DOUBLE, StringType.STRING, StringType.STRING, StringType.STRING,
            IntegerType.BIGINT, BooleanType.BOOLEAN);

    private static final List<String> READ_CONTEXTBASE_COLUMNS = READ_COLLECTION_COLUMNS;
    private static final List<Type> READ_CONTEXTBASE_TYPES = READ_COLLECTION_TYPES;

    private static final List<String> TEXT_SEARCH_COLUMNS = Arrays.asList(
            "id", "entity_key", "entity_type", "version", "snapshot_version", "preview",
            "confidence", "hit_count", "text_score", "top_snippet",
            "snippet_fragment_kind", "line_start", "line_end");
    private static final List<Type> TEXT_SEARCH_TYPES = Arrays.asList(
            IntegerType.BIGINT, StringType.STRING, StringType.STRING, IntegerType.BIGINT,
            IntegerType.BIGINT, StringType.STRING, FloatType.DOUBLE,
            IntegerType.INT, FloatType.DOUBLE, StringType.STRING, StringType.STRING,
            IntegerType.INT, IntegerType.INT);

    private static final List<String> VECTOR_SEARCH_COLUMNS = Arrays.asList(
            "id", "entity_key", "entity_type", "preview", "version", "snapshot_version",
            "confidence", "vector_score", "matched_fragment_kind",
            "matched_snippet");
    private static final List<Type> VECTOR_SEARCH_TYPES = Arrays.asList(
            IntegerType.BIGINT, StringType.STRING, StringType.STRING, StringType.STRING,
            IntegerType.BIGINT, IntegerType.BIGINT, FloatType.DOUBLE,
            FloatType.DOUBLE, StringType.STRING, StringType.STRING);

    private static final List<String> CONTEXT_SEARCH_COLUMNS = Arrays.asList(
            "id", "entity_key", "entity_type", "title", "preview", "version",
            "snapshot_version", "final_score", "text_score",
            "vector_score", "graph_score", "hop_count", "edge_types", "snippet");
    private static final List<Type> CONTEXT_SEARCH_TYPES = Arrays.asList(
            IntegerType.BIGINT, StringType.STRING, StringType.STRING, StringType.STRING,
            StringType.STRING, IntegerType.BIGINT, IntegerType.BIGINT,
            FloatType.DOUBLE, FloatType.DOUBLE, FloatType.DOUBLE, FloatType.DOUBLE,
            IntegerType.INT, StringType.STRING, StringType.STRING);

    private static final List<String> GRAPH_EXPAND_COLUMNS = Arrays.asList(
            "seed_id", "id", "entity_key", "hop", "path_score", "edge_types",
            "path_meta", "snapshot_version");
    private static final List<Type> GRAPH_EXPAND_TYPES = Arrays.asList(
            IntegerType.BIGINT, IntegerType.BIGINT, StringType.STRING, IntegerType.INT,
            FloatType.DOUBLE, StringType.STRING, StringType.STRING, IntegerType.BIGINT);

    private static final List<String> CONTEXT_PACK_COLUMNS = Arrays.asList(
            "packed_text", "used_tokens_estimate", "included_entities", "truncated_entities",
            "citations");
    private static final List<Type> CONTEXT_PACK_TYPES = Arrays.asList(
            StringType.STRING, IntegerType.BIGINT, StringType.STRING, StringType.STRING,
            StringType.STRING);

    private static final Logger LOG = LogManager.getLogger(ContextTvfRelationResolver.class);

    private final ContextMgr contextMgr;
    private final ContextReadExecutor readExecutor;
    private final TextSearchExecutor textSearchExecutor;
    private final VectorSearchExecutor vectorSearchExecutor;
    private final ReferenceExpander referenceExpander;
    private final ContextPacker contextPacker;
    private final ContextSearchExecutor contextSearchExecutor;
    private final SnapshotResolver snapshotResolver;

    public ContextTvfRelationResolver(ContextMgr contextMgr,
                                      ContextReadExecutor readExecutor,
                                      TextSearchExecutor textSearchExecutor,
                                      VectorSearchExecutor vectorSearchExecutor,
                                      ReferenceExpander referenceExpander,
                                      ContextPacker contextPacker,
                                      ContextSearchExecutor contextSearchExecutor,
                                      SnapshotResolver snapshotResolver) {
        this.contextMgr = contextMgr;
        this.readExecutor = readExecutor;
        this.textSearchExecutor = textSearchExecutor;
        this.vectorSearchExecutor = vectorSearchExecutor;
        this.referenceExpander = referenceExpander;
        this.contextPacker = contextPacker;
        this.contextSearchExecutor = contextSearchExecutor;
        this.snapshotResolver = snapshotResolver;
    }

    /**
     * Cheap name-only check used by the analyzer to decide whether to spin up a full resolver.
     * The set must stay in sync with the switch in {@link #resolve(TableFunctionRelation)}.
     * Without this guard every non-context TVF call (unnest, generate_series, files, ...) used
     * to allocate a resolver and do 8 GlobalStateMgr lookups before returning null.
     */
    public static boolean isContextTvf(String functionName) {
        if (functionName == null) {
            return false;
        }
        switch (functionName.toLowerCase(java.util.Locale.ROOT)) {
            case "context_get":
            case "entity_history":
            case "read_collection":
            case "read_contextbase":
            case "text_search":
            case "vector_search":
            case "context_search":
            case "graph_expand":
            case "context_pack":
                return true;
            default:
                return false;
        }
    }

    public Relation resolve(TableFunctionRelation relation) {
        String fn = relation.getFunctionName().getFunction().toLowerCase(java.util.Locale.ROOT);
        switch (fn) {
            case "context_get":
                return resolveContextGet(relation);
            case "entity_history":
                return resolveEntityHistory(relation);
            case "read_collection":
                return resolveReadCollection(relation);
            case "read_contextbase":
                return resolveReadContextBase(relation);
            case "text_search":
                return resolveTextSearch(relation);
            case "vector_search":
                return resolveVectorSearch(relation);
            case "context_search":
                return resolveContextSearch(relation);
            case "graph_expand":
                return resolveGraphExpand(relation);
            case "context_pack":
                return resolveContextPack(relation);
            default:
                return null;
        }
    }

    private Relation resolveContextGet(TableFunctionRelation relation) {
        ContextQueryService queryService = new ContextQueryService(contextMgr, readExecutor);
        ContextQueryService.ReadRequest request = new ContextQueryService.ReadRequest();
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("context_get", args.keySet(), setOf(
                    "id", "entity_id", "entity_key", "contextbase", "collection", "version",
                    "as_of_time", "level", "neighbor_limit", "options"));
            request.id = longArgOrNull(args.containsKey("id") ? args.get("id") : args.get("entity_id"));
            request.entityKey = stringArg(args, "entity_key");
            request.contextBase = stringArg(args, "contextbase");
            request.collection = stringArg(args, "collection");
            request.version = longArgOrNull(args.get("version"));
            request.asOfTime = stringArg(args, "as_of_time");
            request.level = ContextReadExecutor.DisclosureLevel.parse(stringArg(args, "level"));
            request.neighborLimit = intArg(args, "neighbor_limit", 16);
            request.options = stringArg(args, "options");
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() == 1) {
                if (args.get(0) instanceof IntLiteral) {
                    request.id = ((IntLiteral) args.get(0)).getLongValue();
                } else if (args.get(0) instanceof StringLiteral) {
                    request.entityKey = ((StringLiteral) args.get(0)).getValue();
                } else {
                    throw unsupportedLiteral("context_get", args.get(0));
                }
            } else {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "context_get expects 1 positional argument or named arguments");
            }
        }
        // Auth gate: resolve the contextbase from the actual entity rather than trust the
        // user-supplied `contextbase` argument — otherwise a caller with USAGE on cb_public
        // could pass id=42 (belonging to cb_secret) and read across boundaries.
        if (request.id != null) {
            requireUsageOnEntityId(request.id);
        } else if (!Strings.isNullOrEmpty(request.contextBase)) {
            requireUsageOnContextBase(request.contextBase);
        } else {
            requireUsageOnContextBaseId(0L);
        }
        ContextQueryService.ReadResult result = queryService.read(request);
        List<List<Expr>> rows = new ArrayList<>();
        if (result.row != null) {
            ContextReadExecutor.VersionRow row = result.row;
            String selectedBody = selectedBody(result.selectedLines, row.body, request.options);
            boolean lineSelection = isLineSelection(request.options);
            rows.add(Arrays.asList(
                    longExpr(row.entityId),
                    stringExpr(row.entityKey),
                    stringExpr(row.entityType),
                    stringExpr(row.title),
                    stringExpr(selectedBody),
                    stringExpr(row.preview),
                    stringExpr(lineSelection ? selectedBody : row.effectiveRawMarkdown()),
                    longExpr(row.version),
                    stringExpr(row.updatedTime),
                    stringExpr(row.createdTime),
                    longExpr(row.snapshotVersion),
                    stringExpr(row.sourceJson),
                    boolExpr(row.deleted)));
        }
        return toValuesRelation(relation, CONTEXT_GET_COLUMNS, CONTEXT_GET_TYPES, rows);
    }

    private Relation resolveEntityHistory(TableFunctionRelation relation) {
        Long entityId;
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("entity_history", args.keySet(), setOf("id", "entity_id"));
            entityId = longArgOrNull(args.containsKey("id") ? args.get("id") : args.get("entity_id"));
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() != 1 || !(args.get(0) instanceof IntLiteral)) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "entity_history expects one BIGINT id");
            }
            entityId = ((IntLiteral) args.get(0)).getLongValue();
        }
        if (entityId == null) {
            requireUsageOnContextBaseId(0L);
        } else {
            requireUsageOnEntityId(entityId);
        }
        JsonArray history = readExecutor.getHistory(entityId == null ? -1L : entityId);
        List<List<Expr>> rows = new ArrayList<>();
        for (JsonElement el : history) {
            JsonArray data = el.getAsJsonObject().getAsJsonArray("data");
            rows.add(Arrays.asList(
                    longExpr(data.get(0).isJsonNull() ? null : data.get(0).getAsLong()),
                    longExpr(data.get(1).isJsonNull() ? null : data.get(1).getAsLong()),
                    longExpr(data.get(2).isJsonNull() ? null : data.get(2).getAsLong()),
                    stringExpr(data.get(3).isJsonNull() ? null : data.get(3).getAsString()),
                    boolExpr(com.starrocks.context.ContextJsonUtil.parseBool(data.get(4))),
                    stringExpr(data.get(5).isJsonNull() ? null : data.get(5).getAsString()),
                    doubleExpr(data.get(6).isJsonNull() ? null : data.get(6).getAsDouble())));
        }
        return toValuesRelation(relation, ENTITY_HISTORY_COLUMNS, ENTITY_HISTORY_TYPES, rows);
    }

    private Relation resolveReadCollection(TableFunctionRelation relation) {
        long collectionId;
        long snapshotFence = -1L;
        int limit = 1000;
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("read_collection", args.keySet(), setOf(
                    "collection_id", "id", "snapshot_version", "as_of_time", "limit"));
            collectionId = requireLong(args.containsKey("collection_id") ? args.get("collection_id") : args.get("id"),
                    "collection_id");
            if (args.containsKey("snapshot_version") || args.containsKey("as_of_time")) {
                snapshotFence = resolveSnapshotFence(/*contextBaseId=*/0L, args);
            }
            limit = intArg(args, "limit", 1000);
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() != 1 || !(args.get(0) instanceof IntLiteral)) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT, "read_collection expects one BIGINT id");
            }
            collectionId = ((IntLiteral) args.get(0)).getLongValue();
        }
        requireUsageOnCollectionId(collectionId);
        return toSubqueryRelation(relation, READ_COLLECTION_COLUMNS, READ_COLLECTION_TYPES,
                buildReadCollectionSql(collectionId, snapshotFence, limit));
    }

    private Relation resolveReadContextBase(TableFunctionRelation relation) {
        long contextBaseId;
        long snapshotFence = -1L;
        int limit = 2000;
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("read_contextbase", args.keySet(), setOf(
                    "contextbase_id", "id", "snapshot_version", "as_of_time", "limit"));
            contextBaseId = requireLong(args.containsKey("contextbase_id") ? args.get("contextbase_id") : args.get("id"),
                    "contextbase_id");
            if (args.containsKey("snapshot_version") || args.containsKey("as_of_time")) {
                snapshotFence = resolveSnapshotFence(contextBaseId, args);
            }
            limit = intArg(args, "limit", 2000);
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() != 1 || !(args.get(0) instanceof IntLiteral)) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "read_contextbase expects one BIGINT id");
            }
            contextBaseId = ((IntLiteral) args.get(0)).getLongValue();
        }
        requireUsageOnContextBaseId(contextBaseId);
        return toSubqueryRelation(relation, READ_CONTEXTBASE_COLUMNS, READ_CONTEXTBASE_TYPES,
                buildReadContextBaseSql(contextBaseId, snapshotFence, limit));
    }

    // Builds the same SQL shape that ContextReadExecutor.readCollection issues, but with two
    // adjustments so the result is suitable for inlining as a SubqueryRelation:
    //  1) frontmatter_json / source_json are JSON-typed in storage; cast to STRING to honor the
    //     READ_COLLECTION_TYPES contract (positions 10, 11 declared StringType.STRING).
    //  2) the column at position 11 is exposed as `source` (matches READ_COLLECTION_COLUMNS).
    // REST endpoints (ContextReadCollectionAction) keep using ContextReadExecutor.readCollection
    // directly; this builder is only for the TVF resolver path.
    private String buildReadCollectionSql(long collectionId, long snapshotFence, int limit) {
        return buildReadEntitiesSql("collection_id", collectionId, snapshotFence, limit);
    }

    private String buildReadContextBaseSql(long contextBaseId, long snapshotFence, int limit) {
        return buildReadEntitiesSql("contextbase_id", contextBaseId, snapshotFence, limit);
    }

    private String buildReadEntitiesSql(String scopeColumn, long scopeId, long snapshotFence, int limit) {
        int safeLimit = Math.max(1, limit);
        String heads = ContextInternalTables.DATABASE + "." + ContextInternalTables.HEADS;
        String versions = ContextInternalTables.DATABASE + "." + ContextInternalTables.VERSIONS;
        StringBuilder sql = new StringBuilder();
        if (snapshotFence < 0) {
            sql.append("SELECT v.entity_id AS id, v.version, v.entity_key, v.entity_type, ")
                    .append("v.contextbase_id, v.collection_id, v.title, v.preview, v.body, ")
                    .append("v.raw_markdown, ")
                    .append("CAST(v.frontmatter_json AS STRING) AS frontmatter_json, ")
                    .append("CAST(v.source_json AS STRING) AS source, ")
                    .append("v.confidence, v.created_time, v.updated_time, v.commit_time, ")
                    .append("v.snapshot_version, v.deleted ")
                    .append("FROM ").append(heads).append(" h JOIN ").append(versions).append(" v ")
                    .append("ON h.entity_id = v.entity_id AND h.current_version = v.version ")
                    .append("WHERE h.").append(scopeColumn).append(" = ").append(scopeId).append(' ')
                    .append("ORDER BY h.current_snapshot_version DESC LIMIT ").append(safeLimit);
        } else {
            sql.append("SELECT v.entity_id AS id, v.version, v.entity_key, v.entity_type, ")
                    .append("v.contextbase_id, v.collection_id, v.title, v.preview, v.body, ")
                    .append("v.raw_markdown, ")
                    .append("CAST(v.frontmatter_json AS STRING) AS frontmatter_json, ")
                    .append("CAST(v.source_json AS STRING) AS source, ")
                    .append("v.confidence, v.created_time, v.updated_time, v.commit_time, ")
                    .append("v.snapshot_version, v.deleted ")
                    .append("FROM ").append(versions).append(" v ")
                    .append("JOIN (SELECT entity_id, MAX(version) AS max_version FROM ").append(versions)
                    .append(" WHERE ").append(scopeColumn).append(" = ").append(scopeId)
                    .append(" AND snapshot_version <= ").append(snapshotFence)
                    .append(" GROUP BY entity_id) t ")
                    .append("ON v.entity_id = t.entity_id AND v.version = t.max_version ")
                    .append("LEFT JOIN ").append(heads).append(" h ON h.entity_id = v.entity_id ")
                    .append("WHERE v.").append(scopeColumn).append(" = ").append(scopeId).append(' ')
                    .append("LIMIT ").append(safeLimit);
        }
        return sql.toString();
    }

    private Relation resolveTextSearch(TableFunctionRelation relation) {
        TextSearchExecutor.Request request = new TextSearchExecutor.Request();
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("text_search", args.keySet(), setOf(
                    "scope", "contextbase", "collection", "collections", "collection_type",
                    "pattern", "options", "entity_type",
                    "confidence_min", "limit", "offset", "as_of_time", "snapshot_version"));
            String scope = stringArg(args, "scope");
            String contextBase = stringArg(args, "contextbase");
            String collection = stringArg(args, "collection");
            ContextScopeResolver.ResolvedScope resolved =
                    ContextScopeResolver.resolve(contextMgr, scope, contextBase, collection,
                            stringListArg(args.get("collections")), stringArg(args, "collection_type"));
            request.contextBaseId = resolved.contextBaseId;
            request.collectionId = resolved.collectionId;
            request.collectionIds = resolved.collectionIds;
            request.pattern = requiredStringArg(args, "pattern", "text_search");
            request.entityType = stringArg(args, "entity_type");
            request.confidenceMin = doubleArg(args, "confidence_min");
            request.maxResults = intArg(args, "limit", 10);
            request.offset = intArg(args, "offset", 0);
            applyTextOptions(request, stringArg(args, "options"));
            request.snapshotFence = resolveSnapshotFence(resolved.contextBaseId, args);
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() != 2) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "text_search expects 2 positional arguments");
            }
            request.pattern = requireStringLiteral(args.get(1), "pattern");
            if (args.get(0) instanceof IntLiteral) {
                request.contextBaseId = ((IntLiteral) args.get(0)).getLongValue();
            } else if (args.get(0) instanceof StringLiteral) {
                ContextScopeResolver.ResolvedScope resolved = ContextScopeResolver.resolve(
                        contextMgr, ((StringLiteral) args.get(0)).getValue(), null, null);
                request.contextBaseId = resolved.contextBaseId;
                request.collectionId = resolved.collectionId;
            } else {
                throw unsupportedLiteral("text_search", args.get(0));
            }
        }
        requireUsageOnContextBaseId(request.contextBaseId);
        if (Strings.isNullOrEmpty(request.pattern)) {
            return toEmptySubqueryRelation(relation, TEXT_SEARCH_COLUMNS, TEXT_SEARCH_TYPES);
        }
        return toSubqueryRelation(relation, TEXT_SEARCH_COLUMNS, TEXT_SEARCH_TYPES,
                buildTextSearchSql(request));
    }

    private Relation resolveVectorSearch(TableFunctionRelation relation) {
        VectorSearchExecutor.Request request = new VectorSearchExecutor.Request();
        request.allowStaleVector = false;
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("vector_search", args.keySet(), setOf(
                    "scope", "contextbase", "collection", "collections", "collection_type",
                    "query_text", "query_embedding", "options",
                    "entity_type", "confidence_min", "limit", "offset", "as_of_time",
                    "snapshot_version", "allow_stale_vector"));
            ContextScopeResolver.ResolvedScope resolved = ContextScopeResolver.resolve(
                    contextMgr, stringArg(args, "scope"), stringArg(args, "contextbase"),
                    stringArg(args, "collection"), stringListArg(args.get("collections")),
                    stringArg(args, "collection_type"));
            request.contextBaseId = resolved.contextBaseId;
            request.collectionId = resolved.collectionId;
            request.collectionIds = resolved.collectionIds;
            request.queryText = stringArg(args, "query_text");
            request.queryEmbedding = floatArrayArg(args.get("query_embedding"));
            request.entityType = stringArg(args, "entity_type");
            request.confidenceMin = doubleArg(args, "confidence_min");
            request.maxResults = intArg(args, "limit", 10);
            request.offset = intArg(args, "offset", 0);
            request.allowStaleVector = boolArg(args, "allow_stale_vector", false);
            applyVectorOptions(request, stringArg(args, "options"));
            request.snapshotFence = resolveSnapshotFence(resolved.contextBaseId, args);
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() != 2) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "vector_search expects 2 positional arguments");
            }
            request.queryText = requireStringLiteral(args.get(1), "query_text");
            if (args.get(0) instanceof IntLiteral) {
                request.contextBaseId = ((IntLiteral) args.get(0)).getLongValue();
            } else if (args.get(0) instanceof StringLiteral) {
                ContextScopeResolver.ResolvedScope resolved = ContextScopeResolver.resolve(
                        contextMgr, ((StringLiteral) args.get(0)).getValue(), null, null);
                request.contextBaseId = resolved.contextBaseId;
                request.collectionId = resolved.collectionId;
            } else {
                throw unsupportedLiteral("vector_search", args.get(0));
            }
        }
        if (Strings.isNullOrEmpty(request.queryText)
                && (request.queryEmbedding == null || request.queryEmbedding.length == 0)) {
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    "vector_search requires query_text or query_embedding");
        }
        requireUsageOnContextBaseId(request.contextBaseId);
        float[] queryEmbedding = vectorSearchExecutor.resolveQueryEmbedding(request);
        if (queryEmbedding == null || queryEmbedding.length == 0) {
            return toEmptySubqueryRelation(relation, VECTOR_SEARCH_COLUMNS, VECTOR_SEARCH_TYPES);
        }
        return toSubqueryRelation(relation, VECTOR_SEARCH_COLUMNS, VECTOR_SEARCH_TYPES,
                buildVectorSearchSql(request, queryEmbedding));
    }

    private Relation resolveContextSearch(TableFunctionRelation relation) {
        ContextSearchExecutor.Request request = new ContextSearchExecutor.Request();
        long snapshotFence;
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("context_search", args.keySet(), setOf(
                    "scope", "contextbase", "collection", "collections", "collection_type",
                    "query_text", "query_embedding",
                    "seed_ids", "entity_type", "max_results", "max_tokens", "graph_mode",
                    "text_weight", "vector_weight", "graph_weight", "graph_depth", "max_depth",
                    "graph_seed_topk", "max_frontier", "edge_types", "direction", "as_of_time",
                    "snapshot_version", "allow_stale_vector", "workspace", "filters", "consistency",
                    "retrieval_profile", "graph_strategy"));
            ContextScopeResolver.ResolvedScope resolved = ContextScopeResolver.resolve(
                    contextMgr, stringArg(args, "scope"), stringArg(args, "contextbase"),
                    stringArg(args, "collection"), stringListArg(args.get("collections")),
                    stringArg(args, "collection_type"));
            request.contextBase = resolved.contextBase;
            request.collection = resolved.collection;
            request.contextBaseIdOverride = resolved.contextBaseId;
            request.collectionIdOverride = resolved.collectionId;
            request.collectionIdsOverride = resolved.collectionIds;
            request.queryText = stringArg(args, "query_text");
            request.queryEmbedding = floatArrayArg(args.get("query_embedding"));
            request.allowStaleVector = boolArg(args, "allow_stale_vector", true);
            request.seedIds = longListArg(args.get("seed_ids"));
            request.entityType = stringArg(args, "entity_type");
            request.maxResults = intArg(args, "max_results", 20);
            request.maxTokens = intArg(args, "max_tokens", 4000);
            request.graphMode = parseGraphMode(stringArg(args, "graph_mode"));
            request.textWeight = doubleArg(args, "text_weight", ContextSearchExecutor.DEFAULT_TEXT_WEIGHT);
            request.explicitTextWeight = args.containsKey("text_weight");
            request.vectorWeight = doubleArg(args, "vector_weight", ContextSearchExecutor.DEFAULT_VECTOR_WEIGHT);
            request.explicitVectorWeight = args.containsKey("vector_weight");
            request.graphWeight = doubleArg(args, "graph_weight", ContextSearchExecutor.DEFAULT_GRAPH_WEIGHT);
            request.explicitGraphWeight = args.containsKey("graph_weight");
            request.graphDepth = intArg(args, args.containsKey("max_depth") ? "max_depth" : "graph_depth", 2);
            request.maxFrontier = intArg(args, "max_frontier", 200);
            request.graphSeedTopK = intArg(args, "graph_seed_topk", 0);
            request.graphStrategy = stringArg(args, "graph_strategy");
            request.explicitGraphStrategy = args.containsKey("graph_strategy");
            request.edgeTypes = stringListArg(args.get("edge_types"));
            request.direction = args.containsKey("direction")
                    ? parseDirection(stringArg(args, "direction"))
                    : ContextSearchExecutor.defaultGraphDirection();
            request.workspace = stringArg(args, "workspace");
            request.retrievalProfile = stringArg(args, "retrieval_profile");
            request.consistency = stringArg(args, "consistency");
            snapshotFence = resolveSnapshotFence(resolved.contextBaseId, args);
            request.snapshotVersion = snapshotFence >= 0 ? snapshotFence : null;
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() != 2) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "context_search expects 2 positional arguments");
            }
            request.queryText = requireStringLiteral(args.get(1), "query_text");
            request.allowStaleVector = true;
            snapshotFence = -1L;
            if (args.get(0) instanceof IntLiteral) {
                request.contextBaseIdOverride = ((IntLiteral) args.get(0)).getLongValue();
            } else if (args.get(0) instanceof StringLiteral) {
                ContextScopeResolver.ResolvedScope resolved = ContextScopeResolver.resolve(
                        contextMgr, ((StringLiteral) args.get(0)).getValue(), null, null);
                request.contextBase = resolved.contextBase;
                request.collection = resolved.collection;
                request.contextBaseIdOverride = resolved.contextBaseId;
                request.collectionIdOverride = resolved.collectionId;
            } else {
                throw unsupportedLiteral("context_search", args.get(0));
            }
        }
        // The override field gates the per-base ACL check before we run the actual search.
        requireUsageOnContextBaseId(request.contextBaseIdOverride);
        ContextSearchExecutor.Result result = contextSearchExecutor.search(request);
        snapshotFence = result.explain.get("snapshot_fence") instanceof Number
                ? ((Number) result.explain.get("snapshot_fence")).longValue() : snapshotFence;
        List<Long> ids = new ArrayList<>(result.candidates.size());
        for (ContextSearchExecutor.Candidate candidate : result.candidates) {
            ids.add(candidate.entityId);
        }
        Map<Long, ContextReadExecutor.EntityMeta> metaById = readExecutor.loadEntityMetadata(ids, snapshotFence);
        List<List<Expr>> rows = new ArrayList<>();
        for (ContextSearchExecutor.Candidate candidate : result.candidates) {
            ContextReadExecutor.EntityMeta meta = metaById.get(candidate.entityId);
            rows.add(Arrays.asList(
                    longExpr(candidate.entityId),
                    stringExpr(meta == null ? null : meta.entityKey),
                    stringExpr(meta == null ? null : meta.entityType),
                    stringExpr(meta == null ? null : meta.title),
                    stringExpr(meta == null ? null : meta.preview),
                    longExpr(meta == null ? null : meta.version),
                    longExpr(meta == null ? null : meta.snapshotVersion),
                    doubleExpr(candidate.finalScore),
                    doubleExpr(candidate.textScore),
                    doubleExpr(candidate.vectorScore),
                    doubleExpr(candidate.graphScore),
                    intExpr(candidate.hopCount),
                    stringExpr(json(candidate.edgeTypes)),
                    stringExpr(candidate.snippet)));
        }
        return toValuesRelation(relation, CONTEXT_SEARCH_COLUMNS, CONTEXT_SEARCH_TYPES, rows);
    }

    private Relation resolveGraphExpand(TableFunctionRelation relation) {
        ReferenceExpander.Request request = new ReferenceExpander.Request();
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("graph_expand", args.keySet(), setOf(
                    "scope", "contextbase", "collection", "collections", "collection_type",
                    "seed_ids", "seeds", "direction",
                    "depth", "max_depth", "edge_types", "max_frontier", "require_complete",
                    "as_of_time", "snapshot_version"));
            ContextScopeResolver.ResolvedScope resolved = ContextScopeResolver.resolve(
                    contextMgr, stringArg(args, "scope"), stringArg(args, "contextbase"),
                    stringArg(args, "collection"), stringListArg(args.get("collections")),
                    stringArg(args, "collection_type"));
            request.contextBaseId = resolved.contextBaseId;
            request.collectionId = resolved.collectionId;
            request.collectionIds = resolved.collectionIds;
            request.seeds = longListArg(args.containsKey("seed_ids") ? args.get("seed_ids") : args.get("seeds"));
            request.direction = parseDirection(stringArg(args, "direction"));
            request.depth = intArg(args, args.containsKey("max_depth") ? "max_depth" : "depth", 1);
            request.refKinds = stringListArg(args.get("edge_types"));
            request.maxFrontier = intArg(args, "max_frontier", 200);
            request.requireComplete = boolArg(args, "require_complete", false);
            request.snapshotFence = resolveSnapshotFence(resolved.contextBaseId, args);
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() != 2) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "graph_expand expects 2 positional arguments");
            }
            request.seeds = Collections.singletonList(requireLongLiteral(args.get(0), "seed_id"));
            request.depth = requireIntLiteral(args.get(1), "depth");
        }
        // Positional form skips ContextScopeResolver, so contextBaseId may be null/0 — resolve
        // via the first seed's contextbase membership. This matches the context_get pattern:
        // gate on the actual record's owner, not on user-supplied scope.
        long graphContextBaseId = request.contextBaseId == null ? 0L : request.contextBaseId;
        graphContextBaseId = resolveAuthorizedContextBaseIdForEntityIds(
                request.seeds, graphContextBaseId <= 0 ? null : graphContextBaseId, "seed_ids");
        if (graphContextBaseId > 0) {
            request.contextBaseId = graphContextBaseId;
        }
        ReferenceExpander.Result result = referenceExpander.expand(request);
        List<Long> ids = new ArrayList<>(result.rows.size());
        for (ReferenceExpander.ExpansionRow row : result.rows) {
            ids.add(row.entityId);
        }
        Map<Long, ContextReadExecutor.EntityMeta> metaById = readExecutor.loadEntityMetadata(ids, request.snapshotFence);
        List<List<Expr>> rows = new ArrayList<>();
        for (ReferenceExpander.ExpansionRow row : result.rows) {
            ContextReadExecutor.EntityMeta meta = metaById.get(row.entityId);
            Map<String, Object> pathMeta = new LinkedHashMap<>();
            pathMeta.put("seed_id", row.seedId);
            pathMeta.put("hop", row.hop);
            pathMeta.put("edge_types", row.refKinds);
            rows.add(Arrays.asList(
                    longExpr(row.seedId),
                    longExpr(row.entityId),
                    stringExpr(meta == null ? null : meta.entityKey),
                    intExpr(row.hop),
                    doubleExpr(row.pathScore),
                    stringExpr(json(row.refKinds)),
                    stringExpr(json(pathMeta)),
                    longExpr(meta == null ? null : meta.snapshotVersion)));
        }
        return toValuesRelation(relation, GRAPH_EXPAND_COLUMNS, GRAPH_EXPAND_TYPES, rows);
    }

    private Relation resolveContextPack(TableFunctionRelation relation) {
        ContextPacker.Request request = new ContextPacker.Request();
        long packContextBaseId = 0L;
        if (relation.getFunctionParams().hasNamedArguments()) {
            Map<String, Expr> args = namedArgs(relation.getFunctionParams());
            validateAllowedArgs("context_pack", args.keySet(), setOf(
                    "scope", "contextbase", "collection", "collections", "collection_type",
                    "entity_ids", "max_tokens", "include_citations"));
            String scope = stringArg(args, "scope");
            String contextBase = stringArg(args, "contextbase");
            String collection = stringArg(args, "collection");
            request.entityIds = longListArg(args.get("entity_ids"));
            if ((request.entityIds == null || request.entityIds.isEmpty())
                    && (!Strings.isNullOrEmpty(scope) || !Strings.isNullOrEmpty(contextBase))) {
                ContextScopeResolver.ResolvedScope resolved =
                        ContextScopeResolver.resolve(contextMgr, scope, contextBase, collection,
                                stringListArg(args.get("collections")), stringArg(args, "collection_type"));
                packContextBaseId = resolved.contextBaseId;
                request.entityIds = listPackEntityIds(resolved.contextBaseId, resolved.collectionIds, 128);
            }
            request.maxTokens = intArg(args, "max_tokens", 4000);
            request.includeCitations = boolArg(args, "include_citations", true);
        } else {
            List<Expr> args = relation.getFunctionParams().exprs();
            if (args.size() != 2) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        "context_pack expects 2 positional arguments");
            }
            packContextBaseId = requireLongLiteral(args.get(0), "contextbase_id");
            request.entityIds = listPackEntityIds(packContextBaseId, null, 128);
            request.maxTokens = requireIntLiteral(args.get(1), "max_tokens");
            request.includeCitations = true;
        }
        // Gate on the resolved contextbase if we have one; otherwise derive the scope from the
        // requested entity_ids and require the full list to stay inside one contextbase.
        packContextBaseId = resolveAuthorizedContextBaseIdForEntityIds(
                request.entityIds, packContextBaseId <= 0 ? null : packContextBaseId, "entity_ids");
        ContextPacker.Result result = contextPacker.pack(request);
        List<List<Expr>> rows = Collections.singletonList(Arrays.asList(
                stringExpr(result.packedText),
                longExpr((long) result.usedTokensEstimate),
                stringExpr(json(result.includedEntities)),
                stringExpr(json(result.truncatedEntities)),
                stringExpr(json(result.citations))));
        return toValuesRelation(relation, CONTEXT_PACK_COLUMNS, CONTEXT_PACK_TYPES, rows);
    }

    private ValuesRelation toValuesRelation(TableFunctionRelation source,
                                            List<String> defaultColumns,
                                            List<Type> outputTypes,
                                            List<List<Expr>> rows) {
        List<String> columns = defaultColumns;
        if (source.getColumnOutputNames() != null) {
            if (source.getColumnOutputNames().size() != defaultColumns.size()) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        source.getFunctionName().getFunction() + " expects " + defaultColumns.size()
                                + " output columns, got aliases for " + source.getColumnOutputNames().size());
            }
            columns = source.getColumnOutputNames();
        }
        ValuesRelation relation = new ValuesRelation(rows, columns, outputTypes);
        relation.setAlias(source.getAlias());
        return relation;
    }

    private Relation toSubqueryRelation(TableFunctionRelation source,
                                        List<String> defaultColumns,
                                        List<Type> outputTypes,
                                        String sql) {
        if (source.getColumnOutputNames() != null
                && source.getColumnOutputNames().size() != defaultColumns.size()) {
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    source.getFunctionName().getFunction() + " expects " + defaultColumns.size()
                            + " output columns, got aliases for " + source.getColumnOutputNames().size());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("[context-tvf-debug] {} synth SQL: {}",
                    source.getFunctionName().getFunction(), sql);
        }
        StatementBase parsed;
        try {
            parsed = SqlParser.parseFirstStatement(sql, SqlModeHelper.MODE_DEFAULT);
        } catch (Exception e) {
            String preview = sql.length() > SYNTH_SQL_LOG_PREVIEW_CHARS
                    ? sql.substring(0, SYNTH_SQL_LOG_PREVIEW_CHARS) + "...[truncated]"
                    : sql;
            LOG.warn("[context-tvf-debug] parse failed for {}: {} | sql={}",
                    source.getFunctionName().getFunction(), e.getMessage(), preview);
            throw e;
        }
        if (!(parsed instanceof QueryStatement)) {
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    source.getFunctionName().getFunction() + " rewrite must produce a query statement");
        }
        SubqueryRelation relation = new SubqueryRelation((QueryStatement) parsed, false, source.getPos());
        relation.setAlias(source.getAlias());
        relation.setColumnOutputNames(source.getColumnOutputNames());
        relation.setCreateByPolicyRewritten(true);
        return relation;
    }

    private Relation toEmptySubqueryRelation(TableFunctionRelation source,
                                             List<String> defaultColumns,
                                             List<Type> outputTypes) {
        return toSubqueryRelation(source, defaultColumns, outputTypes,
                buildEmptySubquerySql(defaultColumns, outputTypes));
    }

    private String buildTextSearchSql(TextSearchExecutor.Request request) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT id, entity_key, entity_type, version, snapshot_version, preview, ")
                .append("confidence, hit_count, text_score, top_snippet, ")
                .append("snippet_fragment_kind, line_start, line_end FROM (")
                .append("SELECT id, entity_key, entity_type, version, snapshot_version, preview, ")
                .append("confidence, ")
                .append("COUNT(*) OVER (PARTITION BY id) AS hit_count, ")
                .append("LEAST(1.0, COUNT(*) OVER (PARTITION BY id) / 10.0) AS text_score, ");
        if (request.filenamesOnly || request.countOnly) {
            sql.append("CAST(NULL AS STRING) AS top_snippet, ")
                    .append("CAST(NULL AS STRING) AS snippet_fragment_kind, ")
                    .append("CAST(NULL AS INT) AS line_start, CAST(NULL AS INT) AS line_end, ");
        } else {
            sql.append("snippet AS top_snippet, fragment_kind AS snippet_fragment_kind, ")
                    .append("line_start, line_end, ");
        }
        sql.append("ROW_NUMBER() OVER (PARTITION BY id ORDER BY ")
                .append("CASE WHEN LOWER(fragment_kind) = 'section' THEN 0 ELSE 1 END, fragment_id) ")
                .append("AS entity_rank FROM (")
                .append(buildTextSearchBaseSql(request))
                .append(") text_fragment_hits) text_entity_hits WHERE entity_rank = 1 ")
                .append("ORDER BY text_score DESC, id ");
        appendLimitClause(sql, request.offset, request.maxResults);
        return sql.toString();
    }

    private String buildTextSearchBaseSql(TextSearchExecutor.Request request) {
        String versions = ContextInternalTables.DATABASE + "." + ContextInternalTables.VERSIONS;
        String snippetExpr = buildTextSnippetExpr(request, "v");
        StringBuilder sql = new StringBuilder();
        if (request.snapshotFence < 0) {
            // BE rejects MATCH that shares a WHERE with predicates from a JOINed table
            // ("Match can only used as a pushdown predicate on column with GIN in a single
            // query."). Push the MATCH/LIKE predicate into an inner single-table scan on
            // fragments, then JOIN heads on the outer level so the heads filters land on a
            // separate scan node.
            String fragmentPredicate = buildTextSearchPredicate(request).replace("f.fragment_text", "fragment_text")
                    .replace("LOWER(f.fragment_text)", "LOWER(fragment_text)");
            sql.append("SELECT h.entity_id AS id, h.entity_key, h.entity_type, ")
                    .append("h.current_version AS version, h.current_snapshot_version AS snapshot_version, ")
                    .append("h.current_preview AS preview, ")
                    .append("h.current_confidence AS confidence, f.fragment_id, f.fragment_kind, ")
                    .append("f.line_start, f.line_end, ")
                    .append(snippetExpr).append(" AS snippet ")
                    .append("FROM (SELECT entity_id, version, fragment_id, fragment_kind, ")
                    .append("line_start, line_end, fragment_preview, fragment_text FROM ")
                    .append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.FRAGMENTS)
                    .append(" WHERE ").append(fragmentPredicate)
                    .append(" LIMIT ").append(Math.max(1, request.maxFragmentScan))
                    .append(") f ")
                    .append("JOIN ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.HEADS).append(" h ")
                    .append("ON h.entity_id = f.entity_id AND h.current_version = f.version ");
            // Only LEFT JOIN versions when snippet expansion needs v.raw_markdown / v.body —
            // otherwise the join is dead code and triggers BE primary-key lookup deserialization
            // failures on the JSON columns of __internal_context.context_entity_versions.
            if (hasSnippetExpansion(request)) {
                sql.append("LEFT JOIN ").append(versions).append(" v ")
                        .append("ON h.entity_id = v.entity_id AND h.current_version = v.version ");
            }
            sql.append("WHERE h.current_deleted = false ");
            appendScopeFilters(sql, "h.contextbase_id", request.contextBaseId,
                    "h.collection_id", request.collectionId, request.collectionIds);
            if (!Strings.isNullOrEmpty(request.entityType)) {
                sql.append("AND h.entity_type = ").append(sqlStringLiteral(request.entityType)).append(' ');
            }
            if (request.confidenceMin != null) {
                sql.append("AND h.current_confidence >= ").append(request.confidenceMin).append(' ');
            }
        } else {
            sql.append("SELECT v.entity_id AS id, v.entity_key, v.entity_type, v.version, ")
                    .append("v.snapshot_version, v.preview, ")
                    .append("v.confidence, f.fragment_id, f.fragment_kind, ")
                    .append("f.line_start, f.line_end, ")
                    .append(snippetExpr).append(" AS snippet ")
                    .append("FROM ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.FRAGMENTS).append(" f ")
                    .append("JOIN (SELECT entity_id, MAX(version) AS av FROM ")
                    .append(versions).append(" WHERE snapshot_version <= ")
                    .append(request.snapshotFence);
            appendScopeFilters(sql, "contextbase_id", request.contextBaseId,
                    "collection_id", request.collectionId, request.collectionIds);
            sql.append(" GROUP BY entity_id) av ")
                    .append("ON av.entity_id = f.entity_id AND av.av = f.version ")
                    .append("JOIN ").append(versions).append(" v ")
                    .append("ON v.entity_id = f.entity_id AND v.version = f.version ")
                    .append("LEFT JOIN ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.HEADS).append(" h ON h.entity_id = v.entity_id ")
                    .append("WHERE ").append(buildTextSearchPredicate(request)).append(' ')
                    .append("AND v.deleted = false ");
            if (!Strings.isNullOrEmpty(request.entityType)) {
                sql.append("AND v.entity_type = ").append(sqlStringLiteral(request.entityType)).append(' ');
            }
            if (request.confidenceMin != null) {
                sql.append("AND v.confidence >= ").append(request.confidenceMin).append(' ');
            }
        }
        sql.append("LIMIT ").append(Math.max(0, request.maxFragmentScan));
        return sql.toString();
    }

    private String buildTextSearchPredicate(TextSearchExecutor.Request request) {
        String trimmed = request.pattern.trim();
        List<String> validTokens = new ArrayList<>();
        for (String token : TEXT_TOKEN_DELIMITER.split(trimmed)) {
            if (!token.isEmpty() && TEXT_SEARCH_MATCH_FRIENDLY.matcher(token).matches()) {
                validTokens.add(token);
            }
        }
        if (request.caseInsensitive) {
            String escaped = trimmed.replace("%", "\\\\%");
            return "LOWER(f.fragment_text) LIKE " + sqlStringLiteral("%" + escaped.toLowerCase() + "%");
        }
        if (validTokens.size() == 1) {
            return "f.fragment_text MATCH " + sqlStringLiteral(validTokens.get(0).toLowerCase());
        }
        if (validTokens.size() > 1) {
            StringBuilder predicate = new StringBuilder("(");
            for (int i = 0; i < validTokens.size(); i++) {
                if (i > 0) {
                    predicate.append(" OR ");
                }
                String token = validTokens.get(i).replace("%", "\\\\%");
                predicate.append("LOWER(f.fragment_text) LIKE ")
                        .append(sqlStringLiteral("%" + token.toLowerCase() + "%"));
            }
            predicate.append(')');
            return predicate.toString();
        }
        String escaped = trimmed.replace("%", "\\\\%");
        return "f.fragment_text LIKE " + sqlStringLiteral("%" + escaped + "%");
    }

    private String buildTextSnippetExpr(TextSearchExecutor.Request request, String versionAlias) {
        if (!hasSnippetExpansion(request)) {
            return "f.fragment_preview";
        }
        int before = request.contextLines != null ? request.contextLines
                : (request.beforeLines == null ? 0 : request.beforeLines);
        int after = request.contextLines != null ? request.contextLines
                : (request.afterLines == null ? 0 : request.afterLines);
        String bodyExpr = "COALESCE(" + versionAlias + ".raw_markdown, " + versionAlias + ".body, '')";
        String splitExpr = "split(" + bodyExpr + ", char(10))";
        String startExpr = "greatest(1, coalesce(f.line_start, 1) - " + before + ")";
        String endExpr = "least(cardinality(" + splitExpr + "), "
                + "coalesce(f.line_end, coalesce(f.line_start, 1)) + " + after + ")";
        String lengthExpr = "greatest(1, " + endExpr + " - " + startExpr + " + 1)";
        return "CASE WHEN f.line_start IS NULL OR f.line_end IS NULL THEN f.fragment_preview ELSE "
                + "array_join(array_slice(" + splitExpr + ", " + startExpr + ", " + lengthExpr + "), "
                + "char(10)) END";
    }

    private String buildVectorSearchSql(VectorSearchExecutor.Request request, float[] queryEmbedding) {
        int offset = Math.max(0, request.offset);
        int limit = Math.max(0, request.maxResults);
        int scanLimit = Math.max(Math.max(0, request.maxFragmentScan),
                Math.max(1, offset) + Math.max(1, limit) * 4);
        String vectorLiteral = sqlFloatArrayLiteral(queryEmbedding);
        // Default searches BOTH preview and section fragments (the per-entity fold keeps the best
        // per entity), which needs no fragment_kind filter at all -- those are the only two kinds
        // the writer emits, and a tautological IN as a scan residual would make the BE vector
        // pre-filter read and evaluate fragment_kind over the whole scan range on every query.
        // deepMode (-d) stays section-only. Consistent with VectorSearchExecutor (REST).
        String fragmentFilter = request.deepMode ? "f.fragment_kind = 'section'" : null;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT id, entity_key, entity_type, preview, version, snapshot_version, ")
                .append("confidence, (raw_score + 1.0) / 2.0 AS vector_score, ")
                .append("matched_fragment_kind, matched_snippet FROM (")
                .append("SELECT id, entity_key, entity_type, preview, version, snapshot_version, ")
                .append("confidence, raw_score, matched_fragment_kind, matched_snippet, ")
                .append("ROW_NUMBER() OVER (PARTITION BY id ORDER BY raw_score DESC) AS entity_rank ")
                .append("FROM (")
                .append(buildVectorSearchBaseSql(request, vectorLiteral, fragmentFilter, scanLimit))
                .append(") vector_fragment_hits) vector_entity_hits WHERE entity_rank = 1 ")
                .append("ORDER BY vector_score DESC, id ");
        appendLimitClause(sql, request.offset, request.maxResults);
        return sql.toString();
    }

    private String buildVectorSearchBaseSql(VectorSearchExecutor.Request request,
                                            String vectorLiteral,
                                            String fragmentFilter,
                                            int scanLimit) {
        String versions = ContextInternalTables.DATABASE + "." + ContextInternalTables.VERSIONS;
        // Inner ANN: TopN directly on the fragments scan with the SCOPE as a scan residual
        // (contextbase_id / collection_id are denormalized onto fragments and immutable per entity).
        // This lets the BE pre-filter candidate rows to the scope before the ANN search, and the
        // ORDER BY ... LIMIT sits directly on the scan (no JOIN below the limit) so the vector index
        // rewrite fires and the outer version/deleted JOIN does not disable it. Mirrors
        // VectorSearchExecutor (REST).
        StringBuilder ann = new StringBuilder();
        ann.append("SELECT f.entity_id AS entity_id, f.version AS version, ")
                .append("approx_cosine_similarity(").append(vectorLiteral)
                .append(", f.embedding) AS raw_score, ")
                .append("f.fragment_kind AS matched_fragment_kind, ")
                .append("f.fragment_preview AS matched_snippet ")
                .append("FROM ").append(ContextInternalTables.DATABASE).append('.')
                .append(ContextInternalTables.FRAGMENTS).append(" f ")
                .append("WHERE 1 = 1 ");
        if (fragmentFilter != null) {
            ann.append("AND ").append(fragmentFilter).append(' ');
        }
        appendScopeFilters(ann, "f.contextbase_id", request.contextBaseId,
                "f.collection_id", request.collectionId, request.collectionIds);
        ann.append("ORDER BY raw_score DESC LIMIT ").append(scanLimit);

        StringBuilder sql = new StringBuilder();
        if (request.snapshotFence < 0) {
            sql.append("SELECT h.entity_id AS id, h.entity_key, h.entity_type, ")
                    .append("h.current_preview AS preview, h.current_version AS version, ")
                    .append("h.current_snapshot_version AS snapshot_version, ")
                    .append("h.current_confidence AS confidence, ")
                    .append("ann.raw_score AS raw_score, ")
                    .append("ann.matched_fragment_kind AS matched_fragment_kind, ")
                    .append("ann.matched_snippet AS matched_snippet ")
                    .append("FROM (").append(ann).append(") ann ")
                    .append("JOIN ").append(ContextInternalTables.DATABASE).append('.')
                    .append(ContextInternalTables.HEADS).append(" h ")
                    .append("ON h.entity_id = ann.entity_id AND h.current_version = ann.version ")
                    .append("WHERE h.current_deleted = false ");
            if (!Strings.isNullOrEmpty(request.entityType)) {
                sql.append("AND h.entity_type = ").append(sqlStringLiteral(request.entityType)).append(' ');
            }
            if (request.confidenceMin != null) {
                sql.append("AND h.current_confidence >= ").append(request.confidenceMin).append(' ');
            }
        } else {
            sql.append("SELECT v.entity_id AS id, v.entity_key, v.entity_type, v.preview, ")
                    .append("v.version, v.snapshot_version, v.confidence, ")
                    .append("ann.raw_score AS raw_score, ")
                    .append("ann.matched_fragment_kind AS matched_fragment_kind, ")
                    .append("ann.matched_snippet AS matched_snippet ")
                    .append("FROM (").append(ann).append(") ann ")
                    .append("JOIN (SELECT entity_id, MAX(version) AS av FROM ")
                    .append(versions).append(" WHERE snapshot_version <= ")
                    .append(request.snapshotFence);
            appendScopeFilters(sql, "contextbase_id", request.contextBaseId,
                    "collection_id", request.collectionId, request.collectionIds);
            sql.append(" GROUP BY entity_id) av ")
                    .append("ON av.entity_id = ann.entity_id AND av.av = ann.version ")
                    .append("JOIN ").append(versions).append(" v ")
                    .append("ON v.entity_id = ann.entity_id AND v.version = ann.version ")
                    .append("WHERE v.deleted = false ");
            if (!Strings.isNullOrEmpty(request.entityType)) {
                sql.append("AND v.entity_type = ").append(sqlStringLiteral(request.entityType)).append(' ');
            }
            if (request.confidenceMin != null) {
                sql.append("AND v.confidence >= ").append(request.confidenceMin).append(' ');
            }
        }
        return sql.toString();
    }

    private boolean hasSnippetExpansion(TextSearchExecutor.Request request) {
        return (request.contextLines != null && request.contextLines > 0)
                || (request.beforeLines != null && request.beforeLines > 0)
                || (request.afterLines != null && request.afterLines > 0);
    }

    private String buildEmptySubquerySql(List<String> columns, List<Type> outputTypes) {
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("CAST(NULL AS ").append(sqlType(outputTypes.get(i))).append(") AS ")
                    .append(columns.get(i));
        }
        sql.append(" FROM (SELECT 1) context_tvf_empty WHERE FALSE");
        return sql.toString();
    }

    private void appendScopeFilters(StringBuilder sql,
                                    String contextBaseColumn,
                                    Long contextBaseId,
                                    String collectionColumn,
                                    Long collectionId,
                                    List<Long> collectionIds) {
        if (contextBaseId != null) {
            sql.append(" AND ").append(contextBaseColumn).append(" = ").append(contextBaseId).append(' ');
        }
        if (collectionId != null) {
            sql.append(" AND ").append(collectionColumn).append(" = ").append(collectionId).append(' ');
        } else if (collectionIds != null && !collectionIds.isEmpty()) {
            sql.append(" AND ").append(collectionColumn).append(" IN (")
                    .append(joinIds(collectionIds)).append(") ");
        }
    }

    private void appendLimitClause(StringBuilder sql, int offset, int limit) {
        int safeOffset = Math.max(0, offset);
        int safeLimit = Math.max(0, limit);
        if (safeOffset > 0) {
            sql.append("LIMIT ").append(safeOffset).append(", ").append(safeLimit);
        } else {
            sql.append("LIMIT ").append(safeLimit);
        }
    }

    private String sqlFloatArrayLiteral(float[] values) {
        StringBuilder sql = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append(Float.toString(values[i]));
        }
        sql.append(']');
        return sql.toString();
    }

    private String joinIds(List<Long> ids) {
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append(ids.get(i));
        }
        return sql.toString();
    }

    private String sqlStringLiteral(String value) {
        return "'" + escapeSqlLiteral(value) + "'";
    }

    private String escapeSqlLiteral(String value) {
        return value.replace("\\", "\\\\").replace("'", "''");
    }

    private String sqlType(Type type) {
        if (type.matchesType(IntegerType.BIGINT)) {
            return "BIGINT";
        }
        if (type.matchesType(IntegerType.INT)) {
            return "INT";
        }
        if (type.matchesType(BooleanType.BOOLEAN)) {
            return "BOOLEAN";
        }
        if (type.matchesType(FloatType.DOUBLE)) {
            return "DOUBLE";
        }
        if (type.matchesType(StringType.STRING)) {
            return "STRING";
        }
        throw new IllegalArgumentException("Unsupported TVF rewrite type: " + type);
    }

    private Map<String, Expr> namedArgs(FunctionParams params) {
        Map<String, Expr> out = new LinkedHashMap<>();
        List<String> names = params.getExprsNames();
        for (int i = 0; i < names.size(); i++) {
            out.put(names.get(i), params.exprs().get(i));
        }
        return out;
    }

    private void applyTextOptions(TextSearchExecutor.Request request, String options) {
        if (Strings.isNullOrEmpty(options)) {
            return;
        }
        String[] tokens = options.trim().split("\\s+");
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token.equals("-i")) {
                request.caseInsensitive = true;
            } else if (token.equals("-c")) {
                request.countOnly = true;
            } else if (token.equals("-l")) {
                request.filenamesOnly = true;
            } else if (token.startsWith("-A") || token.startsWith("-B") || token.startsWith("-C")) {
                String value = token.length() > 2 ? token.substring(2) : (i + 1 < tokens.length ? tokens[++i] : "");
                Integer parsed = tryParseInt(value);
                if (parsed == null) {
                    continue;
                }
                if (token.startsWith("-A")) {
                    request.afterLines = parsed;
                } else if (token.startsWith("-B")) {
                    request.beforeLines = parsed;
                } else {
                    request.contextLines = parsed;
                }
            }
        }
    }

    private void applyVectorOptions(VectorSearchExecutor.Request request, String options) {
        if (Strings.isNullOrEmpty(options)) {
            return;
        }
        for (String token : options.trim().split("\\s+")) {
            if (token.equals("-d")) {
                request.deepMode = true;
            } else if (token.equals("-l")) {
                request.idsOnly = true;
            } else if (token.equals("-f")) {
                request.includeFrontmatter = true;
            }
        }
    }

    private long resolveSnapshotFence(long contextBaseId, Map<String, Expr> args) {
        Long resolvedContextBaseId = contextBaseId > 0 ? contextBaseId : null;
        if (args.containsKey("snapshot_version")) {
            return snapshotResolver.resolveFromSelector(resolvedContextBaseId,
                    String.valueOf(requireLongLiteral(args.get("snapshot_version"), "snapshot_version")));
        }
        if (args.containsKey("as_of_time")) {
            return snapshotResolver.resolveFromSelector(resolvedContextBaseId,
                    requiredStringArg(args, "as_of_time", "snapshot selector"));
        }
        return -1L;
    }

    private List<Long> listPackEntityIds(long contextBaseId, List<Long> collectionIds, int limit) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT entity_id FROM __internal_context.context_entity_heads ")
                .append("WHERE contextbase_id = ").append(contextBaseId)
                .append(" AND current_deleted = false ");
        if (collectionIds != null && !collectionIds.isEmpty()) {
            sql.append("AND collection_id IN (");
            for (int i = 0; i < collectionIds.size(); i++) {
                if (i > 0) {
                    sql.append(',');
                }
                sql.append(collectionIds.get(i));
            }
            sql.append(") ");
        }
        sql.append("ORDER BY current_confidence DESC, entity_id LIMIT ").append(limit);
        JsonArray rows = runQuery(sql.toString());
        List<Long> ids = new ArrayList<>(rows.size());
        for (JsonElement row : rows) {
            JsonArray data = row.getAsJsonObject().getAsJsonArray("data");
            if (data.size() > 0 && !data.get(0).isJsonNull()) {
                ids.add(data.get(0).getAsLong());
            }
        }
        return ids;
    }

    long resolveAuthorizedContextBaseIdForEntityIds(Collection<Long> entityIds,
                                                    Long requestedContextBaseId,
                                                    String parameterName) {
        long effectiveContextBaseId = requestedContextBaseId == null ? 0L : requestedContextBaseId;
        if (effectiveContextBaseId > 0) {
            requireUsageOnContextBaseId(effectiveContextBaseId);
        }
        if (entityIds == null || entityIds.isEmpty()) {
            if (effectiveContextBaseId <= 0) {
                requireUsageOnContextBaseId(0L);
            }
            return effectiveContextBaseId;
        }
        for (Long entityId : entityIds) {
            if (entityId == null) {
                continue;
            }
            long entityContextBaseId = readExecutor.resolveContextBaseIdForEntity(entityId);
            if (entityContextBaseId <= 0) {
                throw new ContextException(ContextErrorCode.ACCESS_DENIED,
                        parameterName + " contains an unknown or inaccessible entity_id: " + entityId);
            }
            if (effectiveContextBaseId <= 0) {
                effectiveContextBaseId = entityContextBaseId;
                requireUsageOnContextBaseId(effectiveContextBaseId);
                continue;
            }
            if (entityContextBaseId != effectiveContextBaseId) {
                throw new ContextException(ContextErrorCode.ACCESS_DENIED,
                        parameterName + " must belong to a single contextbase");
            }
        }
        if (effectiveContextBaseId <= 0) {
            requireUsageOnContextBaseId(0L);
        }
        return effectiveContextBaseId;
    }

    private JsonArray runQuery(String sql) {
        return ContextSqlSupport.executeDql(sql);
    }

    private String selectedBody(List<String> selectedLines, String body, String options) {
        if (!isLineSelection(options)) {
            return body;
        }
        if (selectedLines == null || selectedLines.isEmpty()) {
            return "";
        }
        return String.join("\n", selectedLines);
    }

    private boolean isLineSelection(String options) {
        return !Strings.isNullOrEmpty(options) && options.startsWith("-L");
    }

    private String json(Object value) {
        return GSON.toJson(value == null ? Collections.emptyList() : value);
    }

    private void validateAllowedArgs(String fnName, Collection<String> provided, Set<String> allowed) {
        for (String name : provided) {
            if (!allowed.contains(name)) {
                throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                        fnName + " does not support parameter: " + name);
            }
        }
    }

    /**
     * Authorize a TVF call against the owning contextbase. Every semantic-context TVF
     * (context_get, entity_history, read_collection, read_contextbase, text_search,
     * vector_search, context_search, graph_expand, context_pack) goes through this gate
     * before the resolver materializes data.
     *
     * <p>Why this exists: the TVFs run during the analyzer phase and produce a
     * {@code SubqueryRelation} flagged {@code createByPolicyRewritten=true}. The default
     * privilege walker ({@code ColumnPrivilege.AccessControlChecker.visitSubqueryRelation})
     * skips policy-rewritten subqueries, and {@code AuthorizerStmtVisitor.visitTableFunction}
     * only collects {@code queryTable} catalog names. Without this gate any user with plain
     * SELECT could read every semantic-context base by calling the TVFs directly. The gate
     * mirrors the privilege model used by the write-side {@code checkContextBaseOwnership}:
     * system OPERATE / SECURITY bypass, then per-base USAGE grant, then ownership match.
     *
     * <p>The {@code contextBaseName} is resolved from the <em>actual</em> backing record
     * (entity id → heads.contextbase_id, or collection id → collections.getContextBaseId),
     * not from any user-supplied {@code contextbase} argument. Otherwise a caller could
     * claim to be reading "cb_public" but pass an entity id belonging to "cb_secret".
     */
    private void requireUsageOnContextBase(String contextBaseName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            // Internal flows (image replay, daemons) bypass the gate — they don't have a
            // user identity to check against and are already trusted.
            return;
        }
        if (contextBaseName == null || contextBaseName.isEmpty()) {
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    "context TVF requires a resolvable contextbase scope");
        }
        // 1. Admin override.
        try {
            Authorizer.checkSystemAction(ctx, PrivilegeType.OPERATE);
            return;
        } catch (AccessDeniedException ignored) {
            // fall through
        }
        try {
            Authorizer.checkSystemAction(ctx, PrivilegeType.SECURITY);
            return;
        } catch (AccessDeniedException ignored) {
            // fall through
        }
        // 2. Per-base USAGE grant.
        try {
            Authorizer.checkContextBaseAction(ctx, contextBaseName, PrivilegeType.USAGE);
            return;
        } catch (AccessDeniedException ignored) {
            // fall through to ownership
        }
        // 3. Ownership. Compare against UserIdentity.getUser() (principal/email) so the check
        // is stable for ephemeral Bearer/JWT identities across requests from different remote IPs;
        // toString() would include the per-request host and silently break the match.
        ContextMgr.ContextBaseMeta meta = contextMgr.getContextBase(contextBaseName);
        if (meta != null) {
            String owner = meta.getOwner();
            String me = ctx.getCurrentUserIdentity() == null
                    ? null : ctx.getCurrentUserIdentity().getUser();
            if (owner != null && !owner.isEmpty() && owner.equals(me)) {
                return;
            }
        }
        throw new ContextException(ContextErrorCode.ACCESS_DENIED,
                "no USAGE privilege on contextbase '" + contextBaseName + "'");
    }

    /** Resolve a contextbase name from a contextbase id and gate. Accepts boxed Long so the
     *  caller doesn't need to null-check fields like {@code request.contextBaseId} (which is
     *  {@code Long} on the search executors). Null or non-positive is treated the same as
     *  "unresolved scope" — only admins pass that gate. */
    private void requireUsageOnContextBaseId(Long contextBaseId) {
        if (contextBaseId == null || contextBaseId <= 0) {
            requireUsageOnContextBase(null);
            return;
        }
        ContextMgr.ContextBaseMeta meta = contextMgr.getContextBaseById(contextBaseId);
        requireUsageOnContextBase(meta == null ? null : meta.getName());
    }

    /** Resolve a contextbase via an entity id (heads.contextbase_id) and gate. */
    private void requireUsageOnEntityId(long entityId) {
        long cbId = readExecutor.resolveContextBaseIdForEntity(entityId);
        if (cbId <= 0) {
            // Entity does not exist or heads not materialized — fall through so the underlying
            // read returns the standard "not found" error, but only if the caller is admin.
            // Non-admin callers get a denial so we don't disclose existence info.
            requireUsageOnContextBaseId(0L);
            return;
        }
        requireUsageOnContextBaseId(cbId);
    }

    /** Resolve a contextbase via a collection id (collections.getContextBaseId) and gate. */
    private void requireUsageOnCollectionId(long collectionId) {
        ContextMgr.CollectionMeta col = contextMgr.getCollectionById(collectionId);
        if (col == null) {
            requireUsageOnContextBaseId(0L);
            return;
        }
        requireUsageOnContextBaseId(col.getContextBaseId());
    }

    private Set<String> setOf(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    private ContextSearchExecutor.GraphMode parseGraphMode(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return ContextSearchExecutor.GraphMode.AUTO;
        }
        String normalized = value.toUpperCase(java.util.Locale.ROOT);
        if ("REQUIRED".equals(normalized)) {
            // REQUIRED was removed when fusion gained auto-seed-derivation. Surface a loud signal
            // so existing programmatic callers know to drop the parameter rather than silently
            // coerce to AUTO and let the caller think strict semantics are still in force.
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    "graph_mode=REQUIRED is no longer supported; use AUTO (default) or OFF. "
                            + "For strict graph traversal, call CONTEXT_GRAPH_EXPAND with require_complete=true.");
        }
        try {
            return ContextSearchExecutor.GraphMode.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    "invalid graph_mode '" + value + "'; expected AUTO or OFF");
        }
    }

    private ReferenceExpander.Direction parseDirection(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return ReferenceExpander.Direction.FORWARD;
        }
        return ReferenceExpander.Direction.valueOf(value.toUpperCase(java.util.Locale.ROOT));
    }

    private String requiredStringArg(Map<String, Expr> args, String key, String surface) {
        String value = stringArg(args, key);
        if (Strings.isNullOrEmpty(value)) {
            throw new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                    surface + " requires " + key);
        }
        return value;
    }

    private String stringArg(Map<String, Expr> args, String key) {
        Expr expr = args.get(key);
        if (expr == null) {
            return null;
        }
        return requireStringLiteral(expr, key);
    }

    private String requireStringLiteral(Expr expr, String key) {
        if (expr instanceof StringLiteral) {
            return ((StringLiteral) expr).getValue();
        }
        throw unsupportedLiteral(key, expr);
    }

    private Long requireLongLiteral(Expr expr, String key) {
        if (expr instanceof IntLiteral) {
            return ((IntLiteral) expr).getLongValue();
        }
        throw unsupportedLiteral(key, expr);
    }

    private long requireLong(Expr expr, String key) {
        Long value = longArgOrNull(expr);
        if (value == null) {
            throw unsupportedLiteral(key, expr);
        }
        return value;
    }

    private Long longArgOrNull(Expr expr) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof IntLiteral) {
            return ((IntLiteral) expr).getLongValue();
        }
        return null;
    }

    private Integer requireIntLiteral(Expr expr, String key) {
        return requireLongLiteral(expr, key).intValue();
    }

    private int intArg(Map<String, Expr> args, String key, int defaultValue) {
        Expr expr = args.get(key);
        if (expr == null) {
            return defaultValue;
        }
        return requireIntLiteral(expr, key);
    }

    private Boolean boolArg(Map<String, Expr> args, String key, boolean defaultValue) {
        Expr expr = args.get(key);
        if (expr == null) {
            return defaultValue;
        }
        if (expr instanceof BoolLiteral) {
            return ((BoolLiteral) expr).getValue();
        }
        throw unsupportedLiteral(key, expr);
    }

    private Double doubleArg(Map<String, Expr> args, String key) {
        Expr expr = args.get(key);
        if (expr == null) {
            return null;
        }
        return requireDoubleLiteral(expr, key);
    }

    private double doubleArg(Map<String, Expr> args, String key, double defaultValue) {
        Expr expr = args.get(key);
        if (expr == null) {
            return defaultValue;
        }
        return requireDoubleLiteral(expr, key);
    }

    private Double requireDoubleLiteral(Expr expr, String key) {
        if (expr instanceof FloatLiteral) {
            return ((FloatLiteral) expr).getDoubleValue();
        }
        // A SQL fractional literal such as 0.2 is parsed as a DecimalLiteral (not a FloatLiteral),
        // so it must be accepted here too; otherwise fractional weights can only be passed via
        // scientific notation (0.2e0) or as integers.
        if (expr instanceof DecimalLiteral) {
            return ((DecimalLiteral) expr).getDoubleValue();
        }
        if (expr instanceof IntLiteral) {
            return (double) ((IntLiteral) expr).getLongValue();
        }
        throw unsupportedLiteral(key, expr);
    }

    private List<Long> longListArg(Expr expr) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof ArrayExpr) {
            List<Long> values = new ArrayList<>();
            for (Expr child : expr.getChildren()) {
                values.add(requireLongLiteral(child, "array item"));
            }
            return values;
        }
        if (expr instanceof IntLiteral) {
            return Collections.singletonList(((IntLiteral) expr).getLongValue());
        }
        throw unsupportedLiteral("array", expr);
    }

    private List<String> stringListArg(Expr expr) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof ArrayExpr) {
            List<String> values = new ArrayList<>();
            for (Expr child : expr.getChildren()) {
                values.add(requireStringLiteral(child, "array item"));
            }
            return values;
        }
        if (expr instanceof StringLiteral) {
            return Collections.singletonList(((StringLiteral) expr).getValue());
        }
        throw unsupportedLiteral("array", expr);
    }

    private float[] floatArrayArg(Expr expr) {
        if (expr == null) {
            return null;
        }
        if (!(expr instanceof ArrayExpr)) {
            throw unsupportedLiteral("query_embedding", expr);
        }
        float[] out = new float[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = expr.getChildren().get(i);
            if (child instanceof FloatLiteral) {
                out[i] = (float) ((FloatLiteral) child).getDoubleValue();
            } else if (child instanceof DecimalLiteral) {
                // Fractional embedding components such as 0.1 parse as DecimalLiteral, not FloatLiteral.
                out[i] = (float) ((DecimalLiteral) child).getDoubleValue();
            } else if (child instanceof IntLiteral) {
                out[i] = ((IntLiteral) child).getLongValue();
            } else {
                throw unsupportedLiteral("query_embedding", child);
            }
        }
        return out;
    }

    private Integer tryParseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Expr longExpr(Long value) {
        return value == null ? NullLiteral.create(IntegerType.BIGINT) : new IntLiteral(value, IntegerType.BIGINT);
    }

    private Expr intExpr(Integer value) {
        return value == null ? NullLiteral.create(IntegerType.INT) : new IntLiteral(value.longValue(), IntegerType.INT);
    }

    private Expr doubleExpr(Double value) {
        return value == null ? NullLiteral.create(FloatType.DOUBLE) : new FloatLiteral(value, FloatType.DOUBLE);
    }

    private Expr stringExpr(String value) {
        return value == null ? NullLiteral.create(StringType.STRING) : new StringLiteral(value);
    }

    private Expr boolExpr(boolean value) {
        return new BoolLiteral(value);
    }

    private ContextException unsupportedLiteral(String argName, Expr expr) {
        return new ContextException(ContextErrorCode.INVALID_ARGUMENT,
                argName + " must be a literal, got: " + expr.getClass().getSimpleName());
    }
}
