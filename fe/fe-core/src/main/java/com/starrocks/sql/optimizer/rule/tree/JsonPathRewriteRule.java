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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.BottomUpScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.SubfieldAccessPathNormalizer;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * JsonPathRewriteRule rewrites JSON function calls to column access paths for better performance.
 *
 * Example transformation:
 * get_json_string(c1, '$.f1') = 1
 * =>
 * c1.f1 = 1
 *
 * This rule supports the following JSON functions:
 * - get_json_string
 * - get_json_int  
 * - get_json_double
 * - get_json_bool
 */
public class JsonPathRewriteRule implements TreeRewriteRule {

    private static final Logger LOG = LogManager.getLogger(JsonPathRewriteRule.class);
    private static final int DEFAULT_JSON_FLATTEN_DEPTH = 20;
    private static final Pattern JSON_PATH_VALID_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    private static final Set<String> SUPPORTED_JSON_FUNCTIONS = Set.of(
            FunctionSet.GET_JSON_STRING,
            FunctionSet.GET_JSON_INT,
            FunctionSet.GET_JSON_DOUBLE,
            FunctionSet.GET_JSON_BOOL
    );

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable variables = taskContext.getOptimizerContext().getSessionVariable();
        if (!variables.isEnableJSONV2Rewrite() || variables.isCboUseDBLock()) {
            return root;
        }

        ColumnRefFactory columnRefFactory = taskContext.getOptimizerContext().getColumnRefFactory();
        try {
            JsonPathRewriteVisitor visitor = new JsonPathRewriteVisitor(columnRefFactory);
            return root.getOp().accept(visitor, root, null);
        } catch (Exception e) {
            LOG.warn("Failed to rewrite JSON paths in expression: {}", root, e);
            return root;
        }
    }

    /**
     * Context for managing JSON path rewrites and column mappings.
     */
    public static class JsonPathRewriteContext {
        // Maps full paths (tableId.columnName.field) to column references
        private final Map<String, ColumnRefOperator> pathMap = Maps.newHashMap();
        // Records newly created extended columns for scan operators
        private final Map<ColumnRefOperator, Column> extendedColumns = Maps.newHashMap();
        private final ColumnRefFactory columnRefFactory;

        public JsonPathRewriteContext(ColumnRefFactory factory) {
            this.columnRefFactory = factory;
        }

        public ColumnRefFactory getColumnRefFactory() {
            return columnRefFactory;
        }

        public Map<ColumnRefOperator, Column> getExtendedColumns() {
            return extendedColumns;
        }

        /**
         * Gets or creates a column reference for a JSON path.
         *
         * @param jsonColumn The base JSON column
         * @param jsonPath   The JSON access path
         * @return Pair of (isExisting, columnRef) where isExisting indicates if the column already existed
         */
        public Pair<Boolean, ColumnRefOperator> getOrCreateColumn(ColumnRefOperator jsonColumn,
                                                                  ColumnAccessPath jsonPath) {
            Pair<Table, Column> tableAndColumn = columnRefFactory.getTableAndColumn(jsonColumn);
            Preconditions.checkState(tableAndColumn != null,
                    "ColumnRefOperator %s must be attached to a table", jsonColumn);
            String path = jsonPath.getLinearPath();
            String fullPath = tableAndColumn.first.getId() + "." + path;

            ColumnRefOperator existingColumn = pathMap.get(fullPath);
            if (existingColumn != null) {
                if (jsonPath.getValueType().equals(existingColumn.getType())) {
                    // If the existing column matches the type, return it
                    return Pair.create(true, existingColumn);
                } else {
                    throw new IllegalArgumentException("unsupported mixed json path type: "
                            + jsonPath.getValueType() + " and  " + existingColumn.getType());
                }
            }

            // Create new column in table metadata
            Column extendedColumn = createExtendedColumn(tableAndColumn.first, path, jsonPath);

            // Create a ref
            ColumnRefOperator newColumnRef = columnRefFactory.create(path, jsonPath.getValueType(), true);
            columnRefFactory.updateColumnRefToColumns(newColumnRef, extendedColumn, tableAndColumn.first);
            pathMap.put(fullPath, newColumnRef);

            // Record the newly created extended column
            extendedColumns.put(newColumnRef, extendedColumn);

            return Pair.create(false, newColumnRef);
        }

        private Column createExtendedColumn(Table table, String path, ColumnAccessPath jsonPath) {
            if (!table.containColumn(path)) {
                // TODO: support LakeTable
                Preconditions.checkState(table instanceof OlapTable && !(table instanceof LakeTable),
                        "Only OlapTable supports dynamic column addition");
                // NOTE: The safety of adding a column dynamically is ensured by the fact that
                // this rule is only applied during query planning, thus the Table here is already copied for the
                // query. So this change would not affect the original table schema.
                Column extendedColumn = new Column(path, jsonPath.getValueType(), true);
                table.addColumn(extendedColumn);
                return extendedColumn;
            } else {
                return table.getColumn(path);
            }
        }

        public static ColumnAccessPath pathFromColumn(Column column) {
            String name = column.getName();
            ColumnAccessPath res = ColumnAccessPath.createFromLinearPath(name, column.getType());
            res.setExtended(true);
            return res;
        }
    }

    /**
     * Visitor for traversing and rewriting OptExpressions.
     */
    private static class JsonPathRewriteVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final ColumnRefFactory columnRefFactory;

        public JsonPathRewriteVisitor(ColumnRefFactory factory) {
            this.columnRefFactory = factory;
        }

        @Override
        public OptExpression visit(OptExpression optExpr, Void context) {
            List<OptExpression> newInputs = new ArrayList<>();
            for (OptExpression input : optExpr.getInputs()) {
                OptExpression child = input.getOp().accept(this, input, context);
                newInputs.add(child);
            }
            return OptExpression.builder().with(optExpr).setInputs(newInputs).build();
        }

        private Map<ColumnRefOperator, ScalarOperator> rewriteProjections(
                Map<ColumnRefOperator, ScalarOperator> originalProjections,
                JsonPathRewriteContext context,
                JsonPathExpressionRewriter rewriter) {
            Map<ColumnRefOperator, ScalarOperator> rewritten = Maps.newHashMap();

            for (var entry : originalProjections.entrySet()) {
                ScalarOperator rewrittenExpr = rewriteScalar(entry.getValue(), context, rewriter);
                if (!rewrittenExpr.equals(entry.getValue())) {
                    rewritten.put(entry.getKey(), rewrittenExpr);
                } else {
                    // Keep original expression if no change
                    rewritten.put(entry.getKey(), entry.getValue());
                }
            }

            return rewritten;
        }

        @Override
        public OptExpression visitPhysicalOlapScan(OptExpression optExpr, Void v) {
            return rewritePhysicalScan(optExpr, v);
        }

        /**
         * Rewrites PhysicalScanOperator to handle JSON path access.
         */
        private OptExpression rewritePhysicalScan(OptExpression optExpr, Void v) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpr.getOp();
            PhysicalScanOperator.Builder builder =
                    (PhysicalScanOperator.Builder) OperatorBuilderFactory.build(scanOperator)
                            .withOperator(scanOperator);

            JsonPathRewriteContext context = new JsonPathRewriteContext(columnRefFactory);
            JsonPathExpressionRewriter rewriter = new JsonPathExpressionRewriter(context);

            // Rewrite predicate
            builder.setPredicate(rewriteScalar(scanOperator.getPredicate(), context, rewriter));

            // Rewrite projection if exists
            if (scanOperator.getProjection() != null) {
                Map<ColumnRefOperator, ScalarOperator> mapping = Maps.newHashMap();
                for (var entry : scanOperator.getProjection().getColumnRefMap().entrySet()) {
                    mapping.put(entry.getKey(), rewriteScalar(entry.getValue(), context, rewriter));
                }
                builder.getProjection().getColumnRefMap().putAll(mapping);
            }

            if (MapUtils.isNotEmpty(rewriter.getExtendedColumns())) {
                // Add extended columns to scan operator
                Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
                colRefToColumnMetaMap.putAll(scanOperator.getColRefToColumnMetaMap());
                colRefToColumnMetaMap.putAll(rewriter.getExtendedColumns());
                builder.setColRefToColumnMetaMap(colRefToColumnMetaMap);

                // Add access paths
                List<ColumnAccessPath> paths = Lists.newArrayList();
                for (var entry : rewriter.getExtendedColumns().entrySet()) {
                    ColumnAccessPath path = JsonPathRewriteContext.pathFromColumn(entry.getValue());
                    paths.add(path);
                }
                builder.addColumnAccessPaths(paths);
            }

            Operator newOp = builder.build();
            return OptExpression.builder().with(optExpr).setOp(newOp).build();
        }
    }

    private static ScalarOperator rewriteScalar(ScalarOperator scalar,
                                                JsonPathRewriteContext context,
                                                JsonPathExpressionRewriter rewriter) {
        if (scalar == null) {
            return null;
        }
        ScalarOperatorRewriter scalarOperatorRewriter = new ScalarOperatorRewriter();
        return scalarOperatorRewriter.rewrite(scalar, Arrays.asList(rewriter));
    }

    /**
     * Rewrites JSON function calls to column access expressions.
     */
    private static class JsonPathExpressionRewriter extends BottomUpScalarOperatorRewriteRule {
        private final JsonPathRewriteContext context;

        public JsonPathExpressionRewriter(JsonPathRewriteContext context) {
            this.context = context;
        }

        public Map<ColumnRefOperator, Column> getExtendedColumns() {
            return context.getExtendedColumns();
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext rewriteContext) {
            if (isSupportedJsonFunction(call)) {
                return rewriteJsonFunction(call, rewriteContext);
            }
            return call;
        }

        private boolean isSupportedJsonFunction(CallOperator call) {
            return SUPPORTED_JSON_FUNCTIONS.contains(call.getFnName()) && call.getArguments().size() == 2;
        }

        private ScalarOperator rewriteJsonFunction(CallOperator call, ScalarOperatorRewriteContext rewriteContext) {
            ScalarOperator jsonColumn = call.getArguments().get(0);
            ScalarOperator pathArg = call.getArguments().get(1);

            if (!(pathArg instanceof ConstantOperator) || !(jsonColumn instanceof ColumnRefOperator)) {
                return call;
            }

            String path = ((ConstantOperator) pathArg).getVarchar();
            List<String> fields = parseJsonPath(path);

            if (fields == null) {
                return call; // Path was truncated, cannot rewrite
            }

            if (!isValidJsonPath(fields)) {
                return call;
            }

            return createColumnAccessExpression((ColumnRefOperator) jsonColumn, fields, call.getType());
        }

        private ScalarOperator createColumnAccessExpression(ColumnRefOperator jsonColumn,
                                                            List<String> fields,
                                                            Type resultType) {
            Pair<Table, Column> tableAndColumn = context.getColumnRefFactory().getTableAndColumn(jsonColumn);
            if (tableAndColumn == null) {
                return jsonColumn; // Cannot rewrite if not attached to a table
            }

            // Build full path: columnName.field1.field2
            List<String> fullPath = Lists.newArrayList();
            fullPath.add(tableAndColumn.second.getName());
            fullPath.addAll(fields);

            ColumnAccessPath accessPath = ColumnAccessPath.createLinearPath(fullPath, resultType);
            Pair<Boolean, ColumnRefOperator> columnResult = context.getOrCreateColumn(jsonColumn, accessPath);

            // Note: extendedColumns are now automatically recorded in context.getOrCreateColumn()
            return columnResult.second;
        }

        /**
         * Parses a JSON path like $.f1.f2 into ["f1", "f2"].
         * Returns null if the path was truncated due to exceeding depth limit.
         */
        private static List<String> parseJsonPath(String path) {
            List<String> result = Lists.newArrayList();
            boolean wasTruncated =
                    SubfieldAccessPathNormalizer.parseSimpleJsonPath(DEFAULT_JSON_FLATTEN_DEPTH, path, result);
            // If the path was truncated, return null to prevent incorrect rewriting
            if (wasTruncated) {
                return null;
            }
            return result;
        }

        /**
         * Validates if the JSON path contains only supported field names.
         */
        private static boolean isValidJsonPath(List<String> jsonPath) {
            if (CollectionUtils.isEmpty(jsonPath)) {
                return false;
            }

            return jsonPath.stream().allMatch(field ->
                    JSON_PATH_VALID_PATTERN.matcher(field).matches());
        }
    }
}
