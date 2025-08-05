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
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * JsonPathRewriteRule rewrites a json function to a json access path
 * Example:
 * get_json_string(c1, '$.f1') = 1
 * =>
 * c1.f1 = 1
 */
public class JsonPathRewriteRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable variables = taskContext.getOptimizerContext().getSessionVariable();
        if (!variables.isEnableJSONV2Rewrite()) {
            return root;
        }
        ColumnRefFactory columnRefFactory = taskContext.getOptimizerContext().getColumnRefFactory();
        JsonPathRewriteVisitor visitor = new JsonPathRewriteVisitor(columnRefFactory);
        return root.getOp().accept(visitor, root, null);
    }

    // Context to record mapping from original JSON function expressions to rewritten expressions
    public static class JsonPathRewriteContext {

        // full path mapping: t1.c1.f1 => c1.f1
        private final Map<String, ColumnRefOperator> pathMap = Maps.newHashMap();

        private final ColumnRefFactory columnRefFactory;

        public JsonPathRewriteContext(ColumnRefFactory factory) {
            this.columnRefFactory = factory;
        }

        public ColumnRefFactory getColumnRefFactory() {
            return columnRefFactory;
        }

        public Pair<Boolean, ColumnRefOperator> getOrCreateColumn(ColumnRefOperator jsonColumn,
                                                                  ColumnAccessPath jsonPath) {
            Pair<Table, Column> tableAndColumn = columnRefFactory.getTableAndColumn(jsonColumn);
            String path = jsonPath.getLinearPath();
            String fullPath = tableAndColumn.first.getId() + "." + path;
            ColumnRefOperator res = pathMap.get(fullPath);
            if (res != null) {
                return Pair.create(true, res);
            }

            // create a column in the table
            // get_json_int(c1, '$.f1') => c1.f1
            Column extendedColumn;
            Table table = tableAndColumn.first;
            if (!table.containColumn(path)) {
                // Add the column in table metadata
                // For OlapTable, the metadata is a copied for each query, so this change will only affect current
                // query
                Preconditions.checkState(table instanceof OlapTable);
                extendedColumn = new Column(path, jsonPath.getValueType(), true);
                tableAndColumn.first.addColumn(extendedColumn);
            } else {
                extendedColumn = tableAndColumn.first.getColumn(path);
            }

            res = columnRefFactory.create(path, jsonPath.getValueType(), true);
            columnRefFactory.updateColumnRefToColumns(res, extendedColumn, tableAndColumn.first);
            pathMap.put(fullPath, res);
            return Pair.create(false, res);
        }

        public static Column pathToColumn(ColumnAccessPath path, Type valueType) {
            return new Column(path.getLinearPath(), valueType, true);
        }

        public static ColumnAccessPath pathFromColumn(Column column) {
            String name = column.getName();
            ColumnAccessPath res = ColumnAccessPath.createFromLinearPath(name, column.getType());
            res.setExtended(true);
            return res;
        }

    }

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

        //        @Override
        //        public OptExpression visitPhysicalMetaScan(OptExpression optExpr, Void v) {
        //            return rewritePhysicalScan(optExpr, v);
        //        }

        @Override
        public OptExpression visitPhysicalOlapScan(OptExpression optExpr, Void v) {
            return rewritePhysicalScan(optExpr, v);
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression optExpr, Void v) {
            Operator child = optExpr.inputAt(0).getOp();
            if (child instanceof LogicalMetaScanOperator) {
                return rewriteMetaScan(optExpr, v);
            }
            return visit(optExpr, v);
        }

        // PROJECT(get_json_string(c1, 'f1')) -> META_SCAN(c1)
        // =>
        // PROJECT(c1.f1) -> META_SCAN(c1.f1)
        private OptExpression rewriteMetaScan(OptExpression optExpr, Void v) {
            LogicalProjectOperator project = (LogicalProjectOperator) optExpr.getOp();
            LogicalMetaScanOperator metaScan = (LogicalMetaScanOperator) optExpr.inputAt(0).getOp();
            LogicalMetaScanOperator.Builder scanBuilder =
                    LogicalMetaScanOperator.builder().withOperator(metaScan);

            JsonPathRewriteContext context = new JsonPathRewriteContext(columnRefFactory);
            JsonPathRewriter rewriter = new JsonPathRewriter(context);

            // rewrite project
            Map<ColumnRefOperator, ScalarOperator> changed = Maps.newHashMap();
            for (var entry : project.getColumnRefMap().entrySet()) {
                ScalarOperator rewritten = rewriteScalar(entry.getValue(), context, rewriter);
                if (!rewritten.equals(entry.getValue())) {
                    // no change, keep the original column
                    changed.put(entry.getKey(), rewritten);
                }
            }
            Map<ColumnRefOperator, ScalarOperator> projection = Maps.newHashMap();
            projection.putAll(project.getColumnRefMap());
            projection.putAll(changed);
            LogicalProjectOperator newProject = new LogicalProjectOperator(changed);

            // add the columns into scan
            Map<ColumnRefOperator, Column> metaScanColumnMap = Maps.newHashMap();
            metaScanColumnMap.putAll(rewriter.getExtendedColumns());
            for (var entry : metaScan.getColRefToColumnMetaMap().entrySet()) {
                if (!changed.containsKey(entry.getKey())) {
                    metaScanColumnMap.put(entry.getKey(), entry.getValue());
                } else {
                    metaScanColumnMap.put(entry.getKey(), rewriter.getExtendedColumns().get(entry.getKey()));
                }
            }
            scanBuilder.setColRefToColumnMetaMap(metaScanColumnMap);

            // Record the access path into scan node
            List<ColumnAccessPath> paths = Lists.newArrayList();
            for (var entry : rewriter.getExtendedColumns().entrySet()) {
                ColumnAccessPath path = JsonPathRewriteContext.pathFromColumn(entry.getValue());
                paths.add(path);
            }
            scanBuilder.setColumnAccessPaths(paths);

            LogicalMetaScanOperator newMetaScan = scanBuilder.build();
            OptExpression newMetaScanExpr =
                    OptExpression.builder()
                            .with(optExpr.inputAt(0))
                            .setOp(newMetaScan)
                            .build();
            return OptExpression.builder().with(optExpr)
                    .setOp(newProject)
                    .setInputs(Lists.newArrayList(newMetaScanExpr))
                    .build();
        }

        private OptExpression rewritePhysicalScan(OptExpression optExpr, Void v) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpr.getOp();
            PhysicalScanOperator.Builder builder =
                    (PhysicalScanOperator.Builder) OperatorBuilderFactory.build(scanOperator)
                            .withOperator(scanOperator);

            JsonPathRewriteContext context = new JsonPathRewriteContext(columnRefFactory);
            JsonPathRewriter rewriter = new JsonPathRewriter(context);

            // Rewrite predicate
            builder.setPredicate(rewriteScalar(scanOperator.getPredicate(), context, rewriter));

            // Rewrite projection
            if (scanOperator.getProjection() != null) {
                Map<ColumnRefOperator, ScalarOperator> mapping = Maps.newHashMap();
                for (var entry : scanOperator.getProjection().getColumnRefMap().entrySet()) {
                    mapping.put(entry.getKey(), rewriteScalar(entry.getValue(), context, rewriter));
                }
                builder.getProjection().getColumnRefMap().putAll(mapping);
            }

            // Add the fake columns into ScanOperator
            if (MapUtils.isNotEmpty(rewriter.getExtendedColumns())) {
                Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
                colRefToColumnMetaMap.putAll(scanOperator.getColRefToColumnMetaMap());
                colRefToColumnMetaMap.putAll(rewriter.getExtendedColumns());
                builder.setColRefToColumnMetaMap(colRefToColumnMetaMap);
            }

            // Record the access path into scan node
            List<ColumnAccessPath> paths = Lists.newArrayList();
            for (var entry : rewriter.getExtendedColumns().entrySet()) {
                ColumnAccessPath path = JsonPathRewriteContext.pathFromColumn(entry.getValue());
                paths.add(path);
            }
            builder.addColumnAccessPaths(paths);

            Operator newOp = builder.build();
            return OptExpression.builder().with(optExpr).setOp(newOp).build();
        }
    }

    private static ScalarOperator rewriteScalar(ScalarOperator scalar,
                                                JsonPathRewriteContext context,
                                                JsonPathRewriter rewriter) {
        if (scalar == null) {
            return null;
        }
        ScalarOperatorRewriter scalarOperatorRewriter = new ScalarOperatorRewriter();
        return scalarOperatorRewriter.rewrite(scalar, Arrays.asList(rewriter));
    }

    // The actual rewrite rule for ScalarOperator, now takes a context
    private static class JsonPathRewriter extends BottomUpScalarOperatorRewriteRule {
        // only simple field access, no array, no functions, no dot in the path
        private static final Pattern JSON_PATH_VALID_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

        private final JsonPathRewriteContext context;
        // record added fake columns
        private final Map<ColumnRefOperator, Column> extendedColumns = Maps.newHashMap();

        private static final List<String> SUPPORTED_JSON_FUNCTIONS = Arrays.asList(
                FunctionSet.GET_JSON_STRING,
                FunctionSet.GET_JSON_INT,
                FunctionSet.GET_JSON_DOUBLE,
                FunctionSet.GET_JSON_BOOL
        );

        public JsonPathRewriter(JsonPathRewriteContext context) {
            this.context = context;
        }

        public Map<ColumnRefOperator, Column> getExtendedColumns() {
            return extendedColumns;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext rewriteContext) {
            // Only rewrite supported JSON functions with constant path
            if (SUPPORTED_JSON_FUNCTIONS.contains(call.getFnName()) && call.getArguments().size() == 2) {
                return rewriteCall(call, rewriteContext);
            }
            return call;
        }

        private ScalarOperator rewriteCall(CallOperator call, ScalarOperatorRewriteContext rewriteContext) {
            ScalarOperator arg0 = call.getArguments().get(0);
            ScalarOperator arg1 = call.getArguments().get(1);
            if (arg1 instanceof ConstantOperator && arg0 instanceof ColumnRefOperator) {
                String path = ((ConstantOperator) arg1).getVarchar();
                List<String> fields = parseJsonPath(path);
                if (CollectionUtils.isNotEmpty(fields) && isSupported(fields)) {
                    // full path: columnName.field
                    List<String> fullPath = Lists.newArrayList();
                    Pair<Table, Column> tableAndColumn =
                            context.columnRefFactory.getTableAndColumn((ColumnRefOperator) arg0);
                    // only rewrite the expression in scan node, so it must be attached with a table
                    if (tableAndColumn != null) {
                        fullPath.add(tableAndColumn.second.getName());
                        fullPath.addAll(fields);
                        ColumnAccessPath accessPath = ColumnAccessPath.createLinearPath(fullPath, call.getType());
                        Pair<Boolean, ColumnRefOperator> orCreateColumn =
                                context.getOrCreateColumn((ColumnRefOperator) arg0, accessPath);
                        if (!orCreateColumn.first) {
                            extendedColumns.put(orCreateColumn.second,
                                    context.getColumnRefFactory().getColumn(orCreateColumn.second));
                        }
                        return orCreateColumn.second;
                    }
                }
            }

            return call;
        }

        // Parses a JSON path like $.f1.f2 into ["f1", "f2"]
        private static List<String> parseJsonPath(String path) {
            final int jsonFlattenDepth = 20;
            List<String> result = Lists.newArrayList();
            SubfieldAccessPathNormalizer.parseSimpleJsonPath(jsonFlattenDepth, path, result);
            return result;
        }

        private static boolean isSupported(List<String> jsonPath) {
            for (String piece : jsonPath) {
                if (!JSON_PATH_VALID_PATTERN.matcher(piece).matches()) {
                    return false;
                }
            }
            return true;
        }
    }
}
