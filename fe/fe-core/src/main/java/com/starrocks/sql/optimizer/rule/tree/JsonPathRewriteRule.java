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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.BottomUpScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.thrift.TAccessPathType;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        JsonPathRewriteContext context = root.getJsonPathRewriteContext();
        ColumnRefFactory columnRefFactory = taskContext.getOptimizerContext().getColumnRefFactory();
        context.setColumnRefFactory(columnRefFactory);
        return rewriteOptExpression(root, context);
    }

    private static OptExpression rewriteOptExpression(OptExpression optExpr, JsonPathRewriteContext context) {
        JsonPathRewriteVisitor visitor = new JsonPathRewriteVisitor();
        return optExpr.getOp().accept(visitor, optExpr, context);
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

    // Context to record mapping from original JSON function expressions to rewritten expressions
    public static class JsonPathRewriteContext {

        // c1.f1 => get_json_string(c1, '$.f1')
        private final Map<ScalarOperator, ScalarOperator> jsonExprMapping = Maps.newHashMap();
        // full path mapping: t1.c1.f1 => c1.f1
        private final Map<String, ColumnRefOperator> pathMap = Maps.newHashMap();
        // temporary factory
        private ColumnRefFactory columnRefFactory;

        public void setColumnRefFactory(ColumnRefFactory factory) {
            this.columnRefFactory = factory;
        }

        public ColumnRefFactory getColumnRefFactory() {
            return columnRefFactory;
        }

        public Map<String, ColumnRefOperator> getPathMap() {
            return pathMap;
        }

        public Pair<Boolean, ColumnRefOperator> getOrCreateColumn(ColumnRefOperator originalColumn,
                                                                  ColumnAccessPath jsonPath) {
            Pair<Table, Column> tableAndColumn = columnRefFactory.getTableAndColumn(originalColumn);
            String path = jsonPath.getLinearPath();
            String fullPath = tableAndColumn.first.getName() + "." + path;
            ColumnRefOperator res = pathMap.get(fullPath);
            if (res != null) {
                return Pair.create(true, res);
            }

            // create a column in the table
            Column fakeColumn;
            if (!tableAndColumn.first.containColumn(path)) {
                fakeColumn = new Column(path, jsonPath.getValueType(), true);
                tableAndColumn.first.addColumn(fakeColumn);
            } else {
                fakeColumn = tableAndColumn.first.getColumn(path);
            }

            res = columnRefFactory.create(path, jsonPath.getValueType(), true);
            columnRefFactory.updateColumnRefToColumns(res, fakeColumn, tableAndColumn.first);
            pathMap.put(fullPath, res);
            return Pair.create(false, res);
        }

        public void put(ScalarOperator original, ScalarOperator rewritten) {
            jsonExprMapping.put(original, rewritten);
        }

    }

    private static class JsonPathRewriteVisitor extends OptExpressionVisitor<OptExpression, JsonPathRewriteContext> {
        @Override
        public OptExpression visit(OptExpression optExpr, JsonPathRewriteContext context) {
            List<OptExpression> newInputs = new ArrayList<>();
            for (OptExpression input : optExpr.getInputs()) {
                OptExpression child = input.getOp().accept(this, input, context);
                newInputs.add(child);
            }

            return OptExpression.builder().with(optExpr).setInputs(newInputs).build();
        }

        @Override
        public OptExpression visitPhysicalScan(OptExpression optExpr, JsonPathRewriteContext context) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpr.getOp();
            PhysicalOlapScanOperator.Builder builder =
                    (PhysicalOlapScanOperator.Builder) OperatorBuilderFactory.build(scanOperator)
                            .withOperator(scanOperator);

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
            if (MapUtils.isNotEmpty(rewriter.getFakeColumns())) {
                Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
                colRefToColumnMetaMap.putAll(scanOperator.getColRefToColumnMetaMap());
                colRefToColumnMetaMap.putAll(rewriter.getFakeColumns());
                builder.setColRefToColumnMetaMap(colRefToColumnMetaMap);
            }

            // Record the access path into scan node
            List<ColumnAccessPath> paths = Lists.newArrayList();
            for (var entry : rewriter.getFakeColumns().entrySet()) {
                Column fakeColumn = entry.getValue();
                ColumnAccessPath path = buildColumnAccessPathFromFakeColumn(fakeColumn);
                paths.add(path);
            }
            builder.setColumnAccessPath(paths);

            // Recursively rewrite children (should be empty for scan, but for completeness)
            List<OptExpression> newInputs = new ArrayList<>();
            for (OptExpression input : optExpr.getInputs()) {
                newInputs.add(input.getOp().accept(this, input, context));
            }

            Operator newOp = builder.build();
            return OptExpression.builder().with(optExpr).setOp(newOp).setInputs(newInputs).build();
        }

        private static ColumnAccessPath buildColumnAccessPathFromFakeColumn(Column column) {
            List<String> slices = Splitter.on(".").splitToList(column.getName());
            ColumnAccessPath res = new ColumnAccessPath(TAccessPathType.ROOT, slices.get(0), column.getType());
            res.setExtended(true);
            for (int i = 1; i < slices.size(); i++) {
                res.addChildPath(new ColumnAccessPath(TAccessPathType.FIELD, slices.get(i), column.getType()));
            }
            return res;
        }
    }


    // The actual rewrite rule for ScalarOperator, now takes a context
    private static class JsonPathRewriter extends BottomUpScalarOperatorRewriteRule {
        private final JsonPathRewriteContext context;
        // record added fake columns
        private final Map<ColumnRefOperator, Column> fakeColumns = Maps.newHashMap();

        private static final List<String> SUPPORTED_JSON_FUNCTIONS = Arrays.asList(
                FunctionSet.GET_JSON_STRING,
                FunctionSet.GET_JSON_INT,
                FunctionSet.GET_JSON_DOUBLE,
                FunctionSet.GET_JSON_BOOL
        );

        public JsonPathRewriter(JsonPathRewriteContext context) {
            this.context = context;
        }

        public Map<ColumnRefOperator, Column> getFakeColumns() {
            return fakeColumns;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext rewriteContext) {
            // Only rewrite supported JSON functions with constant path
            if (SUPPORTED_JSON_FUNCTIONS.contains(call.getFnName()) && call.getArguments().size() == 2) {
                ScalarOperator arg0 = call.getArguments().get(0);
                ScalarOperator arg1 = call.getArguments().get(1);
                if (arg1 instanceof ConstantOperator && arg0 instanceof ColumnRefOperator) {
                    String path = ((ConstantOperator) arg1).getVarchar();
                    List<String> fields = parseJsonPath(path);
                    if (fields != null && !fields.isEmpty()) {
                        List<String> fullPath = Lists.newArrayList();
                        fullPath.add(((ColumnRefOperator) arg0).getName());
                        fullPath.addAll(fields);
                        ColumnAccessPath accessPath = ColumnAccessPath.createLinearPath(fullPath, call.getType());
                        Pair<Boolean, ColumnRefOperator> orCreateColumn =
                                context.getOrCreateColumn((ColumnRefOperator) arg0, accessPath);
                        if (!orCreateColumn.first) {
                            fakeColumns.put(orCreateColumn.second,
                                    context.getColumnRefFactory().getColumn(orCreateColumn.second));
                        }
                        return orCreateColumn.second;
                    }
                }
            }
            // Recursively rewrite children
            return super.visitCall(call, rewriteContext);
        }

        // Parses a JSON path like $.f1.f2 into ["f1", "f2"]
        private static List<String> parseJsonPath(String path) {
            if (path == null || path.isEmpty()) {
                return null;
            }
            path = path.trim();
            if (path.startsWith("$")) {
                path = path.substring(1);
            }
            if (path.startsWith(".")) {
                path = path.substring(1);
            }
            return Splitter.on(".").splitToList(path);
        }
    }
}
