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
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.BottomUpScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.SubfieldAccessPathNormalizer;
import com.starrocks.type.Type;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * VariantPathRewriteRule rewrites supported scalar VARIANT path functions into synthetic extended columns.
 * <p>
 * Example transformation:
 * get_variant_int(v, '$.a.b')
 * =>
 * v.a.b
 * <p>
 * This rule only supports constant object-field paths and deliberately excludes
 * array/key/index/offset semantics in the first version.
 */
public class VariantPathRewriteRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(VariantPathRewriteRule.class);
    private static final java.util.regex.Pattern VARIANT_PATH_VALID_PATTERN =
            java.util.regex.Pattern.compile("^[a-zA-Z0-9_-]+$");
    public static final String COLUMN_REF_HINT = "VariantPathExtended";

    private static final Set<String> SUPPORTED_VARIANT_FUNCTIONS = Set.of(
            FunctionSet.GET_VARIANT_BOOL,
            FunctionSet.GET_VARIANT_INT,
            FunctionSet.GET_VARIANT_DOUBLE,
            FunctionSet.GET_VARIANT_STRING,
            FunctionSet.GET_VARIANT_DATE,
            FunctionSet.GET_VARIANT_DATETIME,
            FunctionSet.GET_VARIANT_TIME
    );

    protected VariantPathRewriteRule(OperatorType operatorType) {
        super(RuleType.TF_VARIANT_PATH_REWRITE, Pattern.create(operatorType));
    }

    public static VariantPathRewriteRule createForIcebergScan() {
        return new VariantPathRewriteRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    }

    @Override
    public List<OptExpression> transform(OptExpression root, OptimizerContext optimizerContext) {
        SessionVariable variables = optimizerContext.getSessionVariable();
        if (!variables.isEnableVariantPathRewrite() || variables.isCboUseDBLock()) {
            return List.of(root);
        }

        ColumnRefFactory columnRefFactory = optimizerContext.getColumnRefFactory();
        try {
            VariantPathRewriteVisitor visitor = new VariantPathRewriteVisitor(columnRefFactory);
            root = root.getOp().accept(visitor, root, null);
            optimizerContext.getTaskContext().getRequiredColumns().union(root.getOutputColumns());
            return List.of(root);
        } catch (Exception e) {
            LOG.warn("Failed to rewrite VARIANT paths in expression: {}", root, e);
            return List.of(root);
        }
    }

    public static class VariantPathRewriteContext {
        private final Map<String, ColumnRefOperator> pathMap = Maps.newHashMap();
        private final Map<ColumnRefOperator, Column> extendedColumns = Maps.newHashMap();
        private final ColumnRefFactory columnRefFactory;
        private Table scanTable;

        public VariantPathRewriteContext(ColumnRefFactory factory) {
            this.columnRefFactory = factory;
        }

        public void setScanTable(Table table) {
            this.scanTable = table;
        }

        public ColumnRefFactory getColumnRefFactory() {
            return columnRefFactory;
        }

        public Map<ColumnRefOperator, Column> getExtendedColumns() {
            return extendedColumns;
        }

        public Pair<Boolean, ColumnRefOperator> getOrCreateColumn(ColumnRefOperator variantColumn,
                                                                  ColumnAccessPath variantPath) {
            Pair<Table, Column> tableAndColumn = columnRefFactory.getTableAndColumn(variantColumn);
            Preconditions.checkState(tableAndColumn != null,
                    "ColumnRefOperator %s must be attached to a table", variantColumn);

            Table targetTable = scanTable != null ? scanTable : tableAndColumn.first;
            Column targetColumn = targetTable.getColumn(tableAndColumn.second.getName());
            if (targetColumn == null) {
                targetTable = tableAndColumn.first;
                targetColumn = tableAndColumn.second;
            }

            String path = variantPath.getLinearPath();
            String fullPath = targetTable.getId() + "." + path;
            ColumnRefOperator existingColumn = pathMap.get(fullPath);
            if (existingColumn != null) {
                if (variantPath.getValueType().equals(existingColumn.getType())) {
                    return Pair.create(true, existingColumn);
                } else {
                    throw new IllegalArgumentException("unsupported mixed variant path type: "
                            + variantPath.getValueType() + " and " + existingColumn.getType());
                }
            }

            Column extendedColumn = createExtendedColumn(targetTable, targetColumn, path, variantPath);
            ColumnRefOperator newColumnRef = columnRefFactory.create(path, variantPath.getValueType(), true);
            newColumnRef.setHints(List.of(COLUMN_REF_HINT));
            columnRefFactory.updateColumnRefToColumns(newColumnRef, extendedColumn, targetTable);
            pathMap.put(fullPath, newColumnRef);
            extendedColumns.put(newColumnRef, extendedColumn);
            return Pair.create(false, newColumnRef);
        }

        private Column createExtendedColumn(Table table, Column sourceColumn, String path, ColumnAccessPath variantPath) {
            if (table.containColumn(path)) {
                return table.getColumn(path);
            }
            String sourcePhysicalName = sourceColumn != null ? sourceColumn.getPhysicalName() : null;
            return new Column(path, variantPath.getValueType(), false, null, null, true, null, "",
                    Column.COLUMN_UNIQUE_ID_INIT_VALUE, sourcePhysicalName == null ? "" : sourcePhysicalName);
        }

        public static ColumnAccessPath pathFromColumn(Column column) {
            String name = column.getName();
            ColumnAccessPath res = ColumnAccessPath.createFromLinearPath(name, column.getType());
            res.setExtended(true);
            return res;
        }
    }

    private static class VariantPathRewriteVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final ColumnRefFactory columnRefFactory;

        public VariantPathRewriteVisitor(ColumnRefFactory factory) {
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

        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpr, Void context) {
            if (!(optExpr.getOp() instanceof LogicalScanOperator)) {
                return optExpr;
            }
            return rewriteLogicalScan(optExpr);
        }

        private OptExpression rewriteLogicalScan(OptExpression optExpr) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) optExpr.getOp();
            LogicalScanOperator.Builder builder =
                    (LogicalScanOperator.Builder) OperatorBuilderFactory.build(scanOperator).withOperator(scanOperator);

            VariantPathRewriteContext context = new VariantPathRewriteContext(columnRefFactory);
            context.setScanTable(scanOperator.getTable());
            VariantPathExpressionRewriter rewriter = new VariantPathExpressionRewriter(context);

            builder.setPredicate(rewriteScalar(scanOperator.getPredicate(), context, rewriter));

            ColumnRefSet requiredColumnSet = new ColumnRefSet();
            if (builder.getPredicate() != null) {
                requiredColumnSet.union(builder.getPredicate().getUsedColumns());
            }
            if (scanOperator.getProjection() != null) {
                Map<ColumnRefOperator, ScalarOperator> mapping = Maps.newHashMap();
                for (var entry : scanOperator.getProjection().getColumnRefMap().entrySet()) {
                    mapping.put(entry.getKey(), rewriteScalar(entry.getValue(), context, rewriter));
                }
                builder.getProjection().getColumnRefMap().putAll(mapping);

                Map<ColumnRefOperator, ScalarOperator> commonSubMapping = Maps.newHashMap();
                for (var entry : scanOperator.getProjection().getCommonSubOperatorMap().entrySet()) {
                    commonSubMapping.put(entry.getKey(), rewriteScalar(entry.getValue(), context, rewriter));
                }
                builder.getProjection().getCommonSubOperatorMap().putAll(commonSubMapping);

                mapping.values().forEach(x -> requiredColumnSet.union(x.getUsedColumns()));
                commonSubMapping.values().forEach(x -> requiredColumnSet.union(x.getUsedColumns()));
            } else {
                scanOperator.getOutputColumns().forEach(requiredColumnSet::union);
            }

            if (MapUtils.isNotEmpty(rewriter.getExtendedColumns())) {
                Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
                colRefToColumnMetaMap.putAll(rewriter.getExtendedColumns());
                for (var entry : scanOperator.getColRefToColumnMetaMap().entrySet()) {
                    if (requiredColumnSet.contains(entry.getKey())) {
                        colRefToColumnMetaMap.put(entry.getKey(), entry.getValue());
                    }
                }
                builder.setColRefToColumnMetaMap(colRefToColumnMetaMap);
                for (ColumnRefOperator col : rewriter.getExtendedColumns().keySet()) {
                    optExpr.getOutputColumns().union(col);
                }

                List<ColumnAccessPath> paths = Lists.newArrayList();
                for (var entry : rewriter.getExtendedColumns().entrySet()) {
                    paths.add(VariantPathRewriteContext.pathFromColumn(entry.getValue()));
                }
                builder.addColumnAccessPaths(paths);
            }

            Operator newOp = builder.build();
            return OptExpression.builder().with(optExpr).setOp(newOp).build();
        }

    }

    private static ScalarOperator rewriteScalar(ScalarOperator scalar,
                                                VariantPathRewriteContext context,
                                                VariantPathExpressionRewriter rewriter) {
        if (scalar == null) {
            return null;
        }
        ScalarOperatorRewriter scalarOperatorRewriter = new ScalarOperatorRewriter();
        return scalarOperatorRewriter.rewrite(scalar, Arrays.asList(rewriter));
    }

    private static class VariantPathExpressionRewriter extends BottomUpScalarOperatorRewriteRule {
        private final VariantPathRewriteContext context;

        public VariantPathExpressionRewriter(VariantPathRewriteContext context) {
            this.context = context;
        }

        public Map<ColumnRefOperator, Column> getExtendedColumns() {
            return context.getExtendedColumns();
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext rewriteContext) {
            if (isRewriteCandidate(call)) {
                return rewriteVariantFunction(call);
            }
            return call;
        }

        @Override
        public ScalarOperator visitCastOperator(CastOperator operator, ScalarOperatorRewriteContext context) {
            if (!(operator.getChild(0) instanceof CallOperator call)) {
                return operator;
            }
            if (!isRewriteCandidate(operator, call)) {
                return operator;
            }

            ScalarOperator variantColumn = call.getArguments().get(0);
            ScalarOperator pathArg = call.getArguments().get(1);
            if (!(pathArg instanceof ConstantOperator) || !(variantColumn instanceof ColumnRefOperator variantColumnRef)) {
                return operator;
            }

            Pair<Table, Column> tableAndColumn = this.context.getColumnRefFactory().getTableAndColumn(variantColumnRef);
            if (tableAndColumn == null || !isSupportedCastTarget(operator.getType())) {
                return operator;
            }

            String path = ((ConstantOperator) pathArg).getVarchar();
            List<String> fields = parseVariantPath(path);
            if (!isValidVariantPath(fields)) {
                return operator;
            }

            try {
                return createColumnAccessExpression(variantColumnRef, fields, operator.getType());
            } catch (IllegalArgumentException e) {
                return operator;
            }
        }

        static boolean isSupportedVariantFunction(CallOperator call) {
            return SUPPORTED_VARIANT_FUNCTIONS.contains(call.getFnName())
                    && call.getArguments().size() == 2
                    && call.getChild(0).getType().isVariantType();
        }

        static boolean isSupportedVariantQuery(CallOperator call) {
            return FunctionSet.VARIANT_QUERY.equals(call.getFnName())
                    && call.getArguments().size() == 2
                    && call.getChild(0).getType().isVariantType();
        }

        static boolean isSupportedCastTarget(Type type) {
            return type.isBoolean()
                    || type.isIntegerType()
                    || type.isFloatingPointType()
                    || type.isStringType()
                    || type.isDate()
                    || type.isDatetime()
                    || type.isTime();
        }

        static boolean isRewriteCandidate(CallOperator call) {
            return isSupportedVariantFunction(call);
        }

        static boolean isRewriteCandidate(CastOperator operator, CallOperator call) {
            return isSupportedVariantQuery(call) && isSupportedCastTarget(operator.getType());
        }

        private ScalarOperator rewriteVariantFunction(CallOperator call) {
            ScalarOperator variantColumn = call.getArguments().get(0);
            ScalarOperator pathArg = call.getArguments().get(1);
            if (!(pathArg instanceof ConstantOperator) || !(variantColumn instanceof ColumnRefOperator variantColumnRef)) {
                return call;
            }

            Pair<Table, Column> tableAndColumn = context.getColumnRefFactory().getTableAndColumn(variantColumnRef);
            if (tableAndColumn == null) {
                return call;
            }

            String path = ((ConstantOperator) pathArg).getVarchar();
            List<String> fields = parseVariantPath(path);
            if (!isValidVariantPath(fields)) {
                return call;
            }
            try {
                return createColumnAccessExpression(variantColumnRef, fields, call.getType());
            } catch (IllegalArgumentException e) {
                // Mixed-type access on the same path is intentionally left unrevised.
                return call;
            }
        }

        private ScalarOperator createColumnAccessExpression(ColumnRefOperator variantColumn,
                                                            List<String> fields,
                                                            Type resultType) {
            Pair<Table, Column> tableAndColumn = context.getColumnRefFactory().getTableAndColumn(variantColumn);
            Preconditions.checkState(tableAndColumn != null,
                    "ColumnRefOperator %s must be attached to a table when creating column access expression",
                    variantColumn);
            List<String> fullPath = Lists.newArrayList();
            fullPath.add(tableAndColumn.second.getColumnId().getId());
            fullPath.addAll(fields);
            ColumnAccessPath accessPath = ColumnAccessPath.createLinearPath(fullPath, resultType);
            Pair<Boolean, ColumnRefOperator> columnResult = context.getOrCreateColumn(variantColumn, accessPath);
            return columnResult.second;
        }

        static List<String> parseVariantPath(String path) {
            List<String> result = SubfieldAccessPathNormalizer.parseSimpleJsonPath(path);
            return result.isEmpty() ? null : result;
        }

        static boolean isValidVariantPath(List<String> variantPath) {
            if (CollectionUtils.isEmpty(variantPath)) {
                return false;
            }
            return variantPath.stream().allMatch(field -> VARIANT_PATH_VALID_PATTERN.matcher(field).matches());
        }
    }
}
