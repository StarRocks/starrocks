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


package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.CreateTableAnalyzer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.stream.IMTStateTable;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamOperator;
import com.starrocks.sql.optimizer.rule.mv.KeyInference;
import com.starrocks.sql.optimizer.rule.mv.MVOperatorProperty;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Create various tables for MV
 */
class IMTCreator {
    private static final Logger LOG = LogManager.getLogger(IMTCreator.class);

    /*
     * TODO(murphy) partial duplicated with LocalMetaStore::createMaterializedView
     * Create sink table with
     * 1. Columns
     * 2. Distribute by key buckets
     * 3. Duplicate Key/Primary Key
     */
    public static MaterializedView createSinkTable(CreateMaterializedViewStatement stmt, PartitionInfo partitionInfo,
                                                   long mvId, long dbId)
            throws DdlException {
        ExecPlan plan = Preconditions.checkNotNull(stmt.getMaintenancePlan());
        OptExpression physicalPlan = plan.getPhysicalPlan();
        MVOperatorProperty property =
                Preconditions.checkNotNull(physicalPlan.getMvOperatorProperty(), "incremental mv must have property");
        KeyInference.KeyPropertySet keys = property.getKeySet();
        if (keys.empty()) {
            throw UnsupportedException.unsupportedException("mv could not infer keys");
        }
        keys.sortKeys();
        KeyInference.KeyProperty key = keys.getKeys().get(0);

        // Columns
        List<Column> columns = stmt.getMvColumnItems();

        // Duplicate/Primary Key
        Map<Integer, String> columnNames = plan.getOutputColumns().stream().collect(
                Collectors.toMap(ColumnRefOperator::getId, ColumnRefOperator::getName));
        Set<String> keyColumns = key.columns.getStream().mapToObj(columnNames::get).collect(Collectors.toSet());
        for (Column col : columns) {
            if (keyColumns.contains(col.getName())) {
                col.setIsKey(true);
                // TODO: fix me later, Key should be nullable?
                col.setIsAllowNull(false);
            }
        }
        if (!property.getModify().isInsertOnly()) {
            stmt.setKeysType(KeysType.PRIMARY_KEYS);
        }

        // Distribute Key, already set in MVAnalyzer
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(columns);
        if (distributionInfo.getBucketNum() == 0) {
            int numBucket = CatalogUtils.calBucketNumAccordingToBackends();
            distributionInfo.setBucketNum(numBucket);
        }

        // Refresh
        MaterializedView.MvRefreshScheme mvRefreshScheme = new MaterializedView.MvRefreshScheme();
        mvRefreshScheme.setType(MaterializedView.RefreshType.INCREMENTAL);

        String mvName = stmt.getTableName().getTbl();
        return new MaterializedView(mvId, dbId, mvName, columns, stmt.getKeysType(), partitionInfo,
                distributionInfo, mvRefreshScheme);
    }

    public static List<IMTStateTable> createIMT(CreateMaterializedViewStatement stmt, MaterializedView view) throws DdlException {
        ExecPlan maintenancePlan = stmt.getMaintenancePlan();
        OptExpression optExpr = maintenancePlan.getPhysicalPlan();
        List<IMTCreatorVisitor.IMTCreatorResult> createTables = IMTCreatorVisitor.createIMTForOperator(stmt, optExpr, view);

        // Create IMT tables in batch
        for (IMTCreatorVisitor.IMTCreatorResult creatorResult : createTables) {
            List<CreateTableStmt> stmts = creatorResult.getImtTables();
            for (CreateTableStmt create : stmts) {
                LOG.info("creating IMT {} for MV {}", create.getTableName(), view.getName());
                try {
                    GlobalStateMgr.getCurrentState().createTable(create);
                } catch (DdlException e) {
                    // TODO(murphy) cleanup created IMT, or make it atomic
                    LOG.warn("create IMT {} failed due to ", create.getTableName(), e);
                    throw e;
                }
            }
        }

        // Assign imt infos to each operator
        List<IMTStateTable> imtStateTables = Lists.newArrayList();
        for (IMTCreatorVisitor.IMTCreatorResult creatorResult : createTables) {
            OptExpression optExpression = creatorResult.getOptWithIMTs();
            Preconditions.checkState(optExpression.getOp() instanceof PhysicalStreamOperator);
            PhysicalStreamOperator streamOperator = (PhysicalStreamOperator) optExpression.getOp();
            try {
                imtStateTables.addAll(streamOperator.assignIMTInfos());
            } catch (DdlException e) {
                // TODO(murphy) cleanup created IMT, or make it atomic
                LOG.warn("Assign IMT info failed due to ", e);
                throw e;
            }
        }
        return imtStateTables;
    }

    static class IMTCreatorVisitor extends OptExpressionVisitor<Void, Void> {
        private CreateMaterializedViewStatement stmt;
        private MaterializedView view;

        public class IMTCreatorResult {
            private OptExpression optWithIMTs;
            private List<CreateTableStmt> imtTables = new ArrayList<>();

            public IMTCreatorResult(OptExpression optExpression, List<CreateTableStmt> stmts) {
                this.optWithIMTs = optExpression;
                this.imtTables = stmts;
            }

            public OptExpression getOptWithIMTs() {
                return optWithIMTs;
            }

            public List<CreateTableStmt> getImtTables() {
                return imtTables;
            }
        };

        private List<IMTCreatorResult> result = new ArrayList<>();

        public static List<IMTCreatorResult> createIMTForOperator(CreateMaterializedViewStatement stmt,
                                                                  OptExpression optExpr,
                                                                  MaterializedView view) {
            IMTCreatorVisitor visitor = new IMTCreatorVisitor();
            visitor.stmt = stmt;
            visitor.view = view;
            visitor.visit(visitor, optExpr);
            return visitor.result;
        }

        private void visit(IMTCreatorVisitor visitor, OptExpression optExpr) {
            for (OptExpression child : optExpr.getInputs()) {
                visitor.visit(child, null);
            }
            visitor.visit(optExpr, null);
        }

        @Override
        public Void visitPhysicalProject(OptExpression optExpression, Void ctx) {
            return null;
        }

        @Override
        public Void visitPhysicalFilter(OptExpression optExpression, Void ctx) {
            return null;
        }

        @Override
        public Void visitPhysicalStreamScan(OptExpression optExpression, Void ctx) {
            return null;
        }

        @Override
        public Void visitPhysicalStreamJoin(OptExpression optExpr, Void ctx) {
            // Join does not need any IMT
            return null;
        }

        /**
         * StreamAgg needs IMTStateTables as below:
         * 1. ResultStateTable, keeps the result of StreamAgg node which can be used to
         *      generate RetractMessage and intermediate state for some kinds of AggFuncs, eg min/max/sum/count;
         * 2. IntermediateStateTable, keeps the intermediate state of generate AggFuncs,
         *      such as avg/approx_count_distinct/retract_min;
         * 3. DetailStateTable, keeps the detail data of each detail-kind AggFuncs, such as min/max.
         * <p>
         */
        @Override
        public Void visitPhysicalStreamAgg(OptExpression optExpr, Void ctx) {
            PhysicalStreamAggOperator streamAgg = (PhysicalStreamAggOperator) optExpr.getOp();

            // Check supported functions in Stream MV
            Map<ColumnRefOperator, CallOperator> aggregations = streamAgg.getAggregations();
            for (Map.Entry<ColumnRefOperator, CallOperator> aggregation : aggregations.entrySet()) {
                CallOperator operator = aggregation.getValue();
                switch (operator.getFnName()) {
                    // Aggregate Functions which Intermediate state can be set into ResultStateTable
                    case FunctionSet.COUNT:
                    case FunctionSet.SUM:
                        break;
                    case FunctionSet.RETRACT_MAX:
                    case FunctionSet.RETRACT_MIN:
                        break;
                    // TODO: intermediate/detail functions to be supported later.
                    default:
                        throw new NotImplementedException("StreamAgg still not support agg function:" + operator.getFnName());
                }
            }

            // TODO: Check whether it's retractable or not and pass it into be

            // Stream Aggregate result IMTStateTable
            List<CreateTableStmt> imtStateTables = new ArrayList<>();
            CreateTableStmt resultStmt = createAggResultTableStmt(stmt, view, optExpr);
            streamAgg.setResultIMTName(resultStmt.getDbTbl());
            imtStateTables.add(resultStmt);
            result.add(new IMTCreatorResult(optExpr, imtStateTables));

            // TODO: support intermediate/detail IMTStateTable

            return null;
        }

        private CreateTableStmt createAggResultTableStmt(CreateMaterializedViewStatement stmt,
                                                         MaterializedView view,
                                                         OptExpression optExpr) {
            MVOperatorProperty property = optExpr.getMvOperatorProperty();
            ColumnRefFactory columnRefFactory = stmt.getColumnRefFactory();
            Map<String, String> properties = view.getProperties();

            // Duplicate/Primary Key
            KeyInference.KeyProperty bestKey = property.getKeySet().getBestKey();

            // Columns
            // TODO(murphy) create columns according to StreamAgg requirement
            List<Column> keyColumns = new ArrayList<>();
            List<ColumnDef> columnDefs = new ArrayList<>();
            ColumnRefSet columnRefSet = optExpr.getOutputColumns();
            for (int columnId : columnRefSet.getColumnIds()) {
                ColumnRefOperator refOp = columnRefFactory.getColumnRef(columnId);

                boolean isKey = bestKey.columns.contains(refOp);
                if (isKey) {
                    Column newColumn = new Column(refOp.getName(), refOp.getType());
                    newColumn.setIsKey(true);
                    // TODO: fix me later, need support Key is nullable.
                    newColumn.setIsAllowNull(false);
                    keyColumns.add(newColumn);
                }

                TypeDef typeDef = TypeDef.create(refOp.getType().getPrimitiveType());
                ColumnDef columnDef = new ColumnDef(refOp.getName(), typeDef);
                columnDef.setIsKey(isKey);
                // TODO: fix me later, Keys should be nullable
                if (isKey) {
                    columnDef.setAllowNull(false);
                }
                columnDefs.add(columnDef);
            }
            List<String> keysColumnNames  = keyColumns.stream().map(Column::getName).collect(Collectors.toList());
            // Key type
            KeysType keyType = KeysType.PRIMARY_KEYS;
            KeysDesc keyDesc = new KeysDesc(keyType, keysColumnNames);

            // Partition scheme
            // TODO(murphy) support partition the IMT, current we don't support it
            PartitionDesc partitionDesc = null;

            // Distribute Key: hash distribution
            DistributionDesc distributionDesc = new HashDistributionDesc(CatalogUtils.calBucketNumAccordingToBackends(),
                    keysColumnNames);
            Preconditions.checkNotNull(distributionDesc);

            // TODO(murphy) refine it
            String mvName = stmt.getTableName().getTbl();
            long seq = GlobalStateMgr.getCurrentState().getNextId();
            String tableName = "imt_agg_result_" + mvName + seq;
            TableName canonicalName = new TableName(stmt.getTableName().getDb(), tableName);

            // Properties
            Map<String, String> extProperties = Maps.newTreeMap();
            String comment = "IMT for MV StreamAggOperator";
            CreateTableStmt resultStmt = new CreateTableStmt(false, false, canonicalName, columnDefs,
                    CreateTableAnalyzer.EngineType.OLAP.name().toLowerCase(), keyDesc,
                    partitionDesc, distributionDesc, properties,
                    extProperties, comment);
            // TODO(lism): CreateTableStmt's columns should sync with columnDefs.
            List<Column> columns = resultStmt.getColumns();
            for (ColumnDef columnDef : columnDefs) {
                Column col = columnDef.toColumn();
                columns.add(col);
            }
            return resultStmt;
        }
    }
}