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
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.common.EngineType;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.mv.KeyInference;
import com.starrocks.sql.optimizer.rule.mv.MVOperatorProperty;
import com.starrocks.sql.plan.ExecPlan;
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
        Set<String> keyColumns = key.columns.getStream().map(columnNames::get).collect(Collectors.toSet());
        for (Column col : columns) {
            col.setIsKey(keyColumns.contains(col.getName()));
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

    public static void createIMT(CreateMaterializedViewStatement stmt, MaterializedView view) throws DdlException {
        ExecPlan maintenancePlan = stmt.getMaintenancePlan();
        OptExpression optExpr = maintenancePlan.getPhysicalPlan();
        List<CreateTableStmt> createTables = IMTCreatorVisitor.createIMTForOperator(stmt, optExpr, view);

        for (CreateTableStmt create : createTables) {
            LOG.info("creating IMT {} for MV {}", create.getTableName(), view.getName());
            try {
                GlobalStateMgr.getCurrentState().getStarRocksMeta().createTable(create);
            } catch (DdlException e) {
                // TODO(murphy) cleanup created IMT, or make it atomic
                LOG.warn("create IMT {} failed due to ", create.getTableName(), e);
                throw e;
            }
        }
    }

    static class IMTCreatorVisitor extends OptExpressionVisitor<Void, Void> {
        private CreateMaterializedViewStatement stmt;
        private MaterializedView view;
        private List<CreateTableStmt> result = new ArrayList<>();

        public static List<CreateTableStmt> createIMTForOperator(CreateMaterializedViewStatement stmt,
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
        public Void visit(OptExpression optExpr, Void ctx) {
            return null;
        }

        @Override
        public Void visitPhysicalStreamJoin(OptExpression optExpr, Void ctx) {
            // Join does not need any IMT
            return null;
        }

        /**
         * StreamAgg needs two IMT
         * 1. State Table: store the state of all aggregation functions for non-retractable ModifyOp
         * 2. Detail Table: for retractable ModifyOp
         * <p>
         * TODO(murphy) create detail table
         */
        @Override
        public Void visitPhysicalStreamAgg(OptExpression optExpr, Void ctx) {
            MVOperatorProperty property = optExpr.getMvOperatorProperty();
            ColumnRefFactory columnRefFactory = stmt.getColumnRefFactory();
            Map<String, String> properties = view.getProperties();

            if (!property.getModify().isInsertOnly()) {
                throw UnsupportedException.unsupportedException("Not support retractable aggregate");
            }

            // Duplicate/Primary Key
            KeyInference.KeyProperty bestKey = property.getKeySet().getBestKey();

            // Columns
            // TODO(murphy) create columns according to StreamAgg requirement
            List<Column> keyColumns = new ArrayList<>();
            List<ColumnDef> columnDefs = new ArrayList<>();
            ColumnRefSet columnRefSet = optExpr.getOutputColumns();
            for (int columnId : columnRefSet.getColumnIds()) {
                ColumnRefOperator refOp = columnRefFactory.getColumnRef(columnId);

                Column newColumn = new Column(refOp.getName(), refOp.getType());
                boolean isKey = bestKey.columns.contains(refOp);
                newColumn.setIsKey(isKey);
                newColumn.setIsAllowNull(!isKey);
                if (isKey) {
                    keyColumns.add(newColumn);
                }

                TypeDef typeDef = TypeDef.create(refOp.getType().getPrimitiveType());
                ColumnDef columnDef = new ColumnDef(refOp.getName(), typeDef);
                columnDef.setIsKey(isKey);
                columnDef.setAllowNull(!isKey);
                columnDefs.add(columnDef);
            }

            // Key type
            KeysType keyType = KeysType.PRIMARY_KEYS;
            KeysDesc keyDesc =
                    new KeysDesc(keyType, keyColumns.stream().map(Column::getName).collect(Collectors.toList()));

            // Partition scheme
            // TODO(murphy) support partition the IMT, current we don't support it
            PartitionDesc partitionDesc = new PartitionDesc();

            // Distribute Key
            DistributionDesc distributionDesc = new DistributionDesc();
            Preconditions.checkNotNull(distributionDesc);
            HashDistributionInfo distributionInfo = new HashDistributionInfo();
            distributionInfo.setBucketNum(CatalogUtils.calBucketNumAccordingToBackends());
            distributionInfo.setDistributionColumns(keyColumns);

            // TODO(murphy) refine it
            String mvName = stmt.getTableName().getTbl();
            long seq = GlobalStateMgr.getCurrentState().getNextId();
            String tableName = "imt_" + mvName + seq;
            TableName canonicalName = new TableName(stmt.getTableName().getDb(), tableName);

            // Properties
            Map<String, String> extProperties = Maps.newTreeMap();
            String comment = "IMT for MV StreamAggOperator";

            result.add(new CreateTableStmt(false, false, canonicalName, columnDefs,
                    EngineType.defaultEngine().name(), keyDesc, partitionDesc, distributionDesc, properties,
                    extProperties, comment));
            return null;
        }

    }

}
