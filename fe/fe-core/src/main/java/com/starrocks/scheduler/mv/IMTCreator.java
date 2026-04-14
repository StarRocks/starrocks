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
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.TypeDef;
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
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create various tables for MV
 */
class IMTCreator {
    private static final Logger LOG = LogManager.getLogger(IMTCreator.class);

    static void createIMT(CreateMaterializedViewStatement stmt,
                          MaterializedView view,
                          ExecPlan maintenancePlan,
                          ColumnRefFactory columnRefFactory) throws DdlException {
        OptExpression optExpr = maintenancePlan.getPhysicalPlan();
        List<CreateTableStmt> createTables =
                IMTCreatorVisitor.createIMTForOperator(stmt, optExpr, view, columnRefFactory);

        for (CreateTableStmt create : createTables) {
            LOG.info("creating IMT {} for MV {}", create.getTableName(), view.getName());
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(create);
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
        private ColumnRefFactory columnRefFactory;
        private List<CreateTableStmt> result = new ArrayList<>();

        public static List<CreateTableStmt> createIMTForOperator(CreateMaterializedViewStatement stmt,
                                                                 OptExpression optExpr,
                                                                 MaterializedView view,
                                                                 ColumnRefFactory columnRefFactory) {
            IMTCreatorVisitor visitor = new IMTCreatorVisitor();
            visitor.stmt = stmt;
            visitor.view = view;
            visitor.columnRefFactory = columnRefFactory;
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

                TypeDef typeDef = new TypeDef(TypeFactory.createType(refOp.getType().getPrimitiveType()));
                ColumnDef columnDef = new ColumnDef(refOp.getName(), typeDef,
                        /* isKey */ isKey,
                        /* aggregateType */ null,
                        /* aggStateDesc */ null,
                        /* isAllowNull */ !isKey,
                        /* defaultValueDef */ ColumnDef.DefaultValueDef.NOT_SET,
                        /* comment */ "");
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
            String mvName = stmt.getTblName();
            long seq = GlobalStateMgr.getCurrentState().getNextId();
            String tableName = "imt_" + mvName + seq;
            com.starrocks.sql.ast.QualifiedName qualifiedName = 
                    com.starrocks.sql.ast.QualifiedName.of(java.util.Arrays.asList(stmt.getDbName(), tableName));
            TableRef tableRef = new TableRef(qualifiedName, null, com.starrocks.sql.parser.NodePosition.ZERO);

            // Properties
            Map<String, String> extProperties = Maps.newTreeMap();
            String comment = "IMT for MV StreamAggOperator";

            result.add(new CreateTableStmt(false, false, tableRef, columnDefs,
                    EngineType.defaultEngine().name(), keyDesc, partitionDesc, distributionDesc, properties,
                    extProperties, comment));
            return null;
        }

    }

}
