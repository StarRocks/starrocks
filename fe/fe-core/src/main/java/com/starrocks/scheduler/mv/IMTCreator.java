// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.CreateTableAnalyzer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.mv.KeyInference;
import com.starrocks.sql.optimizer.rule.mv.MVOperatorProperty;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create Intermediate-Materialized-Table for MV
 */
class IMTCreator {
    private static final Logger LOG = LogManager.getLogger(IMTCreator.class);

    private static final IMTCreator INSTANCE = new IMTCreator();

    public static void createIMT(CreateMaterializedViewStatement stmt, MaterializedView view) throws DdlException {
        ExecPlan maintenancePlan = stmt.getMaintenancePlan();
        ColumnRefFactory columnRefFactory = stmt.getColumnRefFactory();
        OptExpression optExpr = maintenancePlan.getPhysicalPlan();
        List<OptExpression> streamOperators = new ArrayList<>();
        collectStreamOperators(optExpr, streamOperators);

        List<CreateTableStmt> createTables =
                streamOperators.stream().map(op -> createIMTForOperator(stmt, op, view)).collect(Collectors.toList());
        for (CreateTableStmt create : createTables) {
            LOG.info("creating IMT {} for MV {}", create.getTableName(), view.getName());
            try {
                GlobalStateMgr.getCurrentState().createTable(create);
            } catch (DdlException e) {
                // TODO(murphy) cleanup created IMT, or make it atomic
                LOG.info("create IMT {} failed due to ", create.getTableName(), e);
                throw e;
            }
        }
    }

    private static CreateTableStmt createIMTForOperator(CreateMaterializedViewStatement stmt, OptExpression opt,
                                                        MaterializedView view) {
        PhysicalStreamOperator streamOp = (PhysicalStreamOperator) opt.getOp();
        MVOperatorProperty property = opt.getMvOperatorProperty();
        ColumnRefFactory columnRefFactory = stmt.getColumnRefFactory();
        Map<String, String> properties = view.getProperties();

        // Duplicate/Primary Key
        KeyInference.KeyProperty bestKey = property.getKeySet().getBestKey();

        // Columns
        List<Column> columns = new ArrayList<>();
        List<Column> keyColumns = new ArrayList<>();
        List<ColumnDef> columnDefs = new ArrayList<>();
        ColumnRefSet columnRefSet = opt.getOutputColumns();
        for (int columnId : columnRefSet.getColumnIds()) {
            ColumnRefOperator refOp = columnRefFactory.getColumnRef(columnId);

            Column newColumn = new Column(refOp.getName(), refOp.getType());
            boolean isKey = bestKey.columns.contains(refOp);
            newColumn.setIsKey(isKey);
            newColumn.setIsAllowNull(false);
            columns.add(newColumn);
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
        KeysType keyType = property.getModify().isInsertOnly() ? KeysType.DUP_KEYS : KeysType.PRIMARY_KEYS;
        KeysDesc keyDesc = new KeysDesc(keyType, keyColumns.stream().map(x -> x.getName()).collect(Collectors.toList()));

        // Partition scheme
        // TODO(murphy) support partition the IMT, current we don't support it
        PartitionDesc partitionDesc = new PartitionDesc();

        // Distribute Key
        DistributionDesc distributionDesc = new DistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        HashDistributionInfo distributionInfo = new HashDistributionInfo();
        distributionInfo.setBucketNum(MVManager.calBucketNumAccordingToBackends());
        distributionInfo.setDistributionColumns(keyColumns);

        // Table name: imt_mvname_operatorid_seq;
        // TODO(murphy) refine it
        String mvName = stmt.getTableName().getTbl();
        long seq = GlobalStateMgr.getCurrentState().getNextId();
        String tableName = "imt_" + mvName + seq;
        TableName canonicalName = new TableName(stmt.getTableName().getDb(), tableName);

        // Properties
        Map<String, String> extProperties = Maps.newTreeMap();
        String comment = "IMT for MV";

        return new CreateTableStmt(false, false, canonicalName, columnDefs,
                CreateTableAnalyzer.EngineType.OLAP.name(), keyDesc, partitionDesc, distributionDesc, properties,
                extProperties, comment);
    }

    private static void collectStreamOperators(OptExpression root, List<OptExpression> output) {
        if (root.getOp() instanceof PhysicalStreamOperator) {
            output.add(root);
        }
        root.getInputs().forEach(child -> collectStreamOperators(child, output));
    }
}
