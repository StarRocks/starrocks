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

package com.starrocks.catalog.system.function;

import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.AnalyzeExclusion;
import com.starrocks.statistic.ExternalAnalyzeExclusion;
import com.starrocks.statistic.NativeAnalyzeExclusion;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

public class CboStatsAddExclusion implements GenericFunction {
    public static final String FN_NAME = FunctionSet.CBO_STATS_ADD_EXCLUSION;
    public static final String FULL = "FULL";
    public static final String SAMPLE = "SAMPLE";

    @Override
    public void init(FunctionCallExpr node, ConnectContext context) {
        genericSystemFunctionCheck(node);
        if (node.getChildren().size() != 2) {
            throw new SemanticException(FN_NAME + " input parameter must be 4", node.getPos());
        }

        if (!(node.getChild(0) instanceof StringLiteral)
                || !(node.getChild(1) instanceof StringLiteral)) {
            throw new SemanticException(FN_NAME + " input parameter must be string literal",
                    node.getPos());
        }

        String antiQualifiedName = ((StringLiteral) node.getChild(0)).getStringValue();
        String sampleType = ((StringLiteral) node.getChild(1)).getStringValue();

        if (!(sampleType.toUpperCase().equals(FULL)
                || sampleType.toUpperCase().equals(SAMPLE))) {
            throw new SemanticException(FN_NAME + " sample type must be FULL or SAMPLE", node.getPos());
        }
        List<String> dbMeta = AnalyzerUtils.stringToDbMeta(antiQualifiedName);
        normalizeMeta(antiQualifiedName, node.getChild(0).getPos(),
                sampleType, node.getChild(1).getPos(), dbMeta);
    }

    @Override
    public void prepare(FunctionCallExpr node, ConnectContext context) {
        String antiQualifiedName = ((StringLiteral) node.getChild(0)).getStringValue();
        List<String> dbMeta = AnalyzerUtils.stringToDbMeta(antiQualifiedName);
        List<Long> metaIds = normalizeStrMetaToNumberMeta(dbMeta);
        Set<TableName> tableNames = AnalyzerUtils.getAllTableNames(metaIds.get(0), metaIds.get(1));
        tableNames.forEach(tableName -> Authorizer.checkTablePrivilege(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), tableName));
    }

    @Override
    public ConstantOperator evaluate(List<ConstantOperator> arguments) {
        String antiQualifiedName = arguments.get(0).getVarchar();
        String sampleType = arguments.get(1).getVarchar();
        List<String> dbMeta = AnalyzerUtils.stringToDbMeta(antiQualifiedName);
        List<Long> metaIds = normalizeStrMetaToNumberMeta(dbMeta);

        AnalyzeExclusion[] analyzeExclusionArr = new AnalyzeExclusion[1];
        ErrorReport.wrapWithRuntimeException(() -> {
            if (!CatalogMgr.isExternalCatalog(dbMeta.get(0))) {
                analyzeExclusionArr[0] = new NativeAnalyzeExclusion(metaIds.get(0), metaIds.get(1),
                        sampleType.toUpperCase().equals(SAMPLE) ?
                                StatsConstants.AnalyzeType.SAMPLE : StatsConstants.AnalyzeType.FULL);
            } else {
                analyzeExclusionArr[0] = new ExternalAnalyzeExclusion(dbMeta.get(0), dbMeta.get(1), dbMeta.get(2),
                        sampleType.toUpperCase().equals(SAMPLE) ?
                                StatsConstants.AnalyzeType.SAMPLE : StatsConstants.AnalyzeType.FULL);
            }
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeExclusion(analyzeExclusionArr[0]);
        });

        return ConstantOperator.createVarchar(Long.toString(analyzeExclusionArr[0].getId()));
    }

    private List<Long> normalizeStrMetaToNumberMeta(List<String> meta) {
        List<Long> metaIds = new ArrayList<>();
        //catalog.db.table
        if (meta.get(2) != null) {
            Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(meta.get(0), meta.get(1), meta.get(2));
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(meta.get(0), meta.get(1));
            metaIds.addAll(Arrays.asList(db.getId(), tbl.getId()));
        }

        //catalog.db
        if (meta.get(1) != null) {
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(meta.get(0), meta.get(1));
            metaIds.addAll(Arrays.asList(db.getId(), StatsConstants.DEFAULT_ALL_ID));
        } else {
            //catalog
            metaIds.addAll(Arrays.asList(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID));
        }

        return metaIds;
    }

    private void normalizeMeta(String metaName, NodePosition metaPos,
                               String sampleType, NodePosition typePos, List<String> meta) {
        Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(meta.get(0));
        if (catalog == null) {
            throw new SemanticException(format("Catalog %s is not found", metaName), metaPos);
        }

        if (CatalogMgr.isExternalCatalog(meta.get(0))) {
            //check external catalog
            if (sampleType == SAMPLE) {
                throw new SemanticException("External table don't support SAMPLE analyze", typePos);
            }

            //check catalog.db.tbl
            if (meta.get(1) == null || meta.get(2) == null) {
                throw new SemanticException("For automatic collection tasks, " +
                        "you can only collect statistics of a specific table. " +
                        "You cannot collect statistics of all tables in a database " +
                        "or statistics of all databases in an external catalog.", typePos);
            }

            Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(meta.get(0), meta.get(1), meta.get(2));
            if (tbl == null) {
                throw new SemanticException(format("Table %s is not found", metaName), metaPos);
            }

            if (!tbl.isHiveTable() && !tbl.isIcebergTable() && !tbl.isHudiTable() &&
                    !tbl.isOdpsTable()) {
                throw new SemanticException("Analyze exclusion external table only support " +
                        "hive, iceberg, hudi and odps table", metaPos);
            }
        }

        //check catalog.db
        if (meta.get(1) != null) {
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(meta.get(0), meta.get(1));
            if (db == null) {
                throw new SemanticException(format("DataBase %s is not found", metaName), metaPos);
            }

            if (meta.get(2) == null) {
                if (StatisticUtils.statisticDatabaseBlackListCheck(meta.get(1))) {
                    throw new SemanticException(format("Forbidden collect database: %s", meta.get(1)), metaPos);
                }
            }
        }

        //check catalog.db.tbl
        if (meta.get(2) != null) {
            Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(meta.get(0), meta.get(1), meta.get(2));
            if (tbl == null) {
                throw new SemanticException(format("Table %s is not found", metaName), metaPos);
            }
        }
    }
}
