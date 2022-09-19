// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AnalyzeTypeDesc;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExportAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new ExportAnalyzerVisitor().analyze(statement, session);
    }

    static class ExportAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCancelExportStatement(CancelExportStmt statement, ConnectContext session) {

        }
    }
}
