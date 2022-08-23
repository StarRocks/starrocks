// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class MVUtils {

    private static final Logger LOG = LogManager.getLogger(MVUtils.class);

    public static boolean isEquivalencePredicate(ScalarOperator predicate) {
        if (predicate instanceof InPredicateOperator) {
            return true;
        }
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
            return binary.getBinaryType().isEquivalence();
        }
        return false;
    }

    public static boolean isPredicateUsedForPrefixIndex(ScalarOperator predicate) {
        if (!(predicate instanceof InPredicateOperator)
                && !(predicate instanceof BinaryPredicateOperator)) {
            return false;
        }
        if (predicate instanceof InPredicateOperator) {
            return isInPredicateUsedForPrefixIndex((InPredicateOperator) predicate);
        } else {
            return isBinaryPredicateUsedForPrefixIndex((BinaryPredicateOperator) predicate);
        }
    }

    private static boolean isInPredicateUsedForPrefixIndex(InPredicateOperator predicate) {
        if (predicate.isNotIn()) {
            return false;
        }
        return isColumnRefNested(predicate.getChild(0)) && predicate.allValuesMatch(ScalarOperator::isConstant);
    }

    private static boolean isBinaryPredicateUsedForPrefixIndex(BinaryPredicateOperator predicate) {
        if (predicate.getBinaryType().isNotEqual()) {
            return false;
        }
        return (isColumnRefNested(predicate.getChild(0)) && predicate.getChild(1).isConstant())
                || (isColumnRefNested(predicate.getChild(1)) && predicate.getChild(0).isConstant());
    }

    private static boolean isColumnRefNested(ScalarOperator operator) {
        while (operator instanceof CastOperator) {
            operator = operator.getChild(0);
        }
        return operator.isColumnRef();
    }

    public static String getMVColumnName(Column mvColumn, String functionName, String queryColumn) {
        // Support count(column) MV
        // The origin MV design is bad !!!
        if (mvColumn.getDefineExpr() instanceof CaseExpr && functionName.equals(FunctionSet.COUNT)) {
            return "mv_" + FunctionSet.COUNT + "_" + queryColumn;
        }
        return "mv_" + mvColumn.getAggregationType().name().toLowerCase() + "_" + queryColumn;
    }

    public static Map<String, Expr> parseColumnNameToDefineExpr(OriginStatement originStmt) throws IOException {
        CreateMaterializedViewStmt stmt;

        try {
            List<StatementBase> stmts = SqlParser.parse(originStmt.originStmt, SqlModeHelper.MODE_DEFAULT);
            stmt = (CreateMaterializedViewStmt) stmts.get(originStmt.idx);
            stmt.setIsReplay(true);
            return stmt.parseDefineExprWithoutAnalyze(originStmt.originStmt);
        } catch (Exception e) {
            LOG.warn("error happens when parsing create materialized view use new parser:" + originStmt, e);
        }

        // compatibility old parser can work but new parser failed
        com.starrocks.analysis.SqlParser parser = new com.starrocks.analysis.SqlParser(
                new SqlScanner(new StringReader(originStmt.originStmt),
                        SqlModeHelper.MODE_DEFAULT));
        try {
            stmt = (CreateMaterializedViewStmt) SqlParserUtils.getStmt(parser, originStmt.idx);
            stmt.setIsReplay(true);
            return stmt.parseDefineExprWithoutAnalyze(originStmt.originStmt);
        } catch (Exception e) {
            throw new IOException("error happens when parsing create materialized view stmt use old parser:" +
                    originStmt, e);
        }
    }

}
