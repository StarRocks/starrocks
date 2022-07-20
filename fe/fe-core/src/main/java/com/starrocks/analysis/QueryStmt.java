// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/QueryStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Abstract base class for any statement that returns results
 * via a list of result expressions, for example a
 * SelectStmt or UnionStmt. Also maintains a map of expression substitutions
 * for replacing expressions from ORDER BY or GROUP BY clauses with
 * their corresponding result expressions.
 * Used for sharing members/methods and some of the analysis code, in particular the
 * analysis of the ORDER BY and LIMIT clauses.
 */
public abstract class QueryStmt extends StatementBase {

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    protected WithClause withClause_;

    protected ArrayList<OrderByElement> orderByElements;
    // Limit element could not be null, the default limit element is NO_LIMIT
    protected LimitElement limitElement;
    // This is a internal element which is used to query plan.
    // It will not change the origin stmt and present in toSql.
    protected AssertNumRowsElement assertNumRowsElement;

    /**
     * For a select statment:
     * List of executable exprs in select clause (star-expanded, ordinals and
     * aliases substituted, agg output substituted
     * For a union statement:
     * List of slotrefs into the tuple materialized by the union.
     */
    protected ArrayList<Expr> resultExprs = Lists.newArrayList();

    // For a select statement: select list exprs resolved to base tbl refs
    // For a union statement: same as resultExprs
    protected ArrayList<Expr> baseTblResultExprs = Lists.newArrayList();

    /**
     * Map of expression substitutions for replacing aliases
     * in "order by" or "group by" clauses with their corresponding result expr.
     */
    protected final ExprSubstitutionMap aliasSMap;

    /**
     * Select list item alias does not have to be unique.
     * This list contains all the non-unique aliases. For example,
     * select int_col a, string_col a from alltypessmall;
     * Both columns are using the same alias "a".
     */
    protected final ArrayList<Expr> ambiguousAliasList;

    protected SortInfo sortInfo;

    // evaluateOrderBy_ is true if there is an order by clause that must be evaluated.
    // False for nested query stmts with an order-by clause without offset/limit.
    // sortInfo_ is still generated and used in analysis to ensure that the order-by clause
    // is well-formed.
    protected boolean evaluateOrderBy;

    protected boolean needToSql = false;

    // used by hll
    protected boolean fromInsert = false;

    // order by elements which has been analyzed
    // For example: select k1 a from t order by a;
    // this parameter: order by t.k1
    protected List<OrderByElement> orderByElementsAfterAnalyzed;

    /////////////////////////////////////////
    // END: Members that need to be reset()

    // represent the "INTO OUTFILE" clause
    protected OutFileClause outFileClause;

    QueryStmt(ArrayList<OrderByElement> orderByElements, LimitElement limitElement) {
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
        this.aliasSMap = new ExprSubstitutionMap();
        this.ambiguousAliasList = Lists.newArrayList();
        this.sortInfo = null;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
    }

    public ArrayList<Expr> getBaseTblResultExprs() {
        return baseTblResultExprs;
    }

    public void setNeedToSql(boolean needToSql) {
        this.needToSql = needToSql;
    }

    /**
     * Return the first expr in exprs that is a non-unique alias. Return null if none of
     * exprs is an ambiguous alias.
     */
    protected Expr getFirstAmbiguousAlias(List<Expr> exprs) {
        for (Expr exp : exprs) {
            if (ambiguousAliasList.contains(exp)) {
                return exp;
            }
        }
        return null;
    }

    /**
     * Substitute exprs of the form "<number>"  with the corresponding
     * expressions and any alias references in aliasSmap_.
     * Modifies exprs list in-place.
     */
    protected void substituteOrdinalsAliases(List<Expr> exprs, String errorPrefix,
                                             Analyzer analyzer) throws AnalysisException {
        Expr ambiguousAlias = getFirstAmbiguousAlias(exprs);
        if (ambiguousAlias != null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NON_UNIQ_ERROR, ambiguousAlias.toColumnLabel());
        }

        ListIterator<Expr> i = exprs.listIterator();
        while (i.hasNext()) {
            Expr expr = i.next();
            // We can substitute either by ordinal or by alias.
            // If we substitute by ordinal, we should not replace any aliases, since
            // the new expression was copied from the select clause context, where
            // alias substitution is not performed in the same way.
            Expr substituteExpr = trySubstituteOrdinal(expr, errorPrefix, analyzer);
            if (substituteExpr == null) {
                substituteExpr = expr.trySubstitute(aliasSMap, analyzer, false);
            }
            i.set(substituteExpr);
        }
    }

    // Attempt to replace an expression of form "<number>" with the corresponding
    // select list items.  Return null if not an ordinal expression.
    private Expr trySubstituteOrdinal(Expr expr, String errorPrefix,
                                      Analyzer analyzer) throws AnalysisException {
        if (!(expr instanceof IntLiteral)) {
            return null;
        }
        expr.analyze(analyzer);
        if (!expr.getType().isIntegerType()) {
            return null;
        }
        long pos = ((IntLiteral) expr).getLongValue();
        if (pos < 1) {
            throw new AnalysisException(
                    errorPrefix + ": ordinal must be >= 1: " + expr.toSql());
        }
        if (pos > resultExprs.size()) {
            throw new AnalysisException(
                    errorPrefix + ": ordinal exceeds number of items in select list: "
                            + expr.toSql());
        }

        // Create copy to protect against accidentally shared state.
        return resultExprs.get((int) pos - 1).clone();
    }

    public void getWithClauseDbs(ConnectContext context, Map<String, Database> dbs) throws AnalysisException {
        if (withClause_ != null) {
            withClause_.getDbs(context, dbs);
        }
    }

    // get database used by this query.
    public abstract void getDbs(ConnectContext context, Map<String, Database> dbs) throws AnalysisException;

    /**
     * UnionStmt and SelectStmt have different implementations.
     */
    public abstract ArrayList<String> getColLabels();

    /**
     * Returns all physical (non-inline-view) TableRefs of this statement and the nested
     * statements of inline views. The returned TableRefs are in depth-first order.
     */
    public abstract void collectTableRefs(List<TableRef> tblRefs);

    public ArrayList<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public void setWithClause(WithClause withClause) {
        this.withClause_ = withClause;
    }

    public boolean hasWithClause() {
        return withClause_ != null;
    }

    public WithClause getWithClause() {
        return withClause_;
    }

    public boolean hasOrderByClause() {
        return orderByElements != null;
    }

    public boolean hasLimit() {
        return limitElement != null && limitElement.hasLimit();
    }

    public boolean hasOffset() {
        return limitElement != null && limitElement.hasOffset();
    }

    public long getLimit() {
        return limitElement.getLimit();
    }

    public void setLimit(long limit) throws AnalysisException {
        Preconditions.checkState(limit >= 0);
        long newLimit = hasLimitClause() ? Math.min(limit, getLimit()) : limit;
        limitElement = new LimitElement(newLimit);
    }

    public long getOffset() {
        return limitElement.getOffset();
    }

    public void setAssertNumRowsElement(int desiredNumOfRows, AssertNumRowsElement.Assertion assertion) {
        this.assertNumRowsElement = new AssertNumRowsElement(desiredNumOfRows, toSql(), assertion);
    }

    public void setIsExplain(boolean isExplain) {
        this.isExplain = isExplain;
    }

    public boolean isExplain() {
        return isExplain;
    }

    public boolean hasLimitClause() {
        return limitElement.hasLimit();
    }

    public LimitElement getLimitClause() {
        return limitElement;
    }

    public ArrayList<Expr> getResultExprs() {
        return resultExprs;
    }


    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public ArrayList<OrderByElement> cloneOrderByElements() {
        if (orderByElements == null) {
            return null;
        }
        ArrayList<OrderByElement> result =
                Lists.newArrayListWithCapacity(orderByElements.size());
        for (OrderByElement o : orderByElements) {
            result.add(o.clone());
        }
        return result;
    }

    public WithClause cloneWithClause() {
        return withClause_ != null ? withClause_.clone() : null;
    }

    public OutFileClause cloneOutfileCluse() {
        return outFileClause != null ? outFileClause.clone() : null;
    }

    public String toDigest() {
        return "";
    }

    /**
     * C'tor for cloning.
     */
    protected QueryStmt(QueryStmt other) {
        super(other);
        withClause_ = other.cloneWithClause();
        outFileClause = other.cloneOutfileCluse();
        orderByElements = other.cloneOrderByElements();
        limitElement = other.limitElement.clone();
        resultExprs = Expr.cloneList(other.resultExprs);
        baseTblResultExprs = Expr.cloneList(other.baseTblResultExprs);
        aliasSMap = other.aliasSMap.clone();
        ambiguousAliasList = Expr.cloneList(other.ambiguousAliasList);
        sortInfo = (other.sortInfo != null) ? other.sortInfo.clone() : null;
        analyzer = other.analyzer;
        evaluateOrderBy = other.evaluateOrderBy;
    }

    @Override
    public void reset() {
        super.reset();
        if (orderByElements != null) {
            for (OrderByElement o : orderByElements) {
                o.getExpr().reset();
            }
        }
        limitElement.reset();
        resultExprs.clear();
        baseTblResultExprs.clear();
        aliasSMap.clear();
        ambiguousAliasList.clear();
        orderByElementsAfterAnalyzed = null;
        sortInfo = null;
        evaluateOrderBy = false;
        fromInsert = false;
    }

    public void setFromInsert(boolean value) {
        this.fromInsert = value;
    }

    @Override
    public abstract QueryStmt clone();

    public void setOutFileClause(OutFileClause outFileClause) {
        this.outFileClause = outFileClause;
    }

    public boolean hasOutFileClause() {
        return outFileClause != null;
    }
}
