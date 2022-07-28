// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SetOperationStmt.java

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
import com.starrocks.qe.ConnectContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Representation of a set ops with its list of operands, and optional order by and limit.
 * A set ops materializes its results, and its resultExprs are SlotRefs into a new
 * materialized tuple.
 * During analysis, the operands are normalized (separated into a single sequence of
 * DISTINCT followed by a single sequence of ALL operands) and unnested to the extent
 * possible. This also creates the AggregationInfo for DISTINCT operands.
 * <p>
 * Use of resultExprs vs. baseTblResultExprs:
 * We consistently use/cast the resultExprs of set operands because the final expr
 * substitution happens during planning. The only place where baseTblResultExprs are
 * used is in materializeRequiredSlots() because that is called before plan generation
 * and we need to mark the slots of resolved exprs as materialized.
 */
public class SetOperationStmt extends QueryStmt {
    public enum Operation {
        UNION,
        INTERSECT,
        EXCEPT
    }

    public enum Qualifier {
        ALL,
        DISTINCT
    }

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    // before analysis, this contains the list of set operands derived verbatim
    // from the query;
    // after analysis, this contains all of distinctOperands followed by allOperands
    private final List<SetOperand> operands;

    // filled during analyze(); contains all operands that need to go through
    // distinct aggregation
    protected final List<SetOperand> distinctOperands_ = Lists.newArrayList();

    // filled during analyze(); contains all operands that can be aggregated with
    // a simple merge without duplicate elimination (also needs to merge the output
    // of the DISTINCT operands)
    protected final List<SetOperand> allOperands_ = Lists.newArrayList();

    private AggregateInfo distinctAggInfo;  // only set if we have DISTINCT ops

    // Single tuple materialized by the set operation. Set in analyze().
    private TupleId tupleId;

    // set prior to unnesting
    private String toSqlString;

    // true if any of the operands_ references an AnalyticExpr
    private boolean hasAnalyticExprs_ = false;

    // List of output expressions produced by the set operation without the ORDER BY portion
    // (if any). Same as resultExprs_ if there is no ORDER BY.
    private List<Expr> setOpsResultExprs_ = Lists.newArrayList();

    // END: Members that need to be reset()
    /////////////////////////////////////////

    public SetOperationStmt(
            List<SetOperand> operands,
            ArrayList<OrderByElement> orderByElements,
            LimitElement limitElement) {
        super(orderByElements, limitElement);
        this.operands = operands;
    }

    /**
     * C'tor for cloning.
     */
    protected SetOperationStmt(SetOperationStmt other) {
        super(other.cloneOrderByElements(),
                (other.limitElement == null) ? null : other.limitElement.clone());
        operands = Lists.newArrayList();
        if (analyzer != null) {
            for (SetOperand o : other.distinctOperands_) {
                distinctOperands_.add(o.clone());
            }
            for (SetOperand o : other.allOperands_) {
                allOperands_.add(o.clone());
            }
            operands.addAll(distinctOperands_);
            operands.addAll(allOperands_);
        } else {
            for (SetOperand operand : other.operands) {
                operands.add(operand.clone());
            }
        }
        analyzer = other.analyzer;
        distinctAggInfo =
                (other.distinctAggInfo != null) ? other.distinctAggInfo.clone() : null;
        tupleId = other.tupleId;
        toSqlString = (other.toSqlString != null) ? new String(other.toSqlString) : null;
        hasAnalyticExprs_ = other.hasAnalyticExprs_;
        withClause_ = (other.withClause_ != null) ? other.withClause_.clone() : null;
        setOpsResultExprs_ = Expr.cloneList(other.setOpsResultExprs_);
    }

    @Override
    public SetOperationStmt clone() {
        return new SetOperationStmt(this);
    }

    /**
     * Undoes all changes made by analyze() except distinct propagation and unnesting.
     * After analysis, operands_ contains the list of unnested operands with qualifiers
     * adjusted to reflect distinct propagation. Every operand in that list is reset().
     * The distinctOperands_ and allOperands_ are cleared because they are redundant
     * with operands_.
     */
    @Override
    public void reset() {
        super.reset();
        for (SetOperand op : operands) {
            op.reset();
        }
        distinctOperands_.clear();
        allOperands_.clear();
        distinctAggInfo = null;
        tupleId = null;
        toSqlString = null;
        hasAnalyticExprs_ = false;
        setOpsResultExprs_.clear();
    }

    public List<SetOperand> getOperands() {
        return operands;
    }

    @Override
    public void getDbs(ConnectContext context, Map<String, Database> dbs) throws AnalysisException {
        getWithClauseDbs(context, dbs);
        for (SetOperand op : operands) {
            op.getQueryStmt().getDbs(context, dbs);
        }
    }

    @Override
    public void collectTableRefs(List<TableRef> tblRefs) {
        for (SetOperand op : operands) {
            op.getQueryStmt().collectTableRefs(tblRefs);
        }
    }

    @Override
    public String toSql() {
        if (toSqlString != null) {
            return toSqlString;
        }
        Preconditions.checkState(operands.size() > 0);

        StringBuilder strBuilder = new StringBuilder();
        if (withClause_ != null) {
            strBuilder.append(withClause_.toSql());
            strBuilder.append(" ");
        }

        strBuilder.append(operands.get(0).getQueryStmt().toSql());
        for (int i = 1; i < operands.size() - 1; ++i) {
            strBuilder.append(
                    " " + operands.get(i).getOperation().toString() + " "
                            + ((operands.get(i).getQualifier() == Qualifier.ALL) ? "ALL " : ""));
            if (operands.get(i).getQueryStmt() instanceof SetOperationStmt) {
                strBuilder.append("(");
            }
            strBuilder.append(operands.get(i).getQueryStmt().toSql());
            if (operands.get(i).getQueryStmt() instanceof SetOperationStmt) {
                strBuilder.append(")");
            }
        }
        // Determine whether we need parenthesis around the last Set operand.
        SetOperand lastOperand = operands.get(operands.size() - 1);
        QueryStmt lastQueryStmt = lastOperand.getQueryStmt();
        strBuilder.append(" " + lastOperand.getOperation().toString() + " "
                + ((lastOperand.getQualifier() == Qualifier.ALL) ? "ALL " : ""));
        if (lastQueryStmt instanceof SetOperationStmt || ((hasOrderByClause() || hasLimitClause()) &&
                !lastQueryStmt.hasLimitClause() &&
                !lastQueryStmt.hasOrderByClause())) {
            strBuilder.append("(");
            strBuilder.append(lastQueryStmt.toSql());
            strBuilder.append(")");
        } else {
            strBuilder.append(lastQueryStmt.toSql());
        }
        // Order By clause
        if (hasOrderByClause()) {
            strBuilder.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                strBuilder.append(orderByElements.get(i).getExpr().toSql());
                strBuilder.append(orderByElements.get(i).getIsAsc() ? " ASC" : " DESC");
                strBuilder.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }
        // Limit clause.
        if (hasLimitClause()) {
            strBuilder.append(limitElement.toSql());
        }
        return strBuilder.toString();
    }

    @Override
    public String toDigest() {
        StringBuilder strBuilder = new StringBuilder();
        if (withClause_ != null) {
            strBuilder.append(withClause_.toDigest());
            strBuilder.append(" ");
        }

        strBuilder.append(operands.get(0).getQueryStmt().toDigest());
        for (int i = 1; i < operands.size() - 1; ++i) {
            strBuilder.append(
                    " " + operands.get(i).getOperation().toString() + " "
                            + ((operands.get(i).getQualifier() == Qualifier.ALL) ? "all " : ""));
            if (operands.get(i).getQueryStmt() instanceof SetOperationStmt) {
                strBuilder.append("(");
            }
            strBuilder.append(operands.get(i).getQueryStmt().toDigest());
            if (operands.get(i).getQueryStmt() instanceof SetOperationStmt) {
                strBuilder.append(")");
            }
        }
        // Determine whether we need parenthesis around the last Set operand.
        SetOperand lastOperand = operands.get(operands.size() - 1);
        QueryStmt lastQueryStmt = lastOperand.getQueryStmt();
        strBuilder.append(" " + lastOperand.getOperation().toString() + " "
                + ((lastOperand.getQualifier() == Qualifier.ALL) ? "all " : ""));
        if (lastQueryStmt instanceof SetOperationStmt || ((hasOrderByClause() || hasLimitClause()) &&
                !lastQueryStmt.hasLimitClause() &&
                !lastQueryStmt.hasOrderByClause())) {
            strBuilder.append("(");
            strBuilder.append(lastQueryStmt.toDigest());
            strBuilder.append(")");
        } else {
            strBuilder.append(lastQueryStmt.toDigest());
        }
        // Order By clause
        if (hasOrderByClause()) {
            strBuilder.append(" order by ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                strBuilder.append(orderByElements.get(i).getExpr().toDigest());
                strBuilder.append(orderByElements.get(i).getIsAsc() ? " aec" : " desc");
                strBuilder.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }
        // Limit clause.
        if (hasLimitClause()) {
            strBuilder.append(limitElement.toDigest());
        }
        return strBuilder.toString();
    }

    @Override
    public ArrayList<String> getColLabels() {
        Preconditions.checkState(operands.size() > 0);
        return operands.get(0).getQueryStmt().getColLabels();
    }

    @Override
    public void setNeedToSql(boolean needToSql) {
        super.setNeedToSql(needToSql);
        for (SetOperand operand : operands) {
            operand.getQueryStmt().setNeedToSql(needToSql);
        }
    }

    /**
     * Represents an operand to a SetOperand. It consists of a query statement and its left
     * all/distinct qualifier (null for the first operand).
     */
    public static class SetOperand {
        // Operand indicate this SetOperand is union/intersect/except
        private Operation operation;

        // Effective qualifier. Should not be reset() to preserve changes made during
        // distinct propagation and unnesting that are needed after rewriting Subqueries.
        private Qualifier qualifier_;

        // ///////////////////////////////////////
        // BEGIN: Members that need to be reset()

        private QueryStmt queryStmt;

        // Analyzer used for this operand. Set in analyze().
        // We must preserve the conjuncts registered in the analyzer for partition pruning.
        private Analyzer analyzer;

        // Map from SetOperationStmt's result slots to our resultExprs. Used during plan generation.
        private final ExprSubstitutionMap smap_;

        // END: Members that need to be reset()
        // ///////////////////////////////////////

        public SetOperand(QueryStmt queryStmt, Operation operation, Qualifier qualifier) {
            this.queryStmt = queryStmt;
            this.operation = operation;
            qualifier_ = qualifier;
            smap_ = new ExprSubstitutionMap();
        }

        public boolean isAnalyzed() {
            return analyzer != null;
        }

        public QueryStmt getQueryStmt() {
            return queryStmt;
        }

        public Qualifier getQualifier() {
            return qualifier_;
        }

        public Operation getOperation() {
            return operation;
        }

        // Used for propagating DISTINCT.
        public void setQualifier(Qualifier qualifier) {
            qualifier_ = qualifier;
        }

        public void setOperation(Operation operation) {
            this.operation = operation;
        }

        public void setQueryStmt(QueryStmt queryStmt) {
            this.queryStmt = queryStmt;
        }

        public Analyzer getAnalyzer() {
            return analyzer;
        }

        public ExprSubstitutionMap getSmap() {
            return smap_;
        }

        /**
         * C'tor for cloning.
         */
        private SetOperand(SetOperand other) {
            queryStmt = other.queryStmt.clone();
            this.operation = other.operation;
            qualifier_ = other.qualifier_;
            analyzer = other.analyzer;
            smap_ = other.smap_.clone();
        }

        public void reset() {
            queryStmt.reset();
            analyzer = null;
            smap_.clear();
        }

        @Override
        public SetOperand clone() {
            return new SetOperand(this);
        }
    }
}
