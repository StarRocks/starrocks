// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/StatementBase.java

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

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.qe.OriginStatement;

import java.util.Collections;
import java.util.List;

public abstract class StatementBase implements ParseNode {

    public enum ExplainLevel {
        NORMAL,
        LOGICAL,
        // True if the describe_stmt print verbose information, if `isVerbose` is true, `isExplain` must be set to true.
        VERBOSE,
        // True if the describe_stmt print costs information, if `isCosts` is true, `isExplain` must be set to true.
        COST,
    }

    private ExplainLevel explainLevel;

    // True if this QueryStmt is the top level query from an EXPLAIN <query>
    protected boolean isExplain = false;

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    // Analyzer that was used to analyze this statement.
    protected Analyzer analyzer;

    // END: Members that need to be reset()
    /////////////////////////////////////////

    private OriginStatement origStmt;

    protected StatementBase() {
    }

    /**
     * C'tor for cloning.
     */
    protected StatementBase(StatementBase other) {
        analyzer = other.analyzer;
        isExplain = other.isExplain;
    }

    /**
     * Analyzes the statement and throws an AnalysisException if analysis fails. A failure
     * could be due to a problem with the statement or because one or more tables/views
     * were missing from the globalStateMgr.
     * It is up to the analysis() implementation to ensure the maximum number of missing
     * tables/views get collected in the Analyzer before failing analyze().
     */
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (isAnalyzed()) {
            return;
        }
        if (isExplain) {
            analyzer.setIsExplain();
        }
        this.analyzer = analyzer;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public boolean isAnalyzed() {
        return analyzer != null;
    }

    public void setIsExplain(boolean isExplain, ExplainLevel explainLevel) {
        this.isExplain = isExplain;
        this.explainLevel = explainLevel;
    }

    public boolean isExplain() {
        return isExplain;
    }

    public ExplainLevel getExplainLevel() {
        if (explainLevel == null) {
            return ExplainLevel.NORMAL;
        } else {
            return explainLevel;
        }
    }

    /*
     * Print SQL syntax corresponding to this node.
     *
     * @see com.starrocks.parser.ParseNode#toSql()
     */
    public String toSql() {
        return "";
    }

    public abstract RedirectStatus getRedirectStatus();

    /**
     * Returns the output column labels of this statement, if applicable, or an empty list
     * if not applicable (not all statements produce an output result set).
     * Subclasses must override this as necessary.
     */
    public List<String> getColLabels() {
        return Collections.<String>emptyList();
    }

    /**
     * Returns the unresolved result expressions of this statement, if applicable, or an
     * empty list if not applicable (not all statements produce an output result set).
     * Subclasses must override this as necessary.
     */
    public List<Expr> getResultExprs() {
        return Collections.<Expr>emptyList();
    }

    public void setOrigStmt(OriginStatement origStmt) {
        Preconditions.checkState(origStmt != null);
        this.origStmt = origStmt;
    }

    public OriginStatement getOrigStmt() {
        return origStmt;
    }

    /**
     * Resets the internal analysis state of this node.
     * For easier maintenance, class members that need to be reset are grouped into
     * a 'section' clearly indicated by comments as follows:
     * <p>
     * class SomeStmt extends StatementBase {
     * ...
     * /////////////////////////////////////////
     * // BEGIN: Members that need to be reset()
     * <p>
     * <member declarations>
     * <p>
     * // END: Members that need to be reset()
     * /////////////////////////////////////////
     * ...
     * }
     * <p>
     * In general, members that are set or modified during analyze() must be reset().
     * TODO: Introduce this same convention for Exprs, possibly by moving clone()/reset()
     * into the ParseNode interface for clarity.
     */
    public void reset() {
        analyzer = null;
    }

    // Override this method and return true
    // if the stmt contains some information which need to be encrypted in audit log
    public boolean needAuditEncryption() {
        return false;
    }
}
