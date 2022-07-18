// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/InlineViewRef.java

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
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InlineView;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An inline view is a query statement with an alias. Inline views can be parsed directly
 * from a query string or represent a reference to a local or globalStateMgr view.
 */
public class InlineViewRef extends TableRef {

    // GlobalStateMgr or local view that is referenced.
    // Null for inline views parsed directly from a query string.
    private final View view;

    // If not null, these will serve as the column labels for the inline view. This provides
    // a layer of separation between column labels visible from outside the inline view
    // and column labels used in the query definition. Either all or none of the column
    // labels must be overridden.
    private List<String> explicitColLabels;

    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()

    // The select or union statement of the inline view
    private QueryStmt queryStmt;

    // queryStmt has its own analysis context
    private Analyzer inlineViewAnalyzer;

    // list of tuple ids materialized by queryStmt
    private final ArrayList<TupleId> materializedTupleIds = Lists.newArrayList();

    // Map inline view's output slots to the corresponding resultExpr of queryStmt.
    protected final ExprSubstitutionMap sMap;

    // Map inline view's output slots to the corresponding baseTblResultExpr of queryStmt.
    protected final ExprSubstitutionMap baseTblSmap;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    /**
     * C'tor for creating inline views parsed directly from the a query string.
     */
    public InlineViewRef(String alias, QueryStmt queryStmt) {
        super(null, alias);
        this.queryStmt = queryStmt;
        this.view = null;
        sMap = new ExprSubstitutionMap();
        baseTblSmap = new ExprSubstitutionMap();
    }

    public InlineViewRef(String alias, QueryStmt queryStmt, List<String> colLabels) {
        this(alias, queryStmt);
        explicitColLabels = Lists.newArrayList(colLabels);
    }

    /**
     * C'tor for creating inline views that replace a local or globalStateMgr view ref.
     */
    public InlineViewRef(View view, TableRef origTblRef) {
        super(origTblRef.getName(), origTblRef.getExplicitAlias());
        queryStmt = view.getQueryStmt().clone();
        if (view.isLocalView()) {
            queryStmt.reset();
        }
        this.view = view;
        sMap = new ExprSubstitutionMap();
        baseTblSmap = new ExprSubstitutionMap();
        setJoinAttrs(origTblRef);
        explicitColLabels = view.getColLabels();
        // Set implicit aliases if no explicit one was given.
        if (hasExplicitAlias()) {
            return;
        }
        // TODO(zc)
        // view_.getTableName().toString().toLowerCase(), view.getName().toLowerCase()
        if (view.isLocalView()) {
            aliases_ = new String[] {view.getName()};
        } else {
            aliases_ = new String[] {name.toString(), view.getName()};
        }
    }

    protected InlineViewRef(InlineViewRef other) {
        super(other);
        queryStmt = other.queryStmt.clone();
        view = other.view;
        inlineViewAnalyzer = other.inlineViewAnalyzer;
        if (other.explicitColLabels != null) {
            explicitColLabels = Lists.newArrayList(other.explicitColLabels);
        }
        materializedTupleIds.addAll(other.materializedTupleIds);
        sMap = other.sMap.clone();
        baseTblSmap = other.baseTblSmap.clone();
    }

    public List<String> getColLabels() {
        if (explicitColLabels != null) {
            return explicitColLabels;
        }
        return queryStmt.getColLabels();
    }

    @Override
    public void reset() {
        super.reset();
        queryStmt.reset();
        inlineViewAnalyzer = null;
        materializedTupleIds.clear();
        sMap.clear();
        baseTblSmap.clear();
    }

    @Override
    public TableRef clone() {
        return new InlineViewRef(this);
    }

    public void setNeedToSql(boolean needToSql) {
        queryStmt.setNeedToSql(needToSql);
    }

    /**
     * Analyzes the inline view query block in a child analyzer of 'analyzer', creates
     * a new tuple descriptor for the inline view and registers auxiliary eq predicates
     * between the slots of that descriptor and the select list exprs of the inline view;
     * then performs join clause analysis.
     */
    @Override
    public void analyze(Analyzer analyzer) throws UserException {
    }

    /**
     * Create a non-materialized tuple descriptor in descTbl for this inline view.
     * This method is called from the analyzer when registering this inline view.
     */
    @Override
    public TupleDescriptor createTupleDescriptor(Analyzer analyzer) throws AnalysisException {
        // Create a fake globalStateMgr table for the inline view
        int numColLabels = getColLabels().size();
        Preconditions.checkState(numColLabels > 0);
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        List<Column> columnList = Lists.newArrayList();
        for (int i = 0; i < numColLabels; ++i) {
            // inline view select statement has been analyzed. Col label should be filled.
            Expr selectItemExpr = queryStmt.getResultExprs().get(i);
            // String colAlias = queryStmt.getColLabels().get(i);
            String colAlias = getColLabels().get(i);

            // inline view col cannot have duplicate name
            if (columnSet.contains(colAlias)) {
                throw new AnalysisException(
                        "Duplicated inline view column alias: '" + colAlias + "'" + " in inline view "
                                + "'" + getAlias() + "'");
            }

            columnSet.add(colAlias);
            columnList.add(new Column(colAlias, selectItemExpr.getType(), selectItemExpr.isNullable()));
        }
        InlineView inlineView =
                (view != null) ? new InlineView(view, columnList) : new InlineView(getExplicitAlias(), columnList);

        // Create the non-materialized tuple and set the fake table in it.
        TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor();
        result.setIsMaterialized(false);
        result.setTable(inlineView);
        return result;
    }

    public QueryStmt getViewStmt() {
        return queryStmt;
    }

    public Analyzer getAnalyzer() {
        Preconditions.checkState(isAnalyzed);
        return inlineViewAnalyzer;
    }

    public ExprSubstitutionMap getSmap() {
        Preconditions.checkState(isAnalyzed);
        return sMap;
    }

    @Override
    public String tableRefToSql() {
        // Enclose the alias in quotes if Hive cannot parse it without quotes.
        // This is needed for view compatibility between Impala and Hive.
        String aliasSql = null;
        String alias = getExplicitAlias();
        if (alias != null) {
            aliasSql = ToSqlUtils.getIdentSql(alias);
        }
        if (view != null) {
            // TODO(zc):
            // return view_.toSql() + (aliasSql == null ? "" : " " + aliasSql);
            return name.toSql() + (aliasSql == null ? "" : " " + aliasSql);
        }

        StringBuilder sb = new StringBuilder()
                .append("(")
                .append(queryStmt.toSql())
                .append(") ")
                .append(aliasSql);

        return sb.toString();
    }
}
