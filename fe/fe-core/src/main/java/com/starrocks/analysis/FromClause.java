// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/FromClause.java

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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Wraps a list of TableRef instances that form a FROM clause, allowing them to be
 * analyzed independently of the statement using them. To increase the flexibility of
 * the class it implements the Iterable interface.
 */
public class FromClause implements ParseNode, Iterable<TableRef> {

    private final ArrayList<TableRef> tableRefs_;

    private boolean analyzed_ = false;
    private boolean needToSql = false;

    public FromClause(List<TableRef> tableRefs) {
        tableRefs_ = Lists.newArrayList(tableRefs);
        // Set left table refs to ensure correct toSql() before analysis.
        for (int i = 1; i < tableRefs_.size(); ++i) {
            tableRefs_.get(i).setLeftTblRef(tableRefs_.get(i - 1));
        }
    }

    public FromClause() {
        tableRefs_ = Lists.newArrayList();
    }

    public List<TableRef> getTableRefs() {
        return tableRefs_;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (analyzed_) {
            return;
        }

        if (tableRefs_.isEmpty()) {
            analyzed_ = true;
            return;
        }

        // Start out with table refs to establish aliases.
        TableRef leftTblRef = null;  // the one to the left of tblRef
        for (int i = 0; i < tableRefs_.size(); ++i) {
            // Resolve and replace non-InlineViewRef table refs with a BaseTableRef or ViewRef.
            TableRef tblRef = tableRefs_.get(i);
            tblRef = analyzer.resolveTableRef(tblRef);
            tableRefs_.set(i, Preconditions.checkNotNull(tblRef));
            tblRef.setLeftTblRef(leftTblRef);
            if (tblRef instanceof InlineViewRef) {
                ((InlineViewRef) tblRef).setNeedToSql(needToSql);
            }
            tblRef.analyze(analyzer);
            leftTblRef = tblRef;
        }

        analyzed_ = true;
    }

    public FromClause clone() {
        ArrayList<TableRef> clone = Lists.newArrayList();
        for (TableRef tblRef : tableRefs_) {
            clone.add(tblRef.clone());
        }
        return new FromClause(clone);
    }

    public void reset() {
        for (int i = 0; i < size(); ++i) {
            TableRef origTblRef = get(i);
            get(i).reset();
        }
        this.analyzed_ = false;
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        if (!tableRefs_.isEmpty()) {
            builder.append(" FROM");
            for (int i = 0; i < tableRefs_.size(); ++i) {
                builder.append(" " + tableRefs_.get(i).toSql());
            }
        }
        return builder.toString();
    }

    public String toDigest() {
        StringBuilder builder = new StringBuilder();
        if (!tableRefs_.isEmpty()) {
            builder.append(" from");
            for (int i = 0; i < tableRefs_.size(); ++i) {
                builder.append(" " + tableRefs_.get(i).toSql());
            }
        }
        return builder.toString();
    }

    public boolean isEmpty() {
        return tableRefs_.isEmpty();
    }

    @Override
    public Iterator<TableRef> iterator() {
        return tableRefs_.iterator();
    }

    public int size() {
        return tableRefs_.size();
    }

    public TableRef get(int i) {
        return tableRefs_.get(i);
    }

    public void set(int i, TableRef tableRef) {
        tableRefs_.set(i, tableRef);
    }

    public void add(TableRef t) {
        tableRefs_.add(t);
    }

    public void addAll(List<TableRef> t) {
        tableRefs_.addAll(t);
    }

    public void clear() {
        tableRefs_.clear();
    }
}
