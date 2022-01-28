// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateViewStmt.java

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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CreateViewStmt extends BaseViewStmt {
    private static final Logger LOG = LogManager.getLogger(CreateViewStmt.class);

    private final boolean ifNotExists;
    private final String comment;

    public CreateViewStmt(boolean ifNotExists, TableName tableName, List<ColWithComment> cols,
                          String comment, QueryStmt queryStmt) {
        super(tableName, cols, queryStmt);
        this.ifNotExists = ifNotExists;
        this.comment = Strings.nullToEmpty(comment);
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        tableName.analyze(analyzer);
        viewDefStmt.setNeedToSql(true);

        // check privilege
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }

        if (cols != null) {
            cloneStmt = viewDefStmt.clone();
        }

        // Analyze view define statement
        Analyzer viewAnalyzer = new Analyzer(analyzer);
        viewDefStmt.analyze(viewAnalyzer);

        createView(analyzer);
    }

    void createView(Analyzer analyzer) throws UserException {
        if (cols != null) {
            if (cols.size() != viewDefStmt.getColLabels().size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            for (int i = 0; i < cols.size(); ++i) {
                Type type = viewDefStmt.getBaseTblResultExprs().get(i).getType().clone();
                Column col = new Column(cols.get(i).getColName(), type);
                col.setComment(cols.get(i).getComment());
                finalCols.add(col);
            }
        } else {
            for (int i = 0; i < viewDefStmt.getBaseTblResultExprs().size(); ++i) {
                Type type = viewDefStmt.getBaseTblResultExprs().get(i).getType().clone();
                finalCols.add(new Column(viewDefStmt.getColLabels().get(i), type));
            }
        }
        // Set for duplicate columns
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }

        // format view def string
        originalViewDef = viewDefStmt.toSql();

        if (cols == null) {
            inlineViewDef = originalViewDef;
            return;
        }

        // It's different with createColumnAndViewDefs in here, cloneStmt is origin stmt which analyze without set `setNeedSql`
        Analyzer tmpAnalyzer = new Analyzer(analyzer);
        List<String> colNames = cols.stream().map(ColWithComment::getColName).collect(Collectors.toList());
        viewDefStmt.substituteSelectListForCreateView(tmpAnalyzer, colNames);
        inlineViewDef = viewDefStmt.toSql();
    }
}
