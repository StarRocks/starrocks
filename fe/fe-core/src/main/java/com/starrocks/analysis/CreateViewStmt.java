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
import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

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

        createColumnAndViewDefs(analyzer);
    }
}
