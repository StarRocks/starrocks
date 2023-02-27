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


package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.load.streamload.StreamLoadFunctionalExprProvider;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/*
  Show stream load progress by stream load name

  syntax:
      SHOW [ALL] STREAM LOAD [database.][name]

      without ALL: only show task which is not final
      with ALL: show all of task include history task

      without name: show all of stream load task in database with different name
      with name: show all of task named ${name} in database

      without on db: show all of task in connection db
         if user does not choose db before, return error
      with on db: show all of task in ${db}

      example:
        show stream load named test in database1
        SHOW stream load database1.test;

        show stream load in database1
        SHOW stream load database1;

        show stream load in database1 include history
        use database1;
        SHOW ALL stream load;

        show stream load in all of database
        please use show proc
 */
public class ShowStreamLoadStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Label")
                    .add("Id")
                    .add("LoadId")
                    .add("TxnId")
                    .add("DbName")
                    .add("TableName")
                    .add("State")
                    .add("ErrorMsg")
                    .add("TrackingURL")
                    .add("ChannelNum")
                    .add("PreparedChannelNum")
                    .add("NumRowsNormal")
                    .add("NumRowsAbNormal")
                    .add("NumRowsUnselected")
                    .add("NumLoadBytes")
                    .add("TimeoutSecond")
                    .add("CreateTimeMs")
                    .add("BeforeLoadTimeMs")
                    .add("StartLoadingTimeMs")
                    .add("StartPreparingTimeMs")
                    .add("FinishPreparingTimeMs")
                    .add("EndTimeMs")
                    .add("ChannelState")
                    .build();

    private final LabelName labelName;
    private boolean includeHistory = false;
    private StreamLoadFunctionalExprProvider functionalExprProvider;
    private Expr whereClause;
    private List<OrderByElement> orderElements;
    private LimitElement limitElement;

    public ShowStreamLoadStmt(LabelName labelName, boolean includeHistory) {
        this(labelName, includeHistory, null, null, null, NodePosition.ZERO);
    }

    public ShowStreamLoadStmt(LabelName labelName, boolean includeHistory, Expr expr,
                              List<OrderByElement> orderElements, LimitElement limitElement) {
        this(labelName, includeHistory, expr, orderElements, limitElement, NodePosition.ZERO);
    }

    public ShowStreamLoadStmt(LabelName labelName, boolean includeHistory, Expr expr,
                              List<OrderByElement> orderElements, LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.labelName = labelName;
        this.includeHistory = includeHistory;
        this.whereClause = expr;
        this.orderElements = orderElements;
        this.limitElement = limitElement;
    }



    public String getDbFullName() {
        return labelName.getDbName();
    }

    public String getName() {
        return labelName.getLabelName();
    }

    public boolean isIncludeHistory() {
        return includeHistory;
    }

    public StreamLoadFunctionalExprProvider getFunctionalExprProvider(ConnectContext context) throws AnalysisException {
        if (null == functionalExprProvider) {
            functionalExprProvider = new StreamLoadFunctionalExprProvider();
        }
        functionalExprProvider.analyze(context, whereClause, orderElements, limitElement);
        return functionalExprProvider;
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    public void setDb(String db) {
        this.labelName.setDbName(db);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowStreamLoadStatement(this, context);
    }
}
