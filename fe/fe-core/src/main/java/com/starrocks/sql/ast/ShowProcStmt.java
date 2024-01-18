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

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

// SHOW PROC statement. Used to show proc information, only admin can use.
public class ShowProcStmt extends ShowStmt {

    public static final ImmutableSet<String> NEED_FORWARD_PATH_ROOT;

    static {
        NEED_FORWARD_PATH_ROOT = new ImmutableSet.Builder<String>()
                .add("cluster_balance")
                .add("routine_loads")
                .add("transactions")
                .build();
    }

    private final String path;
    private ProcNodeInterface node;

    public ShowProcStmt(String path) {
        this(path, NodePosition.ZERO);
    }

    public ShowProcStmt(String path, NodePosition pos) {
        super(pos);
        this.path = path;
    }


    public ProcNodeInterface getNode() {
        return node;
    }

    public void setNode(ProcNodeInterface node) {
        this.node = node;
    }

    public String getPath() {
        return path;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ProcResult result = null;
        try {
            result = node.fetchResult();
        } catch (AnalysisException e) {
            return builder.build();
        }

        for (String col : result.getColumnNames()) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            if (path.equals("/") || !path.contains("/")) {
                return RedirectStatus.NO_FORWARD;
            }
            String[] pathGroup = path.split("/");
            if (NEED_FORWARD_PATH_ROOT.contains(pathGroup[1])) {
                return RedirectStatus.FORWARD_NO_SYNC;
            }
            return RedirectStatus.NO_FORWARD;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowProcStmt(this, context);
    }
}
