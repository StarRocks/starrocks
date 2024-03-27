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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

public class CreateTemporaryTableAsSelectStmt extends CreateTableAsSelectStmt {
    private static final Logger LOG = LogManager.getLogger(CreateTemporaryTableAsSelectStmt.class);

    public CreateTemporaryTableAsSelectStmt(CreateTemporaryTableStmt createTemporaryTableStmt,
                                            List<String> columnNames, QueryStatement queryStatement, NodePosition pos) {
        super(createTemporaryTableStmt, columnNames, queryStatement, pos);
    }

    public void setSessionId(UUID sessionId) {
        CreateTemporaryTableStmt createTemporaryTableStmt = (CreateTemporaryTableStmt) getCreateTableStmt();
        createTemporaryTableStmt.setSessionId(sessionId);
    }

    public UUID getSessionId() {
        CreateTemporaryTableStmt createTemporaryTableStmt = (CreateTemporaryTableStmt) getCreateTableStmt();
        return createTemporaryTableStmt.getSessionId();
    }

    @Override
    public boolean createTable(ConnectContext session) throws AnalysisException {
        try {
            CreateTemporaryTableStmt createTemporaryTableStmt = (CreateTemporaryTableStmt) getCreateTableStmt();
            createTemporaryTableStmt.setSessionId(session.getSessionId());
            return session.getGlobalStateMgr().getMetadataMgr().createTemporaryTable(createTemporaryTableStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    @Override
    public void dropTable(ConnectContext session) throws AnalysisException {
        try {
            DropTemporaryTableStmt dropTemporaryTableStmt =
                    new DropTemporaryTableStmt(true, getCreateTableStmt().getDbTbl(), true);
            dropTemporaryTableStmt.setSessionId(session.getSessionId());
            session.getGlobalStateMgr().getMetadataMgr().dropTemporaryTable(dropTemporaryTableStmt);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
    }
}
