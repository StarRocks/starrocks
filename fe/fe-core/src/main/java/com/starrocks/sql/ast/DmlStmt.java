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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public abstract class DmlStmt extends StatementBase {
    private long txnId;
    private Map<String, String> optHints;

    protected DmlStmt(NodePosition pos) {
        super(pos);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    public abstract TableName getTableName();

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public Map<String, String> getOptHints() {
        return optHints;
    }

    public void setOptHints(Map<String, String> optHints) {
        this.optHints = optHints;
    }
}
