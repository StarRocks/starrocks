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

import com.starrocks.sql.parser.NodePosition;

/**
 * AST for: ADMIN SKIP COMMITTED TRANSACTION &lt;txn_id&gt; [REASON '&lt;text&gt;']
 *
 * <p>Operator-only escape hatch to unblock a publish-stuck COMMITTED transaction
 * by discarding its data contribution. The partition visible version still
 * advances (via a no-op metadata write on BE), but the txn becomes
 * VISIBLE-with-no-data instead of failing.
 *
 * <p>Phase-1 limitations: only supports load and lake-compaction txns on lake
 * tables with file_bundling=true. Alter/schema-change txns are not yet
 * supported. Gated by FE config {@code enable_admin_skip_committed_txn}.
 */
public class AdminSkipCommittedTransactionStmt extends DdlStmt {

    private final long txnId;
    private final String reason;

    public AdminSkipCommittedTransactionStmt(long txnId, String reason, NodePosition pos) {
        super(pos);
        this.txnId = txnId;
        this.reason = reason == null ? "" : reason;
    }

    public long getTxnId() {
        return txnId;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSkipCommittedTransactionStatement(this, context);
    }
}
