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

package com.starrocks.qe;

// A single SQL diagnostic produced during statement execution and surfaced to clients
// through SHOW WARNINGS / SHOW ERRORS and the MySQL warning_count field. The level follows
// the MySQL diagnostics area: "Note", "Warning", or "Error". The three fields map directly
// to the Level / Code / Message columns of SHOW WARNINGS.
public class QueryWarning {
    private final String level;
    private final String code;
    private final String message;

    public QueryWarning(String level, String code, String message) {
        this.level = level;
        this.code = code;
        this.message = message;
    }

    // The diagnostic for rows silently filtered or NULL-substituted during a load, shared by the
    // autocommit (StmtExecutor) and explicit-transaction (TransactionStmtExecutor) INSERT paths.
    // MySQL code 1265 (WARN_DATA_TRUNCATED) is the closest standard diagnostic. Unlike the OK
    // packet's int field, the message keeps the exact long count.
    public static QueryWarning filteredRowsWarning(long filteredRows, String trackingUrl) {
        return new QueryWarning("Warning", "1265",
                filteredRows + " row(s) filtered or substituted to NULL during load; "
                        + "tracking_url=" + trackingUrl);
    }

    public String getLevel() {
        return level;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
