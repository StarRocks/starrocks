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

// A single SQL diagnostic produced during statement execution and surfaced to clients through
// SHOW WARNINGS / SHOW ERRORS. The level follows the MySQL diagnostics area: "Note", "Warning",
// or "Error". The three fields map directly to the Level / Code / Message columns of
// SHOW WARNINGS. The OK packet's warning_count is set separately from these entries and is left
// unchanged by this class.
public class QueryWarning {
    private static final String LEVEL_WARNING = "Warning";
    private static final String LEVEL_ERROR = "Error";

    // MysqlErrPacket sends 1064 when QueryState carries no ErrorCode, and substitutes
    // "Unknown error" for an empty message. Both are mirrored below so that SHOW ERRORS reports
    // exactly what the client received in the ERR packet.
    private static final int DEFAULT_ERROR_CODE = 1064;
    private static final String UNKNOWN_ERROR_MESSAGE = "Unknown error";

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
        return new QueryWarning(LEVEL_WARNING, "1265",
                filteredRows + " row(s) filtered or substituted to NULL during load; "
                        + "tracking_url=" + trackingUrl);
    }

    // The diagnostic for a statement that failed, shared by the path that fails inside
    // StmtExecutor.execute() and the path that is rejected before it (ConnectProcessor). Both
    // read the same QueryState the ERR packet is serialized from, so SHOW ERRORS stays a 1:1
    // mirror of the error the client received. A statement forwarded to the leader is the one
    // case where the packet is not built from this state, and StmtExecutor.execute() records
    // nothing for it rather than reporting a code the client never saw.
    public static QueryWarning fromErrorState(QueryState state) {
        String message = state.getErrorMessage();
        if (message == null || message.isEmpty()) {
            message = UNKNOWN_ERROR_MESSAGE;
        }
        int errorCode = state.getErrorCode() != null ? state.getErrorCode().getCode() : DEFAULT_ERROR_CODE;
        return new QueryWarning(LEVEL_ERROR, String.valueOf(errorCode), message);
    }

    public boolean isError() {
        return LEVEL_ERROR.equalsIgnoreCase(level);
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
