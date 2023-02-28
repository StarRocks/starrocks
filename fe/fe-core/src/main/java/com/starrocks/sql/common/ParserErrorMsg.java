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

package com.starrocks.sql.common;

import static com.starrocks.sql.common.ErrorMsgProxy.BaseMessage;
public interface ParserErrorMsg {

    // --------- error in building AST phase ---------
    @BaseMessage("at line {0}, column {1}")
    String nodePositionPoint(int a0, int a1);

    @BaseMessage("from line {0}, column {1} to line {2}, column {3}")
    String nodePositionRange(int a0, int a1, int a2, int a3);

    @BaseMessage("No viable statement for input ''{0}'', please check the SQL Reference")
    String noViableStatement(String a0);

    @BaseMessage("Input ''{0}'' is not valid at this position, please check the SQL Reference")
    String inputMismatch(String a0);

    @BaseMessage("Input ''{0}'' is valid only for ''{1}'', please check the SQL Reference")
    String failedPredicate(String a0, String a1);

    @BaseMessage("Statement exceeds maximum length limit, please consider modify ''parse_tokens_limit'' session variable")
    String tokenExceedLimit();

    @BaseMessage("The inserted rows are {0} exceeded the maximum limit {1}, please consider modify " +
            "''expr_children_limit'' in BE conf")
    String insertRowsExceedLimit(long a0, int a1);

    @BaseMessage("The number of exprs are {0} exceeded the maximum limit {1}, please consider modify " +
            "''expr_children_limit'' in BE conf")
    String exprsExceedLimit(long a0, int a1);

    @BaseMessage("The number of children in expr are {0} exceeded the maximum limit {1}, please consider modify " +
            "''expr_children_limit'' in BE conf")
    String argsOfExprExceedLimit(long a0, int a1);

    @BaseMessage("Invalid db name format ''{0}''")
    String invalidDbFormat(String a0);

    @BaseMessage("Invalid table name format ''{0}''")
    String invalidTableFormat(String a0);

    @BaseMessage("Invalid task name format ''{0}''")
    String invalidTaskFormat(String a0);

    @BaseMessage("Invalid UDF function name ''{0}''")
    String invalidUDFName(String a0);

    @BaseMessage("Unsupported type specification: ''{0}''")
    String unsupportedType(String a0);

    @BaseMessage("AUTO_INCREMENT column {0} must be NOT NULL")
    String nullColFoundInPK(String a0);

    @BaseMessage("Incorrect number of arguments in expr ''{0}''")
    String wrongNumOfArgs(String a0);

    @BaseMessage("Incorrect type/value of arguments in expr ''{0}''")
    String wrongTypeOfArgs(String a0);

    @BaseMessage("Unsupported expr ''{0}''")
    String unsupportedExpr(String a0);

    @BaseMessage("Unsupported expr ''{0}'' in {1} clause")
    String unsupportedExprWithInfo(String a0, String a1);

    @BaseMessage("Unsupported expr ''{0}'' in {1} clause. {2}")
    String unsupportedExprWithInfoAndExplain(String a0, String a1, String a2);

    @BaseMessage("Cannot use duplicated {0} clause in building materialized view")
    String duplicatedClause(String a0);

    @BaseMessage("''{0}'' cannot support ''{1}'' in materialized view")
    String forbidClauseInMV(String a0, String a1);

    @BaseMessage("FE config ''{0}'' is disabled")
    String feConfigDisable(String a0);

    @BaseMessage("Sql to be add black list is empty")
    String emptySql();

    @BaseMessage("Column ''{0}'' can not be AUTO_INCREMENT when {1} COLUMN.")
    String autoIncrementForbid(String a0, String a1);

    @BaseMessage("No tables used")
    String noTableUsed();

    @BaseMessage("Invalid odbc scalar function ''{0}''")
    String invalidOdbcFunc(String a0);

    @BaseMessage("Invalid numeric literal {0}")
    String invalidNumFormat(String a0);

    @BaseMessage("Invalid date literal {0}")
    String invalidDateFormat(String a0);

    @BaseMessage("Numeric overflow {0}")
    String numOverflow(String a0);

    @BaseMessage("Binary literal can only contain hexadecimal digits and an even number of digits")
    String invalidBinaryFormat();

    @BaseMessage("Refresh start time must be after current time")
    String invalidStartTime();


    // --------- error in analyzing phase ---------

}
