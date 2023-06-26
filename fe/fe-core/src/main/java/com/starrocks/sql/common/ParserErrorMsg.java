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

    @BaseMessage("No viable statement for input ''{0}''")
    String noViableStatement(String a0);

    @BaseMessage("Input ''{0}'' is valid only for ''{1}''")
    String failedPredicate(String a0, String a1);

    @BaseMessage("Unexpected input ''{0}'', the most similar input is {1}")
    String unexpectedInput(String a0, String a1);

    @BaseMessage("Statement exceeds maximum length limit, please consider modify ''parse_tokens_limit'' session variable")
    String tokenExceedLimit();

    @BaseMessage("The inserted rows are {0} exceeded the maximum limit {1}, please consider modify " +
            "''expr_children_limit'' in FE conf")
    String insertRowsExceedLimit(long a0, int a1);

    @BaseMessage("The number of exprs are {0} exceeded the maximum limit {1}, please consider modify " +
            "''expr_children_limit'' in FE conf")
    String exprsExceedLimit(long a0, int a1);

    @BaseMessage("The number of children in expr are {0} exceeded the maximum limit {1}, please consider modify " +
            "''expr_children_limit'' in FE conf")
    String argsOfExprExceedLimit(long a0, int a1);

    @BaseMessage("Invalid db name format ''{0}''")
    String invalidDbFormat(String a0);

    @BaseMessage("Invalid table name format ''{0}''")
    String invalidTableFormat(String a0);

    @BaseMessage("Invalid task name format ''{0}''")
    String invalidTaskFormat(String a0);

    @BaseMessage("Invalid pipe name ''{0}''")
    String invalidPipeName(String a0);

    @BaseMessage("Invalid UDF function name ''{0}''")
    String invalidUDFName(String a0);

    @BaseMessage("Unsupported type specification: ''{0}''")
    String unsupportedType(String a0);

    @BaseMessage("Unsupported type specification: ''{0}'' {1}")
    String unsupportedType(String a0, String a1);

    @BaseMessage("Unsupported statement: '{0}'")
    String unsupportedStatement(String a0);

    @BaseMessage("AUTO_INCREMENT column {0} must be NOT NULL")
    String nullColFoundInPK(String a0);

    @BaseMessage("Incorrect number of arguments in expr ''{0}''")
    String wrongNumOfArgs(String a0);

    @BaseMessage("Incorrect number {0} of arguments in expr ''{1}'', {2}")
    String wrongNumOfArgs(int a0, String a1, String a2);

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

    @BaseMessage("Column ''{0}'' can not be AUTO_INCREMENT when {1} COLUMN")
    String autoIncrementForbid(String a0, String a1);

    @BaseMessage("Column ''{0}'' can not be MATERIALIZED COLUMN when {1}")
    String materializedColumnForbid(String a0, String a1);

    @BaseMessage("{0} can not be set when {1}")
    String materializedColumnLimit(String a0, String a1);

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

    @BaseMessage("Invalid map format, which should be key:value")
    String invalidMapFormat();

    @BaseMessage("{0} must be nullable column")
    String foundNotNull(String a0);

    @BaseMessage("{0} has no default values")
    String hasDefaultValue(String a0);

    @BaseMessage("{0} can not be KEY")
    String isKey(String a0);

    // --------- error in analyzing phase ---------
    @BaseMessage("Invalid {0} id format ''{1}''")
    String invalidIdFormat(String a0, String a1);

    @BaseMessage("Invalid {0} value ''{1}''")
    String invalidPropertyValue(String a0, String a1);

    @BaseMessage("Unsupported properties ''{0}''")
    String unsupportedProps(String a0);

    @BaseMessage("Missing necessary properties ''{0}''")
    String missingProps(String a0);

    @BaseMessage("No database selected")
    String noDbSelected();

    @BaseMessage("No catalog selected")
    String noCatalogSelected();

    @BaseMessage("Unsupported operation on {0}")
    String unsupportedOpWithInfo(String a0);

    @BaseMessage("Unsupported operation {0}")
    String unsupportedOp(String a0);

    @BaseMessage("Unsupported predicates. Where clause can only be ''{0}''")
    String invalidWhereExpr(String a0);

    @BaseMessage("''{0}'' must be an aggregate expression or appear in GROUP BY clause")
    String shouldBeAggFunc(String a0);

    @BaseMessage("Exist must have exact one subquery")
    String canOnlyOneExistSub();

    @BaseMessage("Unsupported nest {0} inside aggregation")
    String unsupportedNestAgg(String a0);

    @BaseMessage("The arguments of GROUPING must be expressions referenced by GROUP BY")
    String argsCanOnlyFromGroupBy();

    @BaseMessage("Session's db ''{0}'' not equals label's db ''{1}''")
    String dbNameNotMatch(String a0, String a1);

    @BaseMessage("{0} host or port is wrong. {1}")
    String invalidHostOrPort(String a0, String a1);

    @BaseMessage("Column definition is wrong. {0}")
    String invalidColumnDef(String a0);

    @BaseMessage("Column position is wrong. {0}")
    String invalidColumnPos(String a0);

    @BaseMessage("Field ''{0}'' is not null but doesn't have a default value")
    String withOutDefaultVal(String a0);

    @BaseMessage("Invalid column name format ''{0}''")
    String invalidColFormat(String a0);

    @BaseMessage("Conflicted options {0} and {1}")
    String conflictedOptions(String a0, String a1);
}
