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

    @BaseMessage("Unsupported partition expression: ''{0}''")
    String invalidPartitionExpr(String a0);

    @BaseMessage("Invalid db name format ''{0}''")
    String invalidDbFormat(String a0);

    @BaseMessage("Invalid task name format ''{0}''")
    String invalidTaskFormat(String a0);


}
