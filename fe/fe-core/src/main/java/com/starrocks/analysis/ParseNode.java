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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ParseNode.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.UserException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public interface ParseNode {
    /**
     * Perform semantic analysis of node and all of its children.
     * Throws exception if any errors found.
     *
     * @param analyzer
     * @throws AnalysisException, InternalException
     */
    default void analyze(Analyzer analyzer) throws UserException {
        throw new RuntimeException("New AST not support analyze function");
    }

    /**
     * @return SQL syntax corresponding to this node.
     */
    default String toSql() {
        throw new RuntimeException("New AST not implement toSql function");
    }

    NodePosition getPos();

    default <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        throw new RuntimeException("Not implement accept function");
    }
}
