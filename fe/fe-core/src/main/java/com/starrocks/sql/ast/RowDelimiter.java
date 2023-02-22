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

import com.starrocks.analysis.Delimiter;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

public class RowDelimiter implements ParseNode {
    private final String oriDelimiter;
    private final String delimiter;

    private final NodePosition pos;

    public RowDelimiter(String delimiter) {
        this(delimiter, NodePosition.ZERO);
    }

    public RowDelimiter(String delimiter, NodePosition pos) {
        this.pos = pos;
        this.oriDelimiter = delimiter;
        this.delimiter = Delimiter.convertDelimiter(oriDelimiter);
    }

    public String getRowDelimiter() {
        return delimiter;
    }

    public String toSql() {
        return "'" + oriDelimiter + "'";
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
