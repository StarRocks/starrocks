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

import com.google.common.base.Strings;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.parser.NodePosition;

public class ColWithComment implements ParseNode {
    private final String colName;
    private final String comment;
    private final NodePosition pos;

    public ColWithComment(String colName, String comment, NodePosition pos) {
        this.pos = pos;
        this.colName = colName;
        this.comment = Strings.nullToEmpty(comment);
    }

    public String getColName() {
        return colName;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public void analyze() {
        FeNameFormat.checkColumnName(colName);
    }
}
