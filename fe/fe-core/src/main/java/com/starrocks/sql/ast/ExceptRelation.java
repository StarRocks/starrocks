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

import java.util.List;

public class ExceptRelation extends SetOperationRelation {

    public ExceptRelation(List<QueryRelation> relations, SetQualifier qualifier) {
        super(relations, qualifier);
    }

    public ExceptRelation(List<QueryRelation> relations, SetQualifier qualifier, NodePosition pos) {
        super(relations, qualifier, pos);
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExcept(this, context);
    }
}

