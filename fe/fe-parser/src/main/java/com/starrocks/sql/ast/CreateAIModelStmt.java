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

import java.util.Map;

public final class CreateAIModelStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final String name;
    private final Map<String, String> properties;
    private final String comment;

    public CreateAIModelStmt(boolean ifNotExists, String name, Map<String, String> properties, String comment, NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.name = name;
        this.properties = Map.copyOf(properties);
        this.comment = comment == null ? "" : comment;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateAIModelStatement(this, context);
    }
}
