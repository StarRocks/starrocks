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

package com.starrocks.sql.ast.aiprovider;

import com.google.common.base.Strings;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class CreateAIProviderStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final String name;
    private final String type;
    private final Map<String, String> properties;
    private final String comment;

    public CreateAIProviderStmt(boolean ifNotExists,
                                String name,
                                String type,
                                Map<String, String> properties,
                                String comment,
                                NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.name = name;
        this.type = type;
        this.properties = properties == null ? Collections.emptyMap() : new LinkedHashMap<>(properties);
        this.comment = Strings.nullToEmpty(comment);
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateAIProviderStatement(this, context);
    }
}
