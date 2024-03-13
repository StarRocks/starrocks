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
import java.util.Map;
public class CreateDictionaryStmt extends DdlStmt {
    private final String dictionaryName;
    private final String queryableObject;
    private final List<String> dictionaryKeys;
    private final List<String> dictionaryValues;
    private final Map<String, String> properties;

    public CreateDictionaryStmt(String dictionaryName, String queryableObject, List<String> dictionaryKeys,
                                List<String> dictionaryValues, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.dictionaryName = dictionaryName;
        this.queryableObject = queryableObject;
        this.dictionaryKeys = dictionaryKeys;
        this.dictionaryValues = dictionaryValues;
        this.properties = properties;
    }

    public String getDictionaryName() {
        return dictionaryName;
    }

    public String getQueryableObject() {
        return queryableObject;
    }

    public List<String> getDictionaryKeys() {
        return dictionaryKeys;
    }

    public List<String> getDictionaryValues() {
        return dictionaryValues;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDictionaryStatement(this, context);
    }
}
