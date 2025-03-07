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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;
import java.util.function.Consumer;

public class FileTableFunctionRelation extends TableRelation {

    public static final String IDENTIFIER = "FILES";

    private Map<String, String> properties;

    // function of push down target table schema to files, for insert from files()
    private Consumer<TableFunctionTable> pushDownSchemaFunc;

    public FileTableFunctionRelation(Map<String, String> properties, NodePosition pos) {
        super(new TableName(null, "table_function_table"));
        this.properties = properties;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Consumer<TableFunctionTable> getPushDownSchemaFunc() {
        return pushDownSchemaFunc;
    }

    public void setPushDownSchemaFunc(Consumer<TableFunctionTable> pushDownSchemaFunc) {
        this.pushDownSchemaFunc = pushDownSchemaFunc;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFileTableFunction(this, context);
    }
}