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
import com.google.common.collect.ImmutableSet;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

import static com.starrocks.common.util.Util.normalizeName;

public class CreateFileStmt extends DdlStmt {
    public static final String PROP_CATALOG_DEFAULT = "DEFAULT";
    public static final String PROP_CATALOG = "catalog";
    public static final String PROP_URL = "url";
    public static final String PROP_MD5 = "md5";
    public static final String PROP_SAVE_CONTENT = "save_content";

    public static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(PROP_CATALOG).add(PROP_URL).add(PROP_MD5).build();

    private final String fileName;
    private String dbName;
    private final Map<String, String> properties;

    // if saveContent is true, the file content will be saved in FE memory, so the file size will be limited
    // by the configuration. If it is false, only URL will be saved.
    private boolean saveContent = true;

    public CreateFileStmt(String fileName, String dbName, Map<String, String> properties) {
        this(fileName, dbName, properties, NodePosition.ZERO);
    }

    public CreateFileStmt(String fileName, String dbName, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.fileName = fileName;
        this.dbName = normalizeName(dbName);
        this.properties = properties;
    }

    public String getFileName() {
        return fileName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = normalizeName(dbName);
    }

    public String getCatalogName() {
        String catalog = properties.get(PROP_CATALOG);
        return Strings.isNullOrEmpty(catalog) ? PROP_CATALOG_DEFAULT : catalog;
    }

    public String getDownloadUrl() {
        return properties.get(PROP_URL);
    }

    public String getChecksum() {
        return properties.get(PROP_MD5);
    }

    public boolean isSaveContent() {
        return saveContent;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCreateFileStatement(this, context);
    }
}
