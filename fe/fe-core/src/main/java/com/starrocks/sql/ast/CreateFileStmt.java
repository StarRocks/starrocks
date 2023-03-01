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
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;
import java.util.Optional;

public class CreateFileStmt extends DdlStmt {
    public static final String PROP_CATALOG_DEFAULT = "DEFAULT";
    private static final String PROP_CATALOG = "catalog";
    private static final String PROP_URL = "url";
    private static final String PROP_MD5 = "md5";
    private static final String PROP_SAVE_CONTENT = "save_content";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(PROP_CATALOG).add(PROP_URL).add(PROP_MD5).build();

    private final String fileName;
    private String dbName;
    private final Map<String, String> properties;

    // needed item set after analyzed
    private String catalogName;
    private String downloadUrl;
    private String checksum;
    // if saveContent is true, the file content will be saved in FE memory, so the file size will be limited
    // by the configuration. If it is false, only URL will be saved.
    private boolean saveContent = true;

    public CreateFileStmt(String fileName, String dbName, Map<String, String> properties) {
        this(fileName, dbName, properties, NodePosition.ZERO);
    }

    public CreateFileStmt(String fileName, String dbName, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.fileName = fileName;
        this.dbName = dbName;
        this.properties = properties;
    }

    public String getFileName() {
        return fileName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDownloadUrl() {
        return downloadUrl;
    }

    public String getChecksum() {
        return checksum;
    }

    public boolean isSaveContent() {
        return saveContent;
    }

    public void analyzeProperties() throws SemanticException {
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new SemanticException(optional.get() + " is invalid property");
        }

        catalogName = properties.get(PROP_CATALOG);
        if (Strings.isNullOrEmpty(catalogName)) {
            catalogName = PROP_CATALOG_DEFAULT;
        }

        downloadUrl = properties.get(PROP_URL);
        if (Strings.isNullOrEmpty(downloadUrl)) {
            throw new SemanticException("download url is missing");
        }

        if (properties.containsKey(PROP_MD5)) {
            checksum = properties.get(PROP_MD5);
        }

        if (properties.containsKey(PROP_SAVE_CONTENT)) {
            throw new SemanticException("'save_content' property is not supported yet");
            /*
            String val = properties.get(PROP_SAVE_CONTENT);
            if (val.equalsIgnoreCase("true")) {
                saveContent = true;
            } else if (val.equalsIgnoreCase("false")) {
                saveContent = false;
            } else {
                throw new AnalysisException("Invalid 'save_content' property: " + val);
            }
            */
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateFileStatement(this, context);
    }
}
