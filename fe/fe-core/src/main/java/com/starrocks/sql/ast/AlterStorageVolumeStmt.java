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
import com.starrocks.common.util.PrintableMap;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class AlterStorageVolumeStmt extends DdlStmt {
    private String storageVolumeName;

    private Map<String, String> properties;
    private final String comment;

    public AlterStorageVolumeStmt(String storageVolumeName, Map<String, String> properties,
                                  String comment, NodePosition pos) {
        super(pos);
        this.storageVolumeName = storageVolumeName;
        this.properties = properties;
        this.comment = Strings.nullToEmpty(comment);
    }

    public String getName() {
        return storageVolumeName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterStorageVolumeStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER STORAGE VOLUME ").append(storageVolumeName);
        if (!comment.isEmpty()) {
            sb.append(" COMMENT = '").append(comment).append("'");
        }
        if (!properties.isEmpty()) {
            sb.append(" SET (").
                    append(new PrintableMap<>(properties, "=", true, false))
                    .append(")");
        }
        return sb.toString();
    }

    @Override
    public boolean needAuditEncryption() {
        if (properties.containsKey(CloudConfigurationConstants.AWS_S3_ACCESS_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AWS_S3_SECRET_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY) ||
                properties.containsKey(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN)) {
            return true;
        }
        return false;
    }
}
