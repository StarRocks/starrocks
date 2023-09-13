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

import java.util.List;
import java.util.Map;

public class CreateStorageVolumeStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final String storageVolumeName;
    private final String storageVolumeType;
    private Map<String, String> properties;
    private final List<String> locations;
    private final String comment;

    public CreateStorageVolumeStmt(boolean ifNotExists,
                                   String storageVolumeName,
                                   String storageVolumeType,
                                   Map<String, String> properties,
                                   List<String> locations,
                                   String comment,
                                   NodePosition pos) {
        super(pos);

        this.ifNotExists = ifNotExists;
        this.storageVolumeName = storageVolumeName;
        this.storageVolumeType = storageVolumeType;
        this.locations = locations;
        this.properties = properties;
        this.comment = Strings.nullToEmpty(comment);
    }

    public String getName() {
        return storageVolumeName;
    }

    public List<String> getStorageLocations() {
        return locations;
    }

    public String getStorageVolumeType() {
        return storageVolumeType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateStorageVolumeStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STORAGE VOLUME ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(storageVolumeName);
        sb.append(" TYPE = ").append(storageVolumeType);
        sb.append(" LOCATIONS = (");
        for (int i = 0; i < locations.size(); ++i) {
            if (i == 0) {
                sb.append("'").append(locations.get(i)).append("'");
            } else {
                sb.append(", '").append(locations.get(i)).append("'");
            }
        }
        sb.append(")");
        if (!comment.isEmpty()) {
            sb.append(" COMMENT '").append(comment).append("'");
        }
        sb.append(" PROPERTIES (").
                append(new PrintableMap<>(properties, "=", true, false)).append(")");
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
