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

import com.google.cloud.hadoop.repackaged.gcs.com.google.common.base.Strings;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class CreateStorageVolumeStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final String storageVolumeName;
    private final String storageVolumeType;
    private Map<String, String> storageParams;
    private final List<String> locations;
    private final Boolean enabled;
    private final String comment;

    public CreateStorageVolumeStmt(boolean ifNotExists,
                                   String storageVolumeName,
                                   String storageVolumeType,
                                   Map<String, String> storageParams,
                                   List<String> locations,
                                   Boolean enabled,
                                   String comment,
                                   NodePosition pos) {
        super(pos);

        this.ifNotExists = ifNotExists;
        this.storageVolumeName = storageVolumeName;
        this.storageVolumeType = storageVolumeType;
        this.locations = locations;
        this.storageParams = storageParams;
        this.enabled = enabled;
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
        sb.append("'").append(storageVolumeName).append("'");
        sb.append(" TYPE = ").append(storageVolumeType);
        sb.append(" (").
                append(new PrintableMap<>(storageParams, "=", true, false)).append(")");
        sb.append(" STORAGE_LOCATIONS = (");
        for (int i = 0; i < locations.size(); ++i) {
            if (i == 0) {
                sb.append("'").append(locations.get(i)).append("'");
            } else {
                sb.append(", '").append(locations.get(i)).append("'");
            }
        }
        sb.append(")");

        sb.append(" ENABLED = ").append(enabled);
        if (!comment.isEmpty()) {
            sb.append(" COMMENT = ").append(comment);
        }
        return sb.toString();
    }
}
