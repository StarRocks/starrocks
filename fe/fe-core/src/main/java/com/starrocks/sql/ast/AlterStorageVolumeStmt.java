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

import java.util.Map;

public class AlterStorageVolumeStmt extends DdlStmt {
    private String storageVolumeName;

    private Map<String, String> storageParams;
    private final Boolean enabled;
    private final String comment;
    private final Boolean isDefault;

    public AlterStorageVolumeStmt(String storageVolumeName, Map<String, String> storageParams, Boolean enabled,
                                  Boolean isDefault, String comment, NodePosition pos) {
        super(pos);
        this.storageVolumeName = storageVolumeName;
        this.storageParams = storageParams;
        this.enabled = enabled;
        this.comment = Strings.nullToEmpty(comment);
        this.isDefault = isDefault;
    }

    public String getName() {
        return storageVolumeName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterStorageVolumeStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER STORAGE VOLUME ").append(storageVolumeName).append(" SET");
        if (isDefault != null && isDefault) {
            sb.append(" AS DEFAULT");
        }
        if (!storageParams.isEmpty()) {
            sb.append(" (").
                    append(new PrintableMap<>(storageParams, "=", true, false))
                    .append(")");
        }
        if (enabled != null) {
            sb.append(" ENABLED = ").append(enabled);
        }
        if (!comment.isEmpty()) {
            sb.append(" COMMENT = '").append(comment).append("'");
        }
        return sb.toString();
    }

}
