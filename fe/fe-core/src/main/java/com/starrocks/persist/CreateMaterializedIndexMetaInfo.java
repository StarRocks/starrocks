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

package com.starrocks.persist;

import com.google.common.base.Objects;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateMaterializedIndexMetaInfo implements Writable {
    public static final Logger LOG = LoggerFactory.getLogger(CreateMaterializedIndexMetaInfo.class);

    private String dbName;
    private String tableName;
    private String indexName;
    private MaterializedIndexMeta indexMeta;

    public CreateMaterializedIndexMetaInfo() {
        // for persist
    }

    public CreateMaterializedIndexMetaInfo(String dbName, String tableName, String indexName,
                                           MaterializedIndexMeta indexMeta) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.indexName = indexName;
        this.indexMeta = indexMeta;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    MaterializedIndexMeta getIndexMeta() {
        return indexMeta;
    }

    public void write(DataOutput out) throws IOException {
        // compatible with old version
        Text.writeString(out, ClusterNamespace.getFullName(dbName));
        Text.writeString(out, ClusterNamespace.getFullName(tableName));
        indexMeta.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        dbName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        tableName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        indexMeta = MaterializedIndexMeta.read(in);
    }

    public static CreateMaterializedIndexMetaInfo read(DataInput in) throws IOException {
        CreateMaterializedIndexMetaInfo createTableInfo = new CreateMaterializedIndexMetaInfo();
        createTableInfo.readFields(in);
        return createTableInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dbName, tableName, indexMeta);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CreateMaterializedIndexMetaInfo)) {
            return false;
        }

        CreateMaterializedIndexMetaInfo info = (CreateMaterializedIndexMetaInfo) obj;

        return dbName.equals(info.dbName)
                && tableName.equals(info.tableName) && indexMeta.equals(info.indexMeta);
    }
}
