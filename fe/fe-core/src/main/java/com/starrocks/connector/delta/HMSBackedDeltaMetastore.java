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

package com.starrocks.connector.delta;

import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import org.apache.hadoop.conf.Configuration;

public class HMSBackedDeltaMetastore extends DeltaLakeMetastore {
    public HMSBackedDeltaMetastore(String catalogName, IHiveMetastore metastore, Configuration hdfsConfiguration) {
        super(catalogName, metastore, hdfsConfiguration);
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        return this.delegate.getMetastoreTable(dbName, tableName);
    }
}
