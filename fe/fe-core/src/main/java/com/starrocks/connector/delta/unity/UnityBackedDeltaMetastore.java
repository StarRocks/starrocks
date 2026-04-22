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

package com.starrocks.connector.delta.unity;

import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.connector.delta.DeltaLakeMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import org.apache.hadoop.conf.Configuration;

/**
 * {@link DeltaLakeMetastore} variant whose {@code IMetastore} delegate is a
 * {@link UnityMetastore}. The parent class continues to handle Delta Kernel snapshot loading,
 * partition-key extraction, and checkpoint/JSON caching; this class only supplies the
 * {@code MetastoreTable} that points at the UC-reported storage location and, when
 * vended-credentials-enabled is true, embeds per-table cloud credentials.
 */
public class UnityBackedDeltaMetastore extends DeltaLakeMetastore {

    private final boolean vendedCredentialsEnabled;

    public UnityBackedDeltaMetastore(String catalogName,
                                     UnityMetastore delegate,
                                     Configuration hdfsConfiguration,
                                     DeltaLakeCatalogProperties deltaLakeCatalogProperties,
                                     boolean vendedCredentialsEnabled) {
        super(catalogName, delegate, hdfsConfiguration, deltaLakeCatalogProperties);
        this.vendedCredentialsEnabled = vendedCredentialsEnabled;
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        return this.delegate.getMetastoreTable(dbName, tableName);
    }

    @Override
    public boolean isVendedCredentialsEnabled() {
        return vendedCredentialsEnabled;
    }
}
