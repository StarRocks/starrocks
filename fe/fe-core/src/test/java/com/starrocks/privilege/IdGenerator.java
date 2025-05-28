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

package com.starrocks.privilege;

<<<<<<< HEAD:fe/fe-core/src/test/java/com/starrocks/privilege/IdGenerator.java
public class IdGenerator {
    private static final int BATCH_ID_INTERVAL = 1000;
    private long nextId;

    public IdGenerator() {
        nextId = BATCH_ID_INTERVAL;
    }

    // performance is more quickly
    public synchronized long getNextId() {
        return nextId++;
=======
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metadata.iceberg.IcebergMetadataTableFactory;

public class MetadataTableFactoryProvider {
    public static AbstractMetadataTableFactory getFactory(String catalogType) {
        if (catalogType.equalsIgnoreCase(ConnectorType.ICEBERG.getName()) ||
                catalogType.equalsIgnoreCase(ConnectorType.UNIFIED.getName())) {
            return IcebergMetadataTableFactory.INSTANCE;
        }
        throw new StarRocksConnectorException("not support getting %s metadata table factory", catalogType);
>>>>>>> f8628553de ([Enhancement] Support query iceberg metadata table for unified catalog (#59412)):fe/fe-core/src/main/java/com/starrocks/connector/metadata/MetadataTableFactoryProvider.java
    }
}
