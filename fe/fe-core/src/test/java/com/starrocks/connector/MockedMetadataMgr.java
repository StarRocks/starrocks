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


package com.starrocks.connector;

import com.google.common.base.Strings;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MockedMetadataMgr extends MetadataMgr {
    private final Map<String, ConnectorMetadata> metadatas = new HashMap<>();
    private final LocalMetastore localMetastore;

    public MockedMetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
        super(localMetastore, connectorMgr, new ConnectorTblMetaInfoMgr());
        this.localMetastore = localMetastore;
    }

    public void registerMockedMetadata(String catalogName, ConnectorMetadata metadata) {
        metadatas.put(catalogName, metadata);
    }

    @Override
    public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        if (Strings.isNullOrEmpty(catalogName) || CatalogMgr.isInternalCatalog(catalogName)) {
            return Optional.of(localMetastore);
        } else {
            return Optional.ofNullable(metadatas.get(catalogName));
        }
    }
}
