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

package com.starrocks.connector.metadata;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorMetadata;

import static java.util.Objects.requireNonNull;

public class TableMetaConnector implements Connector {
    private final TableMetaMetadata metaMetadata;

    public TableMetaConnector(String catalogName, String catalogType) {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(catalogType, "catalogType is null");
        this.metaMetadata = new TableMetaMetadata(catalogName, catalogType);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return this.metaMetadata;
    }
}