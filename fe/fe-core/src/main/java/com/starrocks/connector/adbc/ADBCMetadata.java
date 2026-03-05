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

package com.starrocks.connector.adbc;

import com.starrocks.connector.ConnectorMetadata;

import java.util.Map;

// TODO: Plan 04 will replace this stub with a full ADBC metadata implementation
// that uses the ADBC Java API to retrieve schemas, tables, and columns.
public class ADBCMetadata implements ConnectorMetadata {

    private final Map<String, String> properties;
    private final String catalogName;

    public ADBCMetadata(Map<String, String> properties, String catalogName) {
        this.properties = properties;
        this.catalogName = catalogName;
    }

    public void shutdown() {
        // TODO: Plan 04 will close ADBC connections here
    }
}
