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

package com.starrocks.catalog;

import java.util.List;

public class LanceTable extends Table {

    private final String uri;

    private final String catalogName;

    public LanceTable(long id, String name, List<Column> schema, String uri) {
        this(id, null, name, schema, uri);
    }

    public LanceTable(long id, String catalogName, String name, List<Column> schema, String uri) {
        super(id, name, TableType.LANCE, schema);
        this.uri = uri;
        this.catalogName = catalogName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public String getUri() {
        return uri;
    }

    @Override
    public String getTableLocation() {
        return uri;
    }

    @Override
    public boolean isSupported() {
        return false;
    }
}
