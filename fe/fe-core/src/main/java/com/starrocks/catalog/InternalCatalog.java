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

import com.starrocks.analysis.TableName;

import java.util.Map;

public class InternalCatalog extends Catalog {
    public static final String DEFAULT_INTERNAL_CATALOG_NAME = "default_catalog";
    public static final long DEFAULT_INTERNAL_CATALOG_ID = -11;

    public InternalCatalog(long id, String name, Map<String, String> config, String comment) {
        super(id, DEFAULT_INTERNAL_CATALOG_NAME, config, comment);
    }

    public static boolean isFromDefault(TableName name) {
        return name != null && DEFAULT_INTERNAL_CATALOG_NAME.equalsIgnoreCase(name.getCatalog());
    }

}
