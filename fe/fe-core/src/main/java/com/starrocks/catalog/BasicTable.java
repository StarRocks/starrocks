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

/**
 * BasicTable declares a subset of methods in {@link Table}, which aims to provide basic information about table while
 * eliminating network interactions with external services.
 */
public interface BasicTable {
    String getCatalogName();

    String getName();

    String getComment();

    String getMysqlType();

    String getEngine();

    Table.TableType getType();

    boolean isView();

    boolean isMaterializedView();

    boolean isNativeTableOrMaterializedView();

    long getCreateTime();

    long getLastCheckTime();
}
