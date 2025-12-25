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
 * Defines the operations that can be performed on tables.
 * Used by Table classes to declare which operations they support.
 */
public enum TableOperation {
    CREATE,
    READ,
    INSERT,
    UPDATE,
    DELETE,
    DROP,
    ALTER,
    CREATE_TABLE_LIKE,
    RENAME,
    TRUNCATE,
    REFRESH,

    // Add other operations as needed
}
