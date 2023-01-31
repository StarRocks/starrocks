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

public enum PrivilegeType {
    GRANT,
    NODE,
    OPERATE,

    DELETE,
    DROP,
    INSERT,
    SELECT,
    ALTER,
    EXPORT,
    UPDATE,
    USAGE,
    PLUGIN,
    FILE,
    BLACKLIST,
    REPOSITORY,
    REFRESH,
    IMPERSONATE,

    CREATE_DATABASE,
    CREATE_TABLE,
    CREATE_VIEW,
    CREATE_FUNCTION,
    CREATE_GLOBAL_FUNCTION,
    CREATE_MATERIALIZED_VIEW,
    CREATE_RESOURCE,
    CREATE_RESOURCE_GROUP,
    CREATE_EXTERNAL_CATALOG,
}
