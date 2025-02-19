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

package com.starrocks.connector.iceberg;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;

// Mainly used for table with iceberg equality delete files
public class IcebergTableMORParams {
    // Use to identify the unique id for mor params.
    private final long morId;
    private final List<IcebergMORParams> morParamsList;

    public static IcebergTableMORParams EMPTY = new IcebergTableMORParams(-1, List.of());

    public IcebergTableMORParams(long id, List<IcebergMORParams> morParamsList) {
        this.morId = id;
        this.morParamsList = morParamsList;
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(morParamsList);
    }

    public long getMORId() {
        return morId;
    }

    public int size() {
        return morParamsList.size();
    }

    public List<IcebergMORParams> getMorParamsList() {
        return morParamsList;
    }

}
