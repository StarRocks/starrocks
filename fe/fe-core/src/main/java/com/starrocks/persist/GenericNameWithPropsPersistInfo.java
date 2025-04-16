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


package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.Map;

public class GenericNameWithPropsPersistInfo implements Writable {
    @SerializedName(value = "n")
    public String name;
    @SerializedName(value = "m")
    public Map<String, String> propertyMap;

    public GenericNameWithPropsPersistInfo(String name, Map<String, String> propertyMap) {
        this.name = name;
        this.propertyMap = propertyMap;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getPropertyMap() {
        return propertyMap;
    }
}