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


package com.starrocks.sql.ast;

import com.starrocks.thrift.TVarType;

// Set statement type.
public enum SetType {
    GLOBAL,
    SESSION,
    USER,
    VERBOSE;

    public TVarType toThrift() {
        if (this == SetType.GLOBAL) {
            return TVarType.GLOBAL;
        }
        return TVarType.SESSION;
    }

    public static SetType fromThrift(TVarType tType) {
        if (tType == TVarType.GLOBAL) {
            return SetType.GLOBAL;
        }

        if (tType == TVarType.VERBOSE) {
            return SetType.VERBOSE;
        }
        return SetType.SESSION;
    }
}
