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

package com.starrocks.sql.common;

public enum ErrorType {
    USER_ERROR(0),
    INTERNAL_ERROR(1),
    UNSUPPORTED(2),
    META_NOT_FOUND(3),
    RULE_EXHAUSTED(4);

    private final int code;

    ErrorType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}