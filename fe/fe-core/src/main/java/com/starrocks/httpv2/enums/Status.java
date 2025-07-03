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


package com.starrocks.httpv2.enums;

public enum Status {

    SUCCESS(0, "success"),
    USER_NAME_NULL(10000, "user name is null"),
    LOGIN_SUCCESS(10001, "login success"),
    USER_NAME_PASSWD_ERROR(10002, "user name or password error"),
    USER_LOGIN_FAILURE(10003, "user login failure"),
    ;

    private final int code;
    private final String enMsg;

    Status(int code, String enMsg) {
        this.code = code;
        this.enMsg = enMsg;
    }

    public int getCode() {
        return this.code;
    }

    public String getMsg() {
        return this.enMsg;
    }

}
