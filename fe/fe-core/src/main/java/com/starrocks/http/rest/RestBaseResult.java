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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/RestBaseResult.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.http.rest;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// Base restful result
public class RestBaseResult {

    private static final Gson GSON = new Gson();

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.METHOD})
    public @interface Legacy {
    }

    private static final RestBaseResult OK = new RestBaseResult();

    // For compatibility, status still exists in /api/v1, removed in /api/v2 and later version.
    @Legacy
    @SerializedName("status")
    public ActionStatus status;

    @SerializedName("code")
    public String code;

    // For compatibility, msg still exists in /api/v1, removed in /api/v2 and later version.
    @Legacy
    @SerializedName("msg")
    public String msg;

    @SerializedName("message")
    public String message;

    public RestBaseResult() {
        status = ActionStatus.OK;
        code = Integer.toString(ActionStatus.OK.ordinal());
        msg = "Success";
        message = "OK";
    }

    public RestBaseResult(String msg) {
        status = ActionStatus.FAILED;
        code = Integer.toString(ActionStatus.FAILED.ordinal());
        this.msg = msg;
        this.message = msg;
    }

    public RestBaseResult(String code, ActionStatus status, String msg) {
        this.code = code;
        this.status = status;
        this.msg = msg;
        this.message = msg;
    }

    public static RestBaseResult getOk() {
        return OK;
    }

    @Legacy
    public String toJson() {
        return GSON.toJson(this);
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public ActionStatus getStatus() {
        return status;
    }
}
