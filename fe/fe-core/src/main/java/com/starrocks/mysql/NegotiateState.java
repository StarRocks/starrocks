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

package com.starrocks.mysql;

public enum NegotiateState {
    OK("ok"),
    READ_FIRST_AUTH_PKG_FAILED("read first auth package failed"),
    READ_AUTH_SWITCH_PKG_FAILED("read auth switch package failed"),
    ENABLE_SSL_FAILED("enable ssl failed"),
    READ_SSL_AUTH_PKG_FAILED("read ssl auth package failed"),
    NOT_SUPPORTED_AUTH_MODE("not supported auth mode"),
    KERBEROS_HANDSHAKE_FAILED("kerberos handshake failed"),
    KERBEROS_PLUGIN_NOT_LOADED("kerberos plugin not loaded"),
    AUTHENTICATION_FAILED("authentication failed"),
    SET_DATABASE_FAILED("set database failed");

    private final String msg;
    NegotiateState(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }
}
