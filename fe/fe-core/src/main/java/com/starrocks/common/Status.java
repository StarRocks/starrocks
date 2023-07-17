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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Status.java

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

package com.starrocks.common;

import com.starrocks.proto.StatusPB;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;

import java.util.Optional;

public class Status {
    public static final Status OK = new Status();
    public static final Status CANCELLED = new Status(TStatusCode.CANCELLED, "Cancelled");

    public TStatusCode getErrorCode() {
        return errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    private TStatusCode errorCode; // anything other than OK
    private String errorMsg;

    public static Status createInternalError(String errorMsg) {
        return new Status(TStatusCode.INTERNAL_ERROR, errorMsg);
    }

    public static Status createThriftRPCError(String errorMsg) {
        return new Status(TStatusCode.THRIFT_RPC_ERROR, errorMsg);
    }

    public static Status createOK() {
        return new Status(TStatusCode.OK, null);
    }


    public Status() {
        this.errorCode = TStatusCode.OK;
        this.errorMsg = null;
    }

    public Status(Status status) {
        this(status.getErrorCode(), status.getErrorMsg());
    }

    public Status(TStatusCode code, final String errorMsg) {
        this.errorCode = code;
        this.errorMsg = errorMsg;
    }

    public Status(final TStatus status) {
        this.errorCode = status.status_code;
        if (status.isSetError_msgs()) {
            this.errorMsg = status.error_msgs.get(0);
        }
    }

    public boolean ok() {
        return this.errorCode == TStatusCode.OK;
    }

    public boolean isCancelled() {
        return this.errorCode == TStatusCode.CANCELLED;
    }

    public boolean isTimeout() {
        return this.errorCode == TStatusCode.TIMEOUT;
    }

    public boolean isRpcError() {
        return this.errorCode == TStatusCode.THRIFT_RPC_ERROR;
    }

    public boolean isRemoteFileNotFound() {
        return this.errorCode == TStatusCode.REMOTE_FILE_NOT_FOUND;
    }

    public boolean isGlobalDictError() {
        return this.errorCode == TStatusCode.GLOBAL_DICT_ERROR;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public void setStatus(Status status) {
        this.errorCode = status.errorCode;
        this.errorMsg = status.getErrorMsg();
    }

    public void setInternalErrorStatus(String msg) {
        this.errorCode = TStatusCode.INTERNAL_ERROR;
        this.errorMsg = msg;
    }

    public void setPstatus(StatusPB status) {
        this.errorCode = TStatusCode.findByValue(status.statusCode);
        if (status.errorMsgs != null && !status.errorMsgs.isEmpty()) {
            this.errorMsg = status.errorMsgs.get(0);
        }
    }

    public void setRpcStatus(String msg) {
        this.errorCode = TStatusCode.THRIFT_RPC_ERROR;
        this.errorMsg = msg;
    }

    public void rewriteErrorMsg() {
        if (ok()) {
            return;
        }

        switch (errorCode) {
            case CANCELLED: {
                this.errorMsg = "Cancelled";
                break;
            }
            case ANALYSIS_ERROR: {
                this.errorMsg = "Analysis_error";
                break;
            }
            case NOT_IMPLEMENTED_ERROR: {
                this.errorMsg = "Not_implemented_error";
                break;
            }
            case RUNTIME_ERROR: {
                this.errorMsg = "Runtime_error";
                break;
            }
            case MEM_LIMIT_EXCEEDED: {
                this.errorMsg = "Mem_limit_error";
                break;
            }
            case INTERNAL_ERROR: {
                this.errorMsg = "Internal_error";
                break;
            }
            case THRIFT_RPC_ERROR: {
                this.errorMsg = "Thrift_rpc_error";
                break;
            }
            case TIMEOUT: {
                this.errorMsg = "Timeout";
                break;
            }
            default: {
                this.errorMsg = "Unknown_error";
                break;
            }
        }
    }

    public String getErrorCodeString() {
        return Optional.ofNullable(getErrorCode()).map(Enum::toString).orElse("UNKNOWN");
    }

    @Override
    public String toString() {
        return "errorCode " + errorCode + " " + errorMsg;
    }
}
