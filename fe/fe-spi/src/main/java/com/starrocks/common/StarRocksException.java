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

package com.starrocks.common;

import com.google.common.base.Strings;

/**
 * StarRocksException is the base class of internal exceptions.
 * If ErrorCode is used in the constructor passed in, the relevant Error information will be recorded in ConnectContext.
 * This information will be recorded as the status code of the current stmt executed by this session.
 * <p>
 * The stmt of a session will expose three status codes: {@link ErrorCode>}
 * 1、ErrorCode: The most granular error message, recording the error cause at the bottom of the call stack
 * <p>
 * 2、SqlState: Coarse-grained error information is also recorded in ErrorCode
 * Main references: <a href="https://www.postgresql.org/docs/15/errcodes-appendix.html">...</a>
 * <p>
 * 3、ErrorType: The most coarse-grained error message, which determines the error category
 * based on the first two digits of SQLSTATE. For example, Internal Error, Syntax Error
 */
public class StarRocksException extends Exception {
    private InternalErrorCode internalErrorCode;
    private ErrorCode errorCode;

    public StarRocksException(ErrorCode errorCode, Object... objs) {
        super(errorCode.formatErrorMsg(objs));
        this.errorCode = errorCode;
    }

    public StarRocksException(String msg, Throwable cause) {
        super(Strings.nullToEmpty(msg), cause);
        internalErrorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public StarRocksException(Throwable cause) {
        super(cause);
        internalErrorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public StarRocksException(String msg) {
        super(Strings.nullToEmpty(msg));
        internalErrorCode = InternalErrorCode.INTERNAL_ERR;
    }

    public StarRocksException(InternalErrorCode errCode, String msg) {
        super(Strings.nullToEmpty(msg));
        this.internalErrorCode = errCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public InternalErrorCode getInternalErrorCode() {
        return internalErrorCode;
    }
}
