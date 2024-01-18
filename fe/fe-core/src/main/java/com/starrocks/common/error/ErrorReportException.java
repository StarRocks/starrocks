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

package com.starrocks.common.error;

import com.starrocks.qe.ConnectContext;

public class ErrorReportException extends RuntimeException {
    private final ErrorCode errorCode;

    private ErrorReportException(ErrorCode errorCode, String errorMsg) {
        super(errorMsg);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static String report(ErrorCode errorCode, Object... objs) {
        String errMsg = errorCode.formatErrorMsg(objs);
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            ctx.getState().setError(errMsg);
            ctx.getState().setErrorCode(errorCode);
        }
        throw new ErrorReportException(errorCode, errMsg);
    }
}
