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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/ErrorReport.java

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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.optimizer.validate.ValidateException;

// Used to report error happened when execute SQL of user
public class ErrorReport {

    private static String reportCommon(String pattern, ErrorCode errorCode, Object... objs) {
        String errMsg;
        if (pattern == null) {
            errMsg = errorCode.formatErrorMsg(objs);
        } else {
            errMsg = String.format(pattern, objs);
        }
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            ctx.getState().setError(errMsg);
        }
        // TODO(zc): think about LOG to file
        return errMsg;
    }

    public static SemanticException buildSemanticException(ErrorCode errorCode, Object... objs) {
        return new SemanticException(reportCommon(null, errorCode, objs));
    }

    public static void reportAnalysisException(String pattern, Object... objs)
            throws AnalysisException {
        throw new AnalysisException(reportCommon(pattern, ErrorCode.ERR_UNKNOWN_ERROR, objs));
    }

    public static void reportAnalysisException(ErrorCode errorCode, Object... objs)
            throws AnalysisException {
        reportAnalysisException(null, errorCode, objs);
    }

    public static void reportSemanticException(ErrorCode errorCode, Object... objs) {
        reportSemanticException(null, errorCode, objs);
    }

    public static void reportSemanticException(String pattern, ErrorCode errorCode, Object... objs) {
        throw new SemanticException(reportCommon(pattern, errorCode, objs));
    }

    public static void reportAnalysisException(String pattern, ErrorCode errorCode, Object... objs)
            throws AnalysisException {
        throw new AnalysisException(reportCommon(pattern, errorCode, objs));
    }

    public static void reportDdlException(String pattern, Object... objs)
            throws DdlException {
        reportDdlException(pattern, ErrorCode.ERR_UNKNOWN_ERROR, objs);
    }

    public static void reportDdlException(ErrorCode errorCode, Object... objs)
            throws DdlException {
        reportDdlException(null, errorCode, objs);
    }

    public static void reportDdlException(String pattern, ErrorCode errorCode, Object... objs)
            throws DdlException {
        throw new DdlException(reportCommon(pattern, errorCode, objs));
    }

    public static void reportValidateException(ErrorCode errorCode, ErrorType errorType, Object... objs) {
        throw new ValidateException(errorCode.formatErrorMsg(objs), errorType);
    }

    public interface DdlExecutor {
        void apply() throws UserException;
    }

    public static void wrapWithRuntimeException(DdlExecutor fun) {
        try {
            fun.apply();
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    public static void report(String pattern, Object... objs) {
        report(pattern, ErrorCode.ERR_UNKNOWN_ERROR, objs);
    }

    public static void report(ErrorCode errorCode, Object... objs) {
        report(null, errorCode, objs);
    }

    public static void report(String pattern, ErrorCode errorCode, Object... objs) {
        reportCommon(pattern, errorCode, objs);
    }
}
