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


package com.staros.exception;

import com.staros.proto.StarStatus;
import com.staros.proto.StatusCode;
import org.apache.logging.log4j.message.ParameterizedMessage;

public class StarException extends RuntimeException {
    private final ExceptionCode code;

    public StarException(ExceptionCode code, String msg) {
        super(msg);
        this.code = code;
    }

    public StarException(ExceptionCode code, String messagePattern, Object ... params) {
        this(code, ParameterizedMessage.format(messagePattern, params));
    }

    public ExceptionCode getExceptionCode() {
        return code;
    }

    public StarStatus toStatus() {
        StatusCode c;
        switch (code) {
            case INVALID_ARGUMENT:
                c = StatusCode.INVALID_ARGUMENT;
                break;
            case NOT_EXIST:
                c = StatusCode.NOT_EXIST;
                break;
            case ALREADY_EXIST:
                c = StatusCode.ALREADY_EXIST;
                break;
            case NOT_ALLOWED:
                c = StatusCode.NOT_ALLOWED;
                break;
            case JOURNAL:
                c = StatusCode.JOURNAL;
                break;
            case IO:
                c = StatusCode.IO;
                break;
            case NOT_LEADER:
                c = StatusCode.NOT_LEADER;
                break;
            case NOT_IMPLEMENTED:
                c = StatusCode.NOT_IMPLEMENTED;
                break;
            case FAILED_PRECONDITION:
                c = StatusCode.FAILED_PRECONDITION;
                break;
            case WORKER_NOT_HEALTHY:
                c = StatusCode.WORKER_NOT_HEALTHY;
                break;
            default:
                c = StatusCode.UNKNOWN;
                break;
        }
        return StarStatus.newBuilder().setStatusCode(c).setErrorMsg(getMessage()).build();
    }
}
