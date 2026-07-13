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

package com.starrocks.http.rest.context;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;

/**
 * Structured error envelope per API doc §12.2 — {@code error_code}, {@code error_class},
 * {@code message}, {@code retryable}, {@code degrade_suggestion}, {@code request_id}.
 *
 * <p>Every context REST action that catches a {@link ContextException} should send one of these
 * via {@link RestBaseAction#sendResultByJson(BaseRequest, BaseResponse, Object)} so dashboards and
 * SDK clients can build retry/backoff logic without scraping free-form text.
 */
public final class ContextErrorResult {

    public static ContextErrorResult fromException(ContextException ex, String requestId) {
        ContextErrorResult r = new ContextErrorResult();
        ContextErrorCode code = ex.getCode();
        r.errorCode = code.name();
        r.errorClass = code.errorClass();
        r.message = ex.getMessage();
        r.retryable = code.retryable();
        r.degradeSuggestion = ex.getEffectiveDegradeSuggestion();
        r.requestId = requestId;
        return r;
    }

    public static ContextErrorResult forCode(ContextErrorCode code, String message, String requestId) {
        ContextErrorResult r = new ContextErrorResult();
        r.errorCode = code.name();
        r.errorClass = code.errorClass();
        r.message = message;
        r.retryable = code.retryable();
        r.degradeSuggestion = code.degradeSuggestion();
        r.requestId = requestId;
        return r;
    }

    @JsonProperty("error_code")
    public String errorCode;

    @JsonProperty("error_class")
    public String errorClass;

    public String message;
    public boolean retryable;

    @JsonProperty("degrade_suggestion")
    public String degradeSuggestion;

    @JsonProperty("request_id")
    public String requestId;
}
