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

package com.starrocks.context.error;

/**
 * Exception type carrying a {@link ContextErrorCode}. REST actions catch this to render the
 * structured error response; SQL paths can also let it bubble up since {@link RuntimeException}
 * is on the unchecked side.
 */
public class ContextException extends RuntimeException {

    private final ContextErrorCode code;
    private final String degradeSuggestionOverride;

    public ContextException(ContextErrorCode code, String message) {
        super(message);
        this.code = code;
        this.degradeSuggestionOverride = null;
    }

    public ContextException(ContextErrorCode code, String message, String degradeSuggestionOverride) {
        super(message);
        this.code = code;
        this.degradeSuggestionOverride = degradeSuggestionOverride;
    }

    public ContextErrorCode getCode() {
        return code;
    }

    public String getEffectiveDegradeSuggestion() {
        return degradeSuggestionOverride != null ? degradeSuggestionOverride : code.degradeSuggestion();
    }
}
