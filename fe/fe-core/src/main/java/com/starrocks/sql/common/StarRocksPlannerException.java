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

package com.starrocks.sql.common;

import com.google.common.base.Strings;

public class StarRocksPlannerException extends RuntimeException {
    private final ErrorType type;

    public ErrorType getType() {
        return type;
    }

    public StarRocksPlannerException(String message, ErrorType type) {
        super(message);
        this.type = type;
    }

    public StarRocksPlannerException(ErrorType type, String messageTemplate,  Object... errorMessageArgs) {
        super(Strings.lenientFormat(messageTemplate, errorMessageArgs));
        this.type = type;
    }



    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        return message;
    }
}
