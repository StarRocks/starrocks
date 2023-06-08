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

package com.starrocks.connector.exception;

import static java.lang.String.format;

public class StarRocksConnectorException extends RuntimeException {
    public StarRocksConnectorException(String message) {
        super(message);
    }

    public StarRocksConnectorException(String formatString, Object... args) {
        super(format(formatString, args));
    }

    public StarRocksConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        return getErrorMessage();
    }

    public static void check(boolean test, String message, Object... args) {
        if (!test) {
            throw new StarRocksConnectorException(message, args);
        }
    }

}
