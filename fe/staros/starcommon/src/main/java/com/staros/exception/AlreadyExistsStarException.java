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

public class AlreadyExistsStarException extends StarException {
    private static final ExceptionCode CODE = ExceptionCode.ALREADY_EXIST;

    public AlreadyExistsStarException(String msg) {
        super(CODE, msg);
    }

    public AlreadyExistsStarException(String messagePattern, Object ... params) {
        super(CODE, messagePattern, params);
    }
}
