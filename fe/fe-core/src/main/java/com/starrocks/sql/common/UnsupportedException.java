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

public class UnsupportedException extends StarRocksPlannerException {
    private UnsupportedException(String formatString) {
        super(formatString, ErrorType.UNSUPPORTED);
    }

    public static UnsupportedException unsupportedException(String formatString) {
        throw new UnsupportedException(formatString);
    }
}
