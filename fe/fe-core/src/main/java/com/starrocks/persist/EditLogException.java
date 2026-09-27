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

package com.starrocks.persist;

/**
 * Unchecked exception for a failed EditLog write: the WAL admission gate was closed, or the journal task was
 * aborted / interrupted before it committed. It is unchecked so the common no-throw log* callers stay
 * unaffected; a caller that wants to react to a write failure can catch this single type directly.
 */
public class EditLogException extends RuntimeException {
    public EditLogException(String message) {
        super(message);
    }

    public EditLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
