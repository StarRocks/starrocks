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

public enum ExceptionCode {
    INVALID_ARGUMENT,
    NOT_EXIST,
    ALREADY_EXIST,
    NOT_ALLOWED,
    GRPC,
    JOURNAL,
    IO,
    NOT_LEADER,
    SCHEDULE,
    INTERNAL,
    RESOURCE_EXHAUSTED,
    NOT_IMPLEMENTED,
    FAILED_PRECONDITION,
    WORKER_NOT_HEALTHY,
    INTERRUPTED,
}
