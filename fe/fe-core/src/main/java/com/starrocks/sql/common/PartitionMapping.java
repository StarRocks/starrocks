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

import java.time.LocalDateTime;

public class PartitionMapping {

    // the lowerDateTime is closed
    private final LocalDateTime lowerDateTime;
    // the upperDateTime is open
    private final LocalDateTime upperDateTime;

    public PartitionMapping(LocalDateTime lowerDateTime, LocalDateTime upperDateTime) {

        this.lowerDateTime = lowerDateTime;
        this.upperDateTime = upperDateTime;
    }


    public LocalDateTime getLowerDateTime() {
        return lowerDateTime;
    }

    public LocalDateTime getUpperDateTime() {
        return upperDateTime;
    }
}
