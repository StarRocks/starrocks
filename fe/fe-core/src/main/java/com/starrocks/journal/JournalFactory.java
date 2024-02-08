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


package com.starrocks.journal;

import com.starrocks.common.Config;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;

public class JournalFactory {
    public enum JournalType {
        BDB
    };

    public static Journal create(String nodeName) throws JournalException, InterruptedException {
        JournalType type = JournalType.valueOf(Config.edit_log_type.toUpperCase());
        switch (type) {
            case BDB: {
                BDBEnvironment environment = BDBEnvironment.initBDBEnvironment(nodeName);
                return new BDBJEJournal(environment);
            }

            default:
                throw new JournalException(String.format("unknown journal type %s", Config.edit_log_type));
        }
    }
}
