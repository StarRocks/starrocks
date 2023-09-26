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

package com.starrocks.sql.ast.pipe;

import com.google.common.base.Preconditions;
import com.starrocks.sql.parser.NodePosition;

/**
 * ALTER PIPE <pipe>
 * RETRY ALL |
 * RETRY FILE <filename>
 */
public class AlterPipeClauseRetry extends AlterPipeClause {

    private final boolean retryAll;
    private final String file;

    public AlterPipeClauseRetry(NodePosition pos, boolean retryAll) {
        super(pos);
        Preconditions.checkArgument(retryAll, "must be retry all");
        this.retryAll = true;
        this.file = null;
    }

    public AlterPipeClauseRetry(NodePosition pos, boolean retryAll, String fileName) {
        super(pos);
        Preconditions.checkArgument(!retryAll, "must be retry all");
        this.retryAll = false;
        this.file = fileName;
    }

    public boolean isRetryAll() {
        return retryAll;
    }

    public String getFile() {
        return file;
    }
}
