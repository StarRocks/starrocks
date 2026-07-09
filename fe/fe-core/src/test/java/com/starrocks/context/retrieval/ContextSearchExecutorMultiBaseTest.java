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

package com.starrocks.context.retrieval;

import com.starrocks.context.ContextMgr;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Guards on the multi-contextbase search mode in {@link ContextSearchExecutor}: snapshot
 * selectors (as_of_time / snapshot_version) are rejected because snapshot versions are numbered
 * per contextbase, so a single fence is meaningless across a multi-base scope.
 */
public class ContextSearchExecutorMultiBaseTest {

    private static ContextSearchExecutor newExecutor() {
        return new ContextSearchExecutor(new ContextMgr(), new TextSearchExecutor(),
                new ReferenceExpander());
    }

    private static ContextSearchExecutor.Request multiBaseRequest() {
        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBaseIdsOverride = Arrays.asList(7L, 9L);
        req.queryText = "demo";
        return req;
    }

    @Test
    public void rejectsAsOfTimeForMultiBase() {
        ContextSearchExecutor.Request req = multiBaseRequest();
        req.asOfTime = "2026-01-01T00:00:00Z";
        ContextException ex = Assertions.assertThrows(ContextException.class,
                () -> newExecutor().search(req));
        Assertions.assertEquals(ContextErrorCode.INVALID_ARGUMENT, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("multiple"), ex.getMessage());
    }

    @Test
    public void rejectsSnapshotVersionForMultiBase() {
        ContextSearchExecutor.Request req = multiBaseRequest();
        req.snapshotVersion = 123L;
        ContextException ex = Assertions.assertThrows(ContextException.class,
                () -> newExecutor().search(req));
        Assertions.assertEquals(ContextErrorCode.INVALID_ARGUMENT, ex.getCode());
    }
}
