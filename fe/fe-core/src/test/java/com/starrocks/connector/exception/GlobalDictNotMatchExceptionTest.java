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

import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.thrift.TStatusCode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class GlobalDictNotMatchExceptionTest {
    @Test
    public void testExtract() {
        String msg = "xxx, SlotId: 1, Not dict encoded and not low rows on global dict column. xxx";
        Status status = new Status(TStatusCode.GLOBAL_DICT_NOT_MATCH, msg);
        if (status.isGlobalDictNotMatch()) {
            try {
                throw new GlobalDictNotMatchException(status.getErrorMsg());
            } catch (GlobalDictNotMatchException e) {
                Assert.assertEquals(e.getMessage(), msg);
                Pair<Optional<Integer>, Optional<String>> res = e.extract();
                Assert.assertTrue(res.first.isPresent());
                Assert.assertEquals(1, res.first.get().intValue());
                Assert.assertTrue(res.second.isEmpty());
            }
        }
        status = new Status(TStatusCode.GLOBAL_DICT_NOT_MATCH,
                "xxx, SlotId: 12, FileName: test://test/abc , file doesn't match global dict. xxx");
        if (status.isGlobalDictError()) {
            try {
                throw new GlobalDictNotMatchException(status.getErrorMsg());
            } catch (GlobalDictNotMatchException e) {
                Pair<Optional<Integer>, Optional<String>> res = e.extract();
                Assert.assertTrue(res.first.isPresent());
                Assert.assertEquals(12, res.first.get().intValue());
                Assert.assertTrue(res.second.isPresent());
                Assert.assertEquals("test://test/abc", res.second.get());
            }
        }
        status = new Status(TStatusCode.GLOBAL_DICT_NOT_MATCH,
                "xxx, SlotId: x12, FileName: test://test/abc , file doesn't match global dict. xxx");
        if (status.isGlobalDictError()) {
            try {
                throw new GlobalDictNotMatchException(status.getErrorMsg());
            } catch (GlobalDictNotMatchException e) {
                Pair<Optional<Integer>, Optional<String>> res = e.extract();
                Assert.assertTrue(res.first.isEmpty());
                Assert.assertTrue(res.second.isEmpty());
            }
        }
    }
}
