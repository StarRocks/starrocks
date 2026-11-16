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

package com.starrocks.qe;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefaultCoordinatorHideHostInfoTest {

    @Test
    public void testMaskHostValueKeepsCause() {
        Assertions.assertEquals("Could not resolve host: ***",
                DefaultCoordinator.hideHostInfo("Could not resolve host: cdn-thirdparty.starrocks.com"));
        Assertions.assertEquals("fetch request failed, error: timeout, host: ***, port: 8060",
                DefaultCoordinator.hideHostInfo("fetch request failed, error: timeout, host: 10.0.0.1, port: 8060"));
        Assertions.assertEquals("primary replica on host [***] failed to sync data to secondary replica",
                DefaultCoordinator.hideHostInfo(
                        "primary replica on host [10.0.0.1] failed to sync data to secondary replica"));
        Assertions.assertEquals("unknown endpoint, host = ***",
                DefaultCoordinator.hideHostInfo("unknown endpoint, host = be-0.be.svc"));
        Assertions.assertEquals("Channel (host=***)",
                DefaultCoordinator.hideHostInfo("Channel (host=10.0.0.1)"));
        Assertions.assertEquals("Storage Reader init fail. Tablet: 10. Host: ***",
                DefaultCoordinator.hideHostInfo("Storage Reader init fail. Tablet: 10. Host: 10.0.0.1"));
    }

    @Test
    public void testMessageWithoutHostValueUnchanged() {
        String[] messages = {
                "Memory limit exceeded",
                "connect to localhost:9020 failed",
                "no alive hosts: [be1, be2]",
                "the host is down",
                "failed on ghost: 1",
                "trailing host: ",
        };
        for (String msg : messages) {
            Assertions.assertEquals(msg, DefaultCoordinator.hideHostInfo(msg));
        }
    }
}
