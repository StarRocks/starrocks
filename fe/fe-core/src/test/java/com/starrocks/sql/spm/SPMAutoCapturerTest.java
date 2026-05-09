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

package com.starrocks.sql.spm;

import com.starrocks.qe.ConnectContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SPMAutoCapturerTest {

    @Test
    public void testOnStoppedDropsCapturedConnectContext() throws Exception {
        SPMAutoCapturer capturer = new SPMAutoCapturer();

        ConnectContext captured = Mockito.mock(ConnectContext.class);
        FieldUtils.writeField(capturer, "connect", captured, true);
        Assertions.assertSame(captured, FieldUtils.readField(capturer, "connect", true),
                "precondition: connect installed");

        // The captured ConnectContext is leader-session-only state (carries leader-side query
        // execution state). On demotion it must be released so the next leader rebuilds a
        // fresh context and the demoted FE does not retain references into leader-only state.
        MethodUtils.invokeMethod(capturer, true, "onStopped");

        Assertions.assertNull(FieldUtils.readField(capturer, "connect", true),
                "captured ConnectContext should be cleared on demotion");
    }
}
