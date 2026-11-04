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

package com.starrocks.http.action;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class IndexActionHardwareInfoTest {

    @Test
    public void testLinesAreEncodedAndSeparatorStaysMarkup() {
        // e.g. a process on the FE host named "<img src=x>", and a disk model containing '&'
        List<String> lines = List.of(
                "   PID  %CPU %MEM       VSZ       RSS Name",
                " 62766  95.8  0.0   5.6 MiB   1.9 MiB <img src=x onerror=\"alert(1)\">",
                " /dev/vda: (model: a&b - S/N: 'x')");
        Assertions.assertEquals(
                "   PID  %CPU %MEM       VSZ       RSS Name"
                        + "<br/> 62766  95.8  0.0   5.6 MiB   1.9 MiB &lt;img src=x onerror=&#34;alert(1)&#34;&gt;"
                        + "<br/> /dev/vda: (model: a&amp;b - S/N: &#39;x&#39;)",
                IndexAction.joinEncodedLines(lines));
    }

    @Test
    public void testEmptyLines() {
        Assertions.assertEquals("", IndexAction.joinEncodedLines(List.of()));
    }
}
