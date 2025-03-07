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

import com.starrocks.jni.connector.ScannerHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestScannerHelper {

    @Test
    public void testSplitAndOmitEmptyStrings() {
        {
            String[] values = ScannerHelper.splitAndOmitEmptyStrings("hello,world", ",");
            Assertions.assertEquals(values.length, 2);
            Assertions.assertEquals(values[0], "hello");
            Assertions.assertEquals(values[1], "world");
        }
        {
            String[] values = ScannerHelper.splitAndOmitEmptyStrings(",", ",");
            Assertions.assertEquals(values.length, 0);
        }
        {
            String[] values = ScannerHelper.splitAndOmitEmptyStrings("", ",");
            Assertions.assertEquals(values.length, 0);
        }
        {
            String[] values = ScannerHelper.splitAndOmitEmptyStrings(",hello", ",");
            Assertions.assertEquals(values.length, 1);
            Assertions.assertEquals(values[0], "hello");
        }
    }
}

