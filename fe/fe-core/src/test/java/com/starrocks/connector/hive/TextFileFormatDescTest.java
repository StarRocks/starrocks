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

package com.starrocks.connector.hive;

import com.starrocks.thrift.TTextFileDesc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TextFileFormatDescTest {
    @Test
    public void testToThrift() {
        TextFileFormatDesc desc = new TextFileFormatDesc(null, null, null, null);
        TTextFileDesc tTextFileDesc = desc.toThrift();
        Assertions.assertFalse(tTextFileDesc.isSetField_delim());
        Assertions.assertFalse(tTextFileDesc.isSetLine_delim());
        Assertions.assertFalse(tTextFileDesc.isSetCollection_delim());
        Assertions.assertFalse(tTextFileDesc.isSetMapkey_delim());
        Assertions.assertTrue(tTextFileDesc.isSetSkip_header_line_count());
        Assertions.assertEquals(0, tTextFileDesc.getSkip_header_line_count());

        desc = new TextFileFormatDesc("a", "b", "c", "d", 10);
        tTextFileDesc = desc.toThrift();
        Assertions.assertTrue(tTextFileDesc.isSetField_delim());
        Assertions.assertTrue(tTextFileDesc.isSetLine_delim());
        Assertions.assertTrue(tTextFileDesc.isSetCollection_delim());
        Assertions.assertTrue(tTextFileDesc.isSetMapkey_delim());
        Assertions.assertEquals("a", tTextFileDesc.getField_delim());
        Assertions.assertEquals("b", tTextFileDesc.getLine_delim());
        Assertions.assertEquals("c", tTextFileDesc.getCollection_delim());
        Assertions.assertEquals("d", tTextFileDesc.getMapkey_delim());
        Assertions.assertEquals(10, tTextFileDesc.getSkip_header_line_count());
    }


}
