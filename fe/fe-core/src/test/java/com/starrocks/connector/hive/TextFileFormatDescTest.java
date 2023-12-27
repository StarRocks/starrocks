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
import org.junit.Assert;
import org.junit.Test;

public class TextFileFormatDescTest {
    @Test
    public void testToThrift() {
        TextFileFormatDesc desc = new TextFileFormatDesc(null, null, null, null);
        TTextFileDesc tTextFileDesc = desc.toThrift();
        Assert.assertFalse(tTextFileDesc.isSetField_delim());
        Assert.assertFalse(tTextFileDesc.isSetLine_delim());
        Assert.assertFalse(tTextFileDesc.isSetCollection_delim());
        Assert.assertFalse(tTextFileDesc.isSetMapkey_delim());

        desc = new TextFileFormatDesc("a", "b", "c", "d");
        tTextFileDesc = desc.toThrift();
        Assert.assertTrue(tTextFileDesc.isSetField_delim());
        Assert.assertTrue(tTextFileDesc.isSetLine_delim());
        Assert.assertTrue(tTextFileDesc.isSetCollection_delim());
        Assert.assertTrue(tTextFileDesc.isSetMapkey_delim());
        Assert.assertEquals("a", tTextFileDesc.getField_delim());
        Assert.assertEquals("b", tTextFileDesc.getLine_delim());
        Assert.assertEquals("c", tTextFileDesc.getCollection_delim());
        Assert.assertEquals("d", tTextFileDesc.getMapkey_delim());
    }


}
