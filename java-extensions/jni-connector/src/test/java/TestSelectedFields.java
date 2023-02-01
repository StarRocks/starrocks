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

import com.starrocks.jni.connector.SelectedFields;
import org.junit.Assert;
import org.junit.Test;

public class TestSelectedFields {

    @Test
    public void testBasic() {
        String s = "e.a,e.b,e.c";
        SelectedFields ssf = new SelectedFields();
        ssf.addMultipleNestedPath(s);
        Assert.assertEquals(ssf.getFields().size(), 1);
        Assert.assertEquals(ssf.getFields().get(0), "e");

        SelectedFields ssf2 = ssf.findChildren("e");
        Assert.assertEquals(ssf2.getFields().size(), 3);
        Assert.assertEquals(String.join(",", ssf2.getFields()), "a,b,c");
    }
}

