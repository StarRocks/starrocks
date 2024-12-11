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
<<<<<<< HEAD
import org.junit.Assert;
import org.junit.Test;
=======
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

public class TestSelectedFields {

    @Test
    public void testBasic() {
        String s = "e.a,e.b,e.c";
        SelectedFields ssf = new SelectedFields();
        ssf.addMultipleNestedPath(s);
<<<<<<< HEAD
        Assert.assertEquals(ssf.getFields().size(), 1);
        Assert.assertEquals(ssf.getFields().get(0), "e");

        SelectedFields ssf2 = ssf.findChildren("e");
        Assert.assertEquals(ssf2.getFields().size(), 3);
        Assert.assertEquals(String.join(",", ssf2.getFields()), "a,b,c");
=======
        Assertions.assertEquals(ssf.getFields().size(), 1);
        Assertions.assertEquals(ssf.getFields().get(0), "e");

        SelectedFields ssf2 = ssf.findChildren("e");
        Assertions.assertEquals(ssf2.getFields().size(), 3);
        Assertions.assertEquals(String.join(",", ssf2.getFields()), "a,b,c");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
}

