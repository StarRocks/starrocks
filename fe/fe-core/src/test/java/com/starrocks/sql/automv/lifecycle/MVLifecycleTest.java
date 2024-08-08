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

package com.starrocks.sql.automv.lifecycle;

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.util.TieredList;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class MVLifecycleTest {
    @Test
    public void testMVChangeLog() throws IOException {
        MVName mvName = MVName.generateFromQuery("abc");
        MVChangeLog changeLog = new MVChangeLog(mvName, TieredList.genesis());
        changeLog = changeLog.addNewEntry(MVPhase.MP_CRADLE);
        changeLog = changeLog.addNewEntry(MVPhase.MP_INTERN);
        changeLog = changeLog.addNewEntry(MVPhase.MP_INTERN);
        String jsonData = GsonUtils.GSON.toJson(changeLog);
        Assert.assertTrue(jsonData, jsonData.contains("MP_CRADLE") && jsonData.contains("MP_INTERN"));
        MVChangeLog changeLog1 = GsonUtils.GSON.fromJson(jsonData, MVChangeLog.Builder.class).build();
        String jsonData1 = GsonUtils.GSON.toJson(changeLog1);
        Assert.assertEquals(jsonData1, jsonData);
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(byteOutput);
        changeLog1.write(dataOutput);
        ByteArrayInputStream byteInput = new ByteArrayInputStream(byteOutput.toByteArray());
        DataInput dataInput = new DataInputStream(byteInput);
        MVChangeLog changeLog2 = MVChangeLog.read(dataInput);
        String jsonData2 = GsonUtils.GSON.toJson(changeLog2);
        Assert.assertEquals(jsonData, jsonData2);
    }
}
