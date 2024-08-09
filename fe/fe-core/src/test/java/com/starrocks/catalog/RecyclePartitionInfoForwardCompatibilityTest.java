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

package com.starrocks.catalog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.RecycleLakeListPartitionInfo;
import com.starrocks.lake.RecycleLakeRangePartitionInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.IForwardCompatibleObject;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import mockit.Delegate;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;

public class RecyclePartitionInfoForwardCompatibilityTest {

    public static class RecycleNewPartitionInfoV2 extends RecyclePartitionInfoV2 {
        @SerializedName(value = "new_props")
        private final String newProperty = "hello_world";

        public RecycleNewPartitionInfoV2(long dbId, long tableId) {
            super(dbId, tableId, new Partition(3, "dummy", null, null), DataProperty.DATA_PROPERTY_HDD, (short) 1,
                    false, new DataCacheInfo(false, false));
        }
    }

    Gson getJsonWithRegisteredSubType(Class<? extends RecyclePartitionInfoV2> clazz, String label) {
        RuntimeTypeAdapterFactory<RecyclePartitionInfoV2> partitionInfoFactory
                = RuntimeTypeAdapterFactory.of(RecyclePartitionInfoV2.class, "clazz")
                .registerSubtype(RecycleRangePartitionInfo.class, "RecycleRangePartitionInfo")
                .registerSubtype(RecycleLakeRangePartitionInfo.class, "RecycleLakeRangePartitionInfo")
                .registerSubtype(RecycleListPartitionInfo.class, "RecycleListPartitionInfo")
                .registerSubtype(RecycleLakeListPartitionInfo.class, "RecycleLakeListPartitionInfo");
        // register the new type
        partitionInfoFactory.registerSubtype(clazz, label);
        // the late registered TypeAdapterFactory wins with the same type
        GsonBuilder gsonBuilder = GsonUtils.GSON.newBuilder().registerTypeAdapterFactory(partitionInfoFactory);
        return gsonBuilder.create();
    }

    // Dummy test case just for code coverage.
    // The `ForwardCompatibleRecyclePartitionInfoV2` will be rarely used at all
    @Test
    public void testForwardCompatibleRecyclePartitionInfoV2Creation() {
        ForwardCompatibleRecyclePartitionInfoV2 info =
                new ForwardCompatibleRecyclePartitionInfoV2(1, 2, new Partition(3, "dummy", null, null),
                        DataProperty.DATA_PROPERTY_HDD, (short) 1,
                        false, new DataCacheInfo(false, false));
        Assert.assertEquals(3, info.getPartition().getId());
    }

    @Test
    public void testNewPartitionInfoDeserializeFailed() {
        // old version doesn't have the compatible subtype registered
        Gson oldVersionJson = GsonUtils.GSON;
        // new version with the correct compatible subtype registered
        Gson newVersionJson =
                getJsonWithRegisteredSubType(RecycleNewPartitionInfoV2.class, "RecycleNewPartitionInfoV2");
        // old version with the subtype registered to the dummy `ForwardCompatibleRecyclePartitionInfoV2`
        Gson oldJsonWithForwardCompatibility =
                getJsonWithRegisteredSubType(ForwardCompatibleRecyclePartitionInfoV2.class,
                        "RecycleNewPartitionInfoV2");

        RecycleNewPartitionInfoV2 info = new RecycleNewPartitionInfoV2(1, 2);
        String jsonString = newVersionJson.toJson(info);

        // parse json with gson knowing the new type
        {
            RecyclePartitionInfoV2 readInfo = newVersionJson.fromJson(jsonString, RecyclePartitionInfoV2.class);
            Assert.assertNotNull(readInfo);
            // can correctly recover from the json string
            Assert.assertTrue(readInfo instanceof RecycleNewPartitionInfoV2);
        }

        // parse the json with oldVersionJson that doesn't know the new type
        {
            Assert.assertThrows(JsonParseException.class,
                    () -> oldVersionJson.fromJson(jsonString, RecyclePartitionInfoV2.class));
        }

        // parse json with gson knowing the new type registered to the dummy ForwardedCompatibleObject
        {
            RecyclePartitionInfoV2 readInfo =
                    oldJsonWithForwardCompatibility.fromJson(jsonString, RecyclePartitionInfoV2.class);
            Assert.assertNotNull(readInfo);
            Assert.assertTrue(readInfo instanceof IForwardCompatibleObject);
            Assert.assertTrue(readInfo instanceof ForwardCompatibleRecyclePartitionInfoV2);
        }
    }

    Expectations partialMockGSONExpectations(Gson gson) {
        return new Expectations(GsonUtils.GSON) {
            {
                GsonUtils.GSON.toJson(any);
                result = new Delegate() {
                    public String toJson(Object src) {
                        return gson.toJson(src);
                    }
                };
                minTimes = 0;

                GsonUtils.GSON.fromJson(anyString, (Type) any);
                result = new Delegate() {
                    public <T> T fromJson(String json, Type typeOfT) throws JsonSyntaxException {
                        return gson.fromJson(json, typeOfT);
                    }
                };
            }
        };
    }

    @Test
    public void testCatalogRecycleBinLoadAndSaveForwardCompatibility()
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException, IllegalAccessException,
            NoSuchFieldException {
        // old version doesn't have the compatible subtype registered
        Gson oldVersionJson = GsonUtils.GSON.newBuilder().create();
        // new version with the correct compatible subtype registered
        Gson newVersionJson =
                getJsonWithRegisteredSubType(RecycleNewPartitionInfoV2.class, "RecycleNewPartitionInfoV2");
        // old version with the subtype registered to the dummy `ForwardCompatibleRecyclePartitionInfoV2`
        Gson oldJsonWithForwardCompatibility =
                getJsonWithRegisteredSubType(ForwardCompatibleRecyclePartitionInfoV2.class,
                        "RecycleNewPartitionInfoV2");

        ByteArrayOutputStream baseOS = new ByteArrayOutputStream();

        // the new object to be serialized and deserialized for the test
        RecycleNewPartitionInfoV2 info = new RecycleNewPartitionInfoV2(1000, 2000);
        long testPartitionId = info.getPartition().getId();

        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recyclePartition(info);

        // GsonUtils.GSON = newVersionJson
        partialMockGSONExpectations(newVersionJson);

        recycleBin.save(new DataOutputStream(baseOS));
        byte[] rawBytes = baseOS.toByteArray();
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            CatalogRecycleBin recoverRecycleBin = new CatalogRecycleBin();
            SRMetaBlockReader reader = new SRMetaBlockReader(new DataInputStream(baseIn));
            recoverRecycleBin.load(reader);
            Partition p = recoverRecycleBin.getPartition(testPartitionId);
            Assert.assertNotNull(p);
        }

        // test `oldVersionJson`, the new subtype will be excepted
        partialMockGSONExpectations(oldVersionJson);
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            CatalogRecycleBin recoverRecycleBin = new CatalogRecycleBin();
            SRMetaBlockReader reader = new SRMetaBlockReader(new DataInputStream(baseIn));
            Assert.assertThrows(JsonParseException.class, () -> recoverRecycleBin.load(reader));
        }

        // test `oldJsonWithForwardCompatibility`
        // the new subtype is registered as the ForwardCompatibleRecyclePartitionInfoV2
        partialMockGSONExpectations(oldJsonWithForwardCompatibility);
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            CatalogRecycleBin recoverRecycleBin = new CatalogRecycleBin();
            SRMetaBlockReader reader = new SRMetaBlockReader(new DataInputStream(baseIn));
            recoverRecycleBin.load(reader);
            Partition p = recoverRecycleBin.getPartition(testPartitionId);
            Assert.assertNull(p);
        }
    }
}
