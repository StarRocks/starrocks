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
import com.google.gson.JsonParseException;
import com.google.gson.annotations.SerializedName;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.IForwardCompatibleObject;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV1;
import com.starrocks.utframe.GsonReflectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RecyclePartitionInfoForwardCompatibilityTest {
    private static final Logger LOG = LogManager.getLogger(RecyclePartitionInfoForwardCompatibilityTest.class);

    private static class RecycleNewPartitionInfoV2 extends RecyclePartitionInfoV2 {
        @SerializedName(value = "new_props")
        private final String newProperty = "hello_world";

        public RecycleNewPartitionInfoV2(long dbId, long tableId) {
            super(dbId, tableId, new Partition(3, "dummy", null, null), DataProperty.DATA_PROPERTY_HDD, (short) 1,
                    false, new DataCacheInfo(false, false));
        }
    }

    private Gson getGsonWithRegisteredSubType(Gson baseGson, Class<? extends RecyclePartitionInfoV2> clazz,
                                              String label) {
        RuntimeTypeAdapterFactory<RecyclePartitionInfoV2> partitionInfoFactory
                = RuntimeTypeAdapterFactory.of(RecyclePartitionInfoV2.class, "clazz")
                .registerSubtype(RecycleRangePartitionInfo.class, "RecycleRangePartitionInfo");
        if (clazz != null) {
            partitionInfoFactory.registerSubtype(clazz, label);
        }
        // the late registered TypeAdapterFactory wins with the same type
        return baseGson.newBuilder().registerTypeAdapterFactory(partitionInfoFactory).create();
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
        Gson purifiedGson = GsonReflectUtils.removeRuntimeTypeAdapterFactoryForBaseType(GsonUtils.GSON.newBuilder(),
                RecyclePartitionInfoV2.class).create();

        // old version, Neither `RecycleNewPartitionInfoV2` nor `ForwardCompatibleRecyclePartitionInfoV2` registered
        Gson oldVersionNoFCFallback = getGsonWithRegisteredSubType(purifiedGson, null, null);

        // new version with the correct compatible subtype registered, but no `ForwardCompatibleRecyclePartitionInfoV2`
        Gson newVersionWithSubType =
                getGsonWithRegisteredSubType(purifiedGson, RecycleNewPartitionInfoV2.class,
                        "RecycleNewPartitionInfoV2");

        // new version has only `ForwardCompatibleRecyclePartitionInfoV2` registered
        Gson newVersionFCFallback = GsonUtils.GSON;

        RecycleNewPartitionInfoV2 info = new RecycleNewPartitionInfoV2(1, 2);
        String jsonString = newVersionWithSubType.toJson(info, RecyclePartitionInfoV2.class);
        LOG.info("JSON str for the deserialize testing: {}", jsonString);

        // parse json with gson knowing the new type
        {
            RecyclePartitionInfoV2 readInfo = newVersionWithSubType.fromJson(jsonString, RecyclePartitionInfoV2.class);
            Assert.assertNotNull(readInfo);
            // can correctly recover from the json string
            Assert.assertTrue(readInfo instanceof RecycleNewPartitionInfoV2);
        }

        // parse the json with oldVersionJson that doesn't know the new type
        {
            Assert.assertThrows(JsonParseException.class,
                    () -> oldVersionNoFCFallback.fromJson(jsonString, RecyclePartitionInfoV2.class));
        }

        // parse json with gson knowing the new type registered to the dummy ForwardedCompatibleObject
        {
            RecyclePartitionInfoV2 readInfo =
                    newVersionFCFallback.fromJson(jsonString, RecyclePartitionInfoV2.class);
            Assert.assertNotNull(readInfo);
            Assert.assertTrue(readInfo instanceof IForwardCompatibleObject);
            Assert.assertTrue(readInfo instanceof ForwardCompatibleRecyclePartitionInfoV2);
        }
    }

    @Test
    public void testCatalogRecycleBinLoadAndSaveForwardCompatibility()
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        // Remove the RuntimeTypeAdapterFactory for RecyclePartitionInfoV2, because the same type can't be
        // registered repeatedly.
        Gson purifiedGson = GsonReflectUtils.removeRuntimeTypeAdapterFactoryForBaseType(GsonUtils.GSON.newBuilder(),
                RecyclePartitionInfoV2.class).create();

        // old version, Neither `RecycleNewPartitionInfoV2` nor `ForwardCompatibleRecyclePartitionInfoV2` registered
        Gson oldVersionNoFCFallback = getGsonWithRegisteredSubType(purifiedGson, null, null);

        // new version with the correct compatible subtype registered, but no `ForwardCompatibleRecyclePartitionInfoV2`
        Gson newVersionWithSubType =
                getGsonWithRegisteredSubType(purifiedGson, RecycleNewPartitionInfoV2.class,
                        "RecycleNewPartitionInfoV2");

        // new version has only `ForwardCompatibleRecyclePartitionInfoV2` registered
        Gson newVersionFCFallback = GsonUtils.GSON.newBuilder().create();

        ByteArrayOutputStream baseOS = new ByteArrayOutputStream();

        // the new object to be serialized and deserialized for the test
        RecycleNewPartitionInfoV2 info = new RecycleNewPartitionInfoV2(1000, 2000);
        long testPartitionId = info.getPartition().getId();

        RecycleRangePartitionInfo rangeInfo =
                new RecycleRangePartitionInfo(1001L, 2001L,
                        new Partition(4, "RecycleRangePartitionInfo", null, null),
                        null, DataProperty.DATA_PROPERTY_HDD,
                        (short) 1, false, new DataCacheInfo(true, true));
        long testRangePartitionId = rangeInfo.partition.getId();

        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        // add the two partitions into recycle bin
        recycleBin.recyclePartition(info);
        recycleBin.recyclePartition(rangeInfo);

        // GsonUtils.GSON = newVersionWithSubType, so the new subtype can be serialized correctly.
        GsonReflectUtils.partialMockGsonExpectations(newVersionWithSubType);

        ImageWriter writer = new ImageWriter("dir", ImageFormatVersion.v1, 0);
        writer.setOutputStream(new DataOutputStream(baseOS));

        recycleBin.save(writer);
        byte[] rawBytes = baseOS.toByteArray();
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            CatalogRecycleBin recoverRecycleBin = new CatalogRecycleBin();
            SRMetaBlockReader reader = new SRMetaBlockReaderV1(new DataInputStream(baseIn));
            recoverRecycleBin.load(reader);
            // both partitions should be loaded correctly
            Assert.assertNotNull(recoverRecycleBin.getPartition(testPartitionId));
            Assert.assertNotNull(recoverRecycleBin.getPartition(testRangePartitionId));
        }

        // test `oldVersionJson`, the new subtype will be excepted
        GsonReflectUtils.partialMockGsonExpectations(oldVersionNoFCFallback);
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            CatalogRecycleBin recoverRecycleBin = new CatalogRecycleBin();
            SRMetaBlockReader reader = new SRMetaBlockReaderV1(new DataInputStream(baseIn));
            Assert.assertThrows(JsonParseException.class, () -> recoverRecycleBin.load(reader));
        }

        // test `oldJsonWithForwardCompatibility`
        // the new subtype is registered as the ForwardCompatibleRecyclePartitionInfoV2
        GsonReflectUtils.partialMockGsonExpectations(newVersionFCFallback);
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            CatalogRecycleBin recoverRecycleBin = new CatalogRecycleBin();
            SRMetaBlockReader reader = new SRMetaBlockReaderV1(new DataInputStream(baseIn));
            recoverRecycleBin.load(reader);
            // new subtype partition is ignored
            Partition p = recoverRecycleBin.getPartition(testPartitionId);
            Assert.assertNull(p);
            // range partition should be loaded correctly
            Assert.assertNotNull(recoverRecycleBin.getPartition(testRangePartitionId));
        }
    }
}
