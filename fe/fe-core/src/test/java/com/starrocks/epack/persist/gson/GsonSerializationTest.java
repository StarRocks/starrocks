// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist.gson;

import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;

class GsonSerializationTest {
    private static class ArrayBlockingQueueAdapterTest {
        @SerializedName(value = "queue")
        private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10);

        public void init() {
            int size = queue.remainingCapacity() / 2;
            for (int i = 0; i < size; ++i) {
                queue.add(String.valueOf(i));
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ArrayBlockingQueueAdapterTest other = (ArrayBlockingQueueAdapterTest) obj;
            return Objects.equals(queue, other.queue) && queue.remainingCapacity() == other.queue.remainingCapacity();
        }
    }

    @Test
    public void testArrayBlockingQueueAdapter() {
        ArrayBlockingQueueAdapterTest adapterTest = new ArrayBlockingQueueAdapterTest();
        adapterTest.init();

        ArrayBlockingQueueAdapterTest adapterTest2 = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(adapterTest), ArrayBlockingQueueAdapterTest.class);

        Assert.assertEquals(adapterTest, adapterTest2);
    }
}