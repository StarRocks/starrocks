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

package com.starrocks.persist.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;
import com.starrocks.lake.snapshot.ManualClusterSnapshot;
import com.starrocks.lake.snapshot.ManualClusterSnapshotJob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RuntimeTypeAdapterFactoryTest {
    @Test
    public void test() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapterFactory(RuntimeTypeAdapterFactory
                .of(A.class, "clazz")
                .registerSubtype(B.class, "B")
                .registerSubtype(A.class, "A"));
        Gson gson = builder.create();
        B b = gson.fromJson("{\"a\":1,\"b\":2}", B.class);
        Assertions.assertEquals(1, b.getA());
        Assertions.assertEquals(2, b.getB());

        A a = new A();
        a.a = 10;
        String jsonStr = gson.toJson(a);
        System.out.println(jsonStr);
        A a2 = gson.fromJson(jsonStr, A.class);
        System.out.println(a2.a);
    }

    public static class A {
        @SerializedName("a")
        private int a;

        public int getA() {
            return a;
        }

        public void setA(int a) {
            this.a = a;
        }
    }

    public static class B extends A {
        @SerializedName("b")
        private int b;

        public int getB() {
            return b;
        }

        public void setB(int b) {
            this.b = b;
        }
    }

    public static class WrapperClass {
        @SerializedName(value = "ClusterSnapshotJob")
        public ClusterSnapshotJob job;

        public WrapperClass() {
        }

        public void setJob(ClusterSnapshotJob job) {
            this.job = job;
        }

        public ClusterSnapshotJob getJob() {
            return job;
        }
    }

    @Test
    public void testClusterSnapshotRelativeClass() {
        // test Cluster Snapshot
        {
            WrapperClass wrapper = new WrapperClass();
            wrapper.setJob(new ManualClusterSnapshotJob(2, "test3", "test4", 20));
            String str1 = GsonUtils.GSON.toJson(wrapper);
            WrapperClass deserializedWrapper = GsonUtils.GSON.fromJson(str1, WrapperClass.class);
            Assertions.assertTrue(deserializedWrapper.getJob() instanceof ManualClusterSnapshotJob);
            Assertions.assertTrue(deserializedWrapper.getJob().getSnapshot() instanceof ManualClusterSnapshot);
        }

        {
            WrapperClass wrapper = new WrapperClass();
            wrapper.setJob(new ClusterSnapshotJob(1, "test1", "test2", 10));
            String str1 = GsonUtils.GSON.toJson(wrapper);
            WrapperClass deserializedWrapper = GsonUtils.GSON.fromJson(str1, WrapperClass.class);
            Assertions.assertTrue(!(deserializedWrapper.getJob() instanceof ManualClusterSnapshotJob));
            Assertions.assertTrue(!(deserializedWrapper.getJob().getSnapshot() instanceof ManualClusterSnapshot));
        }
    }
}
