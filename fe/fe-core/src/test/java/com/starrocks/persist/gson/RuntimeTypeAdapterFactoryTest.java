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
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(1, b.getA());
        Assert.assertEquals(2, b.getB());

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
}
