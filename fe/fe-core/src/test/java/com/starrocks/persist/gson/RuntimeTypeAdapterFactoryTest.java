package com.starrocks.persist.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.junit.Test;

public class RuntimeTypeAdapterFactoryTest {
    @Test
    public void test() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapterFactory(RuntimeTypeAdapterFactory
                .of(A.class, "clazz")
                .registerSubtype(B.class, B.class.getSimpleName()));
        Gson gson = builder.create();
        B b = gson.fromJson("{\"a\":1,\"b\":2}", B.class);
        System.out.println("a: " + b.getA() + ", b: " + b.getB());
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
