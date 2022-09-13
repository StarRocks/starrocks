package com.starrocks.example.udf;

public class UDFAdd {
    public Integer evaluate(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }
}
