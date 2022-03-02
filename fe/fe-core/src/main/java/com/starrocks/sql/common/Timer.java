// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.common;

public class Timer {
    private static final String LINE = "================== ";
    private static long start;
    private static long phase;
    private static long nums = 1;

    public static void start() {
        start = System.currentTimeMillis();
        phase = start;
        System.out.println(LINE + nums + ".watch start: " + start + LINE);
    }

    public static void phase(String str) {
        long end = System.currentTimeMillis();
        nums++;
        System.out.println(
                LINE + nums + ".watch phase " + str + ", cost: " + (end - phase) + "ms, total: " + (end - start) +
                        LINE);
        phase = end;
    }

    public static void end() {
        phase("");
        long end = System.currentTimeMillis();
        System.out.println(LINE + nums + ".watch end, cost: " + (end - start) + "ms " + LINE);
    }
}
