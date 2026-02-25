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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Performance test suite for SubtypeSkippingTypeAdapterFactory
 * This test compares deserialization performance with and without SubtypeSkippingTypeAdapterFactory
 * <p>
 * Note: These tests are disabled by default as they are performance benchmarks, not functional tests.
 * To run these tests explicitly, use:
 * ./gradlew :fe-core:test --tests "com.starrocks.persist.gson.SubtypeSkippingPerformanceTest"
 */
@Disabled("Performance tests - run manually when needed")
public class SubtypeSkippingPerformanceTest {

    static class NotificationPayload {
        @SerializedName("base")
        private String baseValue = "base";
    }

    static class EmailNotificationPayload extends NotificationPayload {
        @SerializedName("email")
        private String emailValue = "email";
    }

    static class LegacyNotificationPayload extends NotificationPayload {
        @SerializedName("legacy")
        private String legacyValue = "legacy";
    }

    static class NotificationMapWrapper {
        @SerializedName("map")
        private final Map<String, NotificationPayload> notifications;

        NotificationMapWrapper(Map<String, NotificationPayload> notifications) {
            this.notifications = notifications;
        }

        Map<String, NotificationPayload> getNotifications() {
            return notifications;
        }
    }

    enum EnumKey {
        KEY1,
        KEY2,
        KEY3
    }

    static class EnumNotificationMapWrapper {
        @SerializedName("map")
        private final Map<EnumKey, NotificationPayload> notifications;

        EnumNotificationMapWrapper(Map<EnumKey, NotificationPayload> notifications) {
            this.notifications = notifications;
        }

        Map<EnumKey, NotificationPayload> getNotifications() {
            return notifications;
        }
    }

    static class NotificationListWrapper {
        @SerializedName("list")
        private final List<NotificationPayload> notifications;

        NotificationListWrapper(List<NotificationPayload> notifications) {
            this.notifications = notifications;
        }

        List<NotificationPayload> getNotifications() {
            return notifications;
        }
    }

    static class NestedNotificationWrapper {
        @SerializedName("nested")
        private final Map<String, List<NotificationPayload>> nested;

        NestedNotificationWrapper(Map<String, List<NotificationPayload>> nested) {
            this.nested = nested;
        }

        Map<String, List<NotificationPayload>> getNested() {
            return nested;
        }
    }

    /**
     * Performance test: Compare simple map deserialization performance
     * Test point: Measure the performance impact of using SubtypeSkippingTypeAdapterFactory
     * for filtering legacy subtypes during deserialization
     */
    @Test
    public void testMapPerformanceImpact() {
        // Create test data with mixed legacy and current subtypes
        Map<String, NotificationPayload> testData = createLargeTestData(10000);
        NotificationMapWrapper wrapper = new NotificationMapWrapper(testData);

        // Serialize the data
        Gson writer = gsonWithAdapters(runtimeFactoryWithLegacy());
        String json = writer.toJson(wrapper);

        // Test deserialization without SubtypeSkippingTypeAdapterFactory
        Gson readerWithoutSkipping = gsonWithAdapters(runtimeFactoryWithLegacy());
        long timeWithoutSkipping = measureDeserializationTime(readerWithoutSkipping, json, 50);

        // Test deserialization with SubtypeSkippingTypeAdapterFactory
        Gson readerWithSkipping = gsonWithSkipping(runtimeFactoryWithLegacy());
        long timeWithSkipping = measureDeserializationTime(readerWithSkipping, json, 50);

        // Log performance results
        System.out.println("=== Map Performance Test Results ===");
        System.out.println("Test data size: 10,000 entries");
        System.out.println("Iterations: 50");
        System.out.println("Without SubtypeSkippingTypeAdapterFactory: " + timeWithoutSkipping + " ms");
        System.out.println("With SubtypeSkippingTypeAdapterFactory: " + timeWithSkipping + " ms");
        long timeDiff = timeWithSkipping - timeWithoutSkipping;
        double percentageChange = (double) timeDiff / timeWithoutSkipping * 100;
        double speedRatio = (double) timeWithSkipping / timeWithoutSkipping;
        System.out.println("Time difference: " + (timeDiff >= 0 ? "+" : "") + timeDiff + " ms");
        System.out.println("Percentage change: " + String.format("%+.2f", percentageChange) + "%");
        System.out.println("Speed ratio: " + String.format("%.2f", speedRatio) + "x");
        System.out.println();
    }

    /**
     * Performance test: Compare list deserialization performance
     * Test point: Measure the performance impact of using SubtypeSkippingTypeAdapterFactory
     * for filtering legacy subtypes in List collections
     */
    @Test
    public void testListPerformanceImpact() {
        // Create test data with mixed legacy and current subtypes
        List<NotificationPayload> testData = createLargeListData(50000);
        NotificationListWrapper wrapper = new NotificationListWrapper(testData);

        // Serialize the data
        Gson writer = gsonWithAdapters(runtimeFactoryWithLegacy());
        String json = writer.toJson(wrapper);

        // Test deserialization without SubtypeSkippingTypeAdapterFactory
        Gson readerWithoutSkipping = gsonWithAdapters(runtimeFactoryWithLegacy());
        long timeWithoutSkipping = measureDeserializationTimeForList(readerWithoutSkipping, json, 50);

        // Test deserialization with SubtypeSkippingTypeAdapterFactory
        Gson readerWithSkipping = gsonWithSkipping(runtimeFactoryWithLegacy());
        long timeWithSkipping = measureDeserializationTimeForList(readerWithSkipping, json, 50);

        // Log performance results
        System.out.println("=== List Performance Test Results ===");
        System.out.println("Test data size: 50,000 list entries");
        System.out.println("Iterations: 50");
        System.out.println("Without SubtypeSkippingTypeAdapterFactory: " + timeWithoutSkipping + " ms");
        System.out.println("With SubtypeSkippingTypeAdapterFactory: " + timeWithSkipping + " ms");
        long timeDiff = timeWithSkipping - timeWithoutSkipping;
        double percentageChange = (double) timeDiff / timeWithoutSkipping * 100;
        double speedRatio = (double) timeWithSkipping / timeWithoutSkipping;
        System.out.println("Time difference: " + (timeDiff >= 0 ? "+" : "") + timeDiff + " ms");
        System.out.println("Percentage change: " + String.format("%+.2f", percentageChange) + "%");
        System.out.println("Speed ratio: " + String.format("%.2f", speedRatio) + "x");
        System.out.println();
    }

    /**
     * Performance test: Compare nested structure deserialization performance
     * Test point: Measure performance impact on complex nested structures with mixed subtypes
     */
    @Test
    public void testNestedStructurePerformanceImpact() {
        // Create complex nested test data
        Map<String, List<NotificationPayload>> nestedData = createNestedTestData(10000, 10);
        NestedNotificationWrapper wrapper = new NestedNotificationWrapper(nestedData);

        // Serialize the data
        Gson writer = gsonWithAdapters(runtimeFactoryWithLegacy());
        String json = writer.toJson(wrapper);

        // Test deserialization without SubtypeSkippingTypeAdapterFactory
        Gson readerWithoutSkipping = gsonWithAdapters(runtimeFactoryWithoutLegacy());
        long timeWithoutSkipping = measureDeserializationTime(readerWithoutSkipping, json, 50);

        // Test deserialization with SubtypeSkippingTypeAdapterFactory
        Gson readerWithSkipping = gsonWithSkipping(runtimeFactoryWithoutLegacy());
        long timeWithSkipping = measureDeserializationTime(readerWithSkipping, json, 50);

        // Log performance results
        System.out.println("=== Nested Structure Performance Test Results ===");
        System.out.println("Test data size: 10,000 outer entries x 10 inner entries");
        System.out.println("Iterations: 50");
        System.out.println("Without SubtypeSkippingTypeAdapterFactory: " + timeWithoutSkipping + " ms");
        System.out.println("With SubtypeSkippingTypeAdapterFactory: " + timeWithSkipping + " ms");
        long timeDiff = timeWithSkipping - timeWithoutSkipping;
        double percentageChange = (double) timeDiff / timeWithoutSkipping * 100;
        double speedRatio = (double) timeWithSkipping / timeWithoutSkipping;
        System.out.println("Time difference: " + (timeDiff >= 0 ? "+" : "") + timeDiff + " ms");
        System.out.println("Percentage change: " + String.format("%+.2f", percentageChange) + "%");
        System.out.println("Speed ratio: " + String.format("%.2f", speedRatio) + "x");
        System.out.println();
    }

    /**
     * Performance test: Compare complex map key deserialization performance
     * Test point: Measure performance impact on maps with complex keys containing mixed subtypes
     */
    @Test
    public void testComplexMapKeyPerformanceImpact() {
        // Create test data with enum keys
        Map<EnumKey, NotificationPayload> testData = createEnumKeyTestData(10000);
        EnumNotificationMapWrapper wrapper = new EnumNotificationMapWrapper(testData);

        // Serialize the data
        Gson writer = gsonWithAdapters(runtimeFactoryWithLegacy());
        String json = writer.toJson(wrapper);

        // Test deserialization without SubtypeSkippingTypeAdapterFactory
        Gson readerWithoutSkipping = gsonWithAdapters(runtimeFactoryWithLegacy());
        long timeWithoutSkipping = measureDeserializationTime(readerWithoutSkipping, json, 50);

        // Test deserialization with SubtypeSkippingTypeAdapterFactory
        Gson readerWithSkipping = gsonWithSkipping(runtimeFactoryWithLegacy());
        long timeWithSkipping = measureDeserializationTime(readerWithSkipping, json, 50);

        // Log performance results
        System.out.println("=== Complex Map Key Performance Test Results ===");
        System.out.println("Test data size: 10,000 enum key entries");
        System.out.println("Iterations: 50");
        System.out.println("Without SubtypeSkippingTypeAdapterFactory: " + timeWithoutSkipping + " ms");
        System.out.println("With SubtypeSkippingTypeAdapterFactory: " + timeWithSkipping + " ms");
        long timeDiff = timeWithSkipping - timeWithoutSkipping;
        double percentageChange = (double) timeDiff / timeWithoutSkipping * 100;
        double speedRatio = (double) timeWithSkipping / timeWithoutSkipping;
        System.out.println("Time difference: " + (timeDiff >= 0 ? "+" : "") + timeDiff + " ms");
        System.out.println("Percentage change: " + String.format("%+.2f", percentageChange) + "%");
        System.out.println("Speed ratio: " + String.format("%.2f", speedRatio) + "x");
        System.out.println();
    }

    /**
     * Create large test data with mixed subtypes for performance testing
     */
    private Map<String, NotificationPayload> createLargeTestData(int size) {
        Map<String, NotificationPayload> data = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            String key = "item_" + i;
            NotificationPayload payload;
            switch (i % 4) {
                case 0:
                    payload = new EmailNotificationPayload();
                    break;
                case 1:
                    payload = new LegacyNotificationPayload();
                    break;
                case 2:
                    payload = null;
                    break;
                default:
                    payload = new EmailNotificationPayload();
                    break;
            }
            data.put(key, payload);
        }
        return data;
    }

    /**
     * Create large list test data with mixed subtypes for performance testing
     */
    private List<NotificationPayload> createLargeListData(int size) {
        List<NotificationPayload> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            NotificationPayload payload;
            switch (i % 4) {
                case 0:
                    payload = new EmailNotificationPayload();
                    break;
                case 1:
                    payload = new LegacyNotificationPayload();
                    break;
                case 2:
                    payload = null;
                    break;
                default:
                    payload = new EmailNotificationPayload();
                    break;
            }
            data.add(payload);
        }
        return data;
    }

    /**
     * Create nested test data for performance testing
     */
    private Map<String, List<NotificationPayload>> createNestedTestData(int outerSize, int innerSize) {
        Map<String, List<NotificationPayload>> data = new LinkedHashMap<>();
        for (int i = 0; i < outerSize; i++) {
            String key = "list_" + i;
            List<NotificationPayload> list = new ArrayList<>();
            for (int j = 0; j < innerSize; j++) {
                NotificationPayload payload;
                switch (j % 4) {
                    case 0:
                        payload = new EmailNotificationPayload();
                        break;
                    case 1:
                        payload = new LegacyNotificationPayload();
                        break;
                    case 2:
                        payload = null;
                        break;
                    default:
                        payload = new EmailNotificationPayload();
                        break;
                }
                list.add(payload);
            }
            data.put(key, list);
        }
        return data;
    }

    /**
     * Create enum key test data for performance testing
     */
    private Map<EnumKey, NotificationPayload> createEnumKeyTestData(int size) {
        Map<EnumKey, NotificationPayload> data = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            EnumKey key = EnumKey.values()[i % EnumKey.values().length];
            NotificationPayload payload;
            switch (i % 4) {
                case 0:
                    payload = new EmailNotificationPayload();
                    break;
                case 1:
                    payload = new LegacyNotificationPayload();
                    break;
                case 2:
                    payload = null;
                    break;
                default:
                    payload = new EmailNotificationPayload();
                    break;
            }
            data.put(key, payload);
        }
        return data;
    }

    /**
     * Measure deserialization time for the given Gson instance and JSON string
     */
    private long measureDeserializationTime(Gson gson, String json, int iterations) {
        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            try {
                gson.fromJson(json, NotificationMapWrapper.class);
            } catch (Exception e) {
                // Expected for some test cases - ignore exceptions during performance measurement
            }
        }

        long endTime = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
    }

    /**
     * Measure deserialization time for List wrapper
     */
    private long measureDeserializationTimeForList(Gson gson, String json, int iterations) {
        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            try {
                gson.fromJson(json, NotificationListWrapper.class);
            } catch (Exception e) {
                // Expected for some test cases - ignore exceptions during performance measurement
            }
        }

        long endTime = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
    }

    private RuntimeTypeAdapterFactory<NotificationPayload> runtimeFactoryWithLegacy() {
        return RuntimeTypeAdapterFactory.of(NotificationPayload.class, "clazz")
                .registerSubtype(EmailNotificationPayload.class, "email")
                .registerSubtype(LegacyNotificationPayload.class, "legacy");
    }

    private RuntimeTypeAdapterFactory<NotificationPayload> runtimeFactoryWithoutLegacy() {
        return RuntimeTypeAdapterFactory.of(NotificationPayload.class, "clazz")
                .registerSubtype(EmailNotificationPayload.class, "email");
    }

    private Gson gsonWithAdapters(RuntimeTypeAdapterFactory<NotificationPayload> runtimeFactory) {
        return new GsonBuilder()
                .addSerializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                .addDeserializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                // In the current GsonUtils implementation, serializeNulls is disabled by default,
                // and in practice values are not expected to be null.
                // However, enable this option to be safe for future code changes.
                .serializeNulls()
                .enableComplexMapKeySerialization()
                .registerTypeAdapterFactory(runtimeFactory)
                .create();
    }

    private Gson gsonWithSkipping(RuntimeTypeAdapterFactory<NotificationPayload> runtimeFactory) {
        return new GsonBuilder()
                .addSerializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                .addDeserializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                // In the current GsonUtils implementation, serializeNulls is disabled by default,
                // and in practice values are not expected to be null.
                // However, enable this option to be safe for future code changes.
                .serializeNulls()
                .enableComplexMapKeySerialization()
                .registerTypeAdapterFactory(new SubtypeSkippingTypeAdapterFactory())
                .registerTypeAdapterFactory(runtimeFactory)
                .create();
    }
}
