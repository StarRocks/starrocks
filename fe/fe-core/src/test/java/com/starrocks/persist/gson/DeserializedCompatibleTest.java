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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeserializedCompatibleTest {

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

    static class NotificationSetWrapper {
        @SerializedName("set")
        private final Set<NotificationPayload> notifications;

        NotificationSetWrapper(Set<NotificationPayload> notifications) {
            this.notifications = notifications;
        }

        Set<NotificationPayload> getNotifications() {
            return notifications;
        }
    }

    static class NotificationKeyedMapWrapper {
        @SerializedName("map")
        private final Map<NotificationPayload, String> notifications;

        NotificationKeyedMapWrapper(Map<NotificationPayload, String> notifications) {
            this.notifications = notifications;
        }

        Map<NotificationPayload, String> getNotifications() {
            return notifications;
        }
    }

    static class CoordinateKey {
        @SerializedName("x")
        private final int x;
        @SerializedName("y")
        private final int y;

        CoordinateKey(int x, int y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CoordinateKey that)) {
                return false;
            }
            return this.x == that.x && this.y == that.y;
        }

        @Override
        public int hashCode() {
            return 31 * x + y;
        }
    }

    static class ComplexKeyNotificationMapWrapper {
        @SerializedName("map")
        private final Map<CoordinateKey, NotificationPayload> notifications;

        ComplexKeyNotificationMapWrapper(Map<CoordinateKey, NotificationPayload> notifications) {
            this.notifications = notifications;
        }

        Map<CoordinateKey, NotificationPayload> getNotifications() {
            return notifications;
        }
    }

    enum EnumKey {
        KEY1,
        KEY2,
        KEY3
    }

    static class EnumKeyNotificationMapWrapper {
        @SerializedName("map")
        private final Map<EnumKey, NotificationPayload> notifications;

        EnumKeyNotificationMapWrapper(Map<EnumKey, NotificationPayload> notifications) {
            this.notifications = notifications;
        }

        Map<EnumKey, NotificationPayload> getNotifications() {
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

    static class MultiLayerNotificationWrapper {
        @SerializedName("multi")
        private final List<Map<String, List<NotificationPayload>>> multiLayer;

        MultiLayerNotificationWrapper(List<Map<String, List<NotificationPayload>>> multiLayer) {
            this.multiLayer = multiLayer;
        }

        List<Map<String, List<NotificationPayload>>> getMultiLayer() {
            return multiLayer;
        }
    }

    static class UltraNestedNotificationWrapper {
        @SerializedName("ultra")
        private final Map<String, List<Map<String, NotificationPayload>>> ultraNested;

        UltraNestedNotificationWrapper(Map<String, List<Map<String, NotificationPayload>>> ultraNested) {
            this.ultraNested = ultraNested;
        }

        Map<String, List<Map<String, NotificationPayload>>> getUltraNested() {
            return ultraNested;
        }
    }

    @Test
    public void mapSkipsLegacySubtypeButKeepsNullEntry() {
        Gson writer = gsonWithAdapters(false, runtimeFactoryWithLegacy());

        Map<String, NotificationPayload> original = Maps.newLinkedHashMap();
        original.put("email", new EmailNotificationPayload());
        original.put("null", null);
        original.put("legacy", new LegacyNotificationPayload());

        String json = writer.toJson(new NotificationMapWrapper(original));

        Gson reader = gsonWithSkipping(false, runtimeFactoryWithoutLegacy());
        Map<String, NotificationPayload> result = reader.fromJson(json, NotificationMapWrapper.class)
                .getNotifications();

        Assertions.assertEquals(2, result.size());
        Assertions.assertInstanceOf(EmailNotificationPayload.class, result.get("email"));
        Assertions.assertTrue(result.containsKey("null"));
        Assertions.assertNull(result.get("null"));
        Assertions.assertFalse(result.containsKey("legacy"));
    }

    @Test
    public void listSkipsLegacySubtypeButKeepsNullEntry() {
        Gson writer = gsonWithAdapters(false, runtimeFactoryWithLegacy());

        List<NotificationPayload> original = new ArrayList<>();
        original.add(new EmailNotificationPayload());
        original.add(null);
        original.add(new LegacyNotificationPayload());

        String json = writer.toJson(new NotificationListWrapper(original));

        Gson reader = gsonWithSkipping(false, runtimeFactoryWithoutLegacy());
        List<NotificationPayload> result = reader.fromJson(json, NotificationListWrapper.class)
                .getNotifications();

        Assertions.assertEquals(2, result.size());
        Assertions.assertInstanceOf(EmailNotificationPayload.class, result.get(0));
        Assertions.assertNull(result.get(1));
    }

    @Test
    public void mapKeySkipsLegacySubtype() {
        Gson writer = gsonWithAdapters(true, runtimeFactoryWithLegacy());

        Map<NotificationPayload, String> original = new LinkedHashMap<>();
        NotificationPayload emailKey = new EmailNotificationPayload();
        NotificationPayload legacyKey = new LegacyNotificationPayload();
        original.put(emailKey, "ok");
        original.put(legacyKey, "legacy");

        String json = writer.toJson(new NotificationKeyedMapWrapper(original));

        Gson reader = gsonWithSkipping(true, runtimeFactoryWithoutLegacy());
        Map<NotificationPayload, String> result = reader
                .fromJson(json, NotificationKeyedMapWrapper.class)
                .getNotifications();

        Assertions.assertEquals(1, result.size());
        NotificationPayload onlyKey = result.keySet().iterator().next();
        Assertions.assertInstanceOf(EmailNotificationPayload.class, onlyKey);
        Assertions.assertEquals("ok", result.get(onlyKey));
    }

    @Test
    public void complexKeyMapSkipsLegacySubtype() {
        Gson writer = gsonWithAdapters(true, runtimeFactoryWithLegacy());

        Map<CoordinateKey, NotificationPayload> original = new LinkedHashMap<>();
        CoordinateKey emailKey = new CoordinateKey(1, 1);
        CoordinateKey nullKey = new CoordinateKey(2, 2);
        CoordinateKey legacyKey = new CoordinateKey(3, 3);
        original.put(emailKey, new EmailNotificationPayload());
        original.put(nullKey, null);
        original.put(legacyKey, new LegacyNotificationPayload());

        String json = writer.toJson(new ComplexKeyNotificationMapWrapper(original));

        Gson reader = gsonWithSkipping(true, runtimeFactoryWithoutLegacy());
        Map<CoordinateKey, NotificationPayload> result = reader
                .fromJson(json, ComplexKeyNotificationMapWrapper.class)
                .getNotifications();

        Assertions.assertEquals(2, result.size());
        Assertions.assertInstanceOf(EmailNotificationPayload.class, result.get(emailKey));
        Assertions.assertTrue(result.containsKey(nullKey));
        Assertions.assertNull(result.get(nullKey));
        Assertions.assertFalse(result.containsKey(legacyKey));
    }

    @Test
    public void enumKeyMapSkipsLegacySubtype() {
        Gson writer = gsonWithAdapters(true, runtimeFactoryWithLegacy());

        Map<EnumKey, NotificationPayload> original = new LinkedHashMap<>();
        original.put(EnumKey.KEY1, new EmailNotificationPayload());
        original.put(EnumKey.KEY2, null);
        original.put(EnumKey.KEY3, new LegacyNotificationPayload());

        String json = writer.toJson(new EnumKeyNotificationMapWrapper(original));

        Gson reader = gsonWithSkipping(true, runtimeFactoryWithoutLegacy());
        Map<EnumKey, NotificationPayload> result = reader
                .fromJson(json, EnumKeyNotificationMapWrapper.class)
                .getNotifications();

        Assertions.assertEquals(2, result.size());
        Assertions.assertInstanceOf(EmailNotificationPayload.class, result.get(EnumKey.KEY1));
        Assertions.assertTrue(result.containsKey(EnumKey.KEY2));
        Assertions.assertNull(result.get(EnumKey.KEY2));
        Assertions.assertFalse(result.containsKey(EnumKey.KEY3));
    }

    @Test
    public void nestedStructuresSkipLegacySubtype() {
        Gson writer = gsonWithAdapters(false, runtimeFactoryWithLegacy());

        Map<String, List<NotificationPayload>> nested = Maps.newLinkedHashMap();
        List<NotificationPayload> inbox = new ArrayList<>();
        inbox.add(new EmailNotificationPayload());
        inbox.add(null);
        inbox.add(new LegacyNotificationPayload());
        nested.put("inbox", inbox);

        List<NotificationPayload> archive = new ArrayList<>();
        archive.add(null);
        nested.put("archive", archive);

        String json = writer.toJson(new NestedNotificationWrapper(nested));

        Gson reader = gsonWithSkipping(false, runtimeFactoryWithoutLegacy());
        Map<String, List<NotificationPayload>> result = reader
                .fromJson(json, NestedNotificationWrapper.class)
                .getNested();

        Assertions.assertEquals(2, result.get("inbox").size());
        Assertions.assertInstanceOf(EmailNotificationPayload.class, result.get("inbox").get(0));
        Assertions.assertNull(result.get("inbox").get(1));
        Assertions.assertEquals(1, result.get("archive").size());
        Assertions.assertNull(result.get("archive").get(0));
    }

    @Test
    public void setSkipsLegacySubtype() {
        Gson writer = gsonWithAdapters(false, runtimeFactoryWithLegacy());

        LinkedHashSet<NotificationPayload> original = Sets.newLinkedHashSet();
        original.add(new EmailNotificationPayload());
        original.add(null);
        original.add(new LegacyNotificationPayload());

        String json = writer.toJson(new NotificationSetWrapper(original));

        Gson reader = gsonWithSkipping(false, runtimeFactoryWithoutLegacy());
        Set<NotificationPayload> result = reader.fromJson(json, NotificationSetWrapper.class)
                .getNotifications();

        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains(null));
        Assertions.assertTrue(result.stream().anyMatch(EmailNotificationPayload.class::isInstance));
        Assertions.assertFalse(result.stream().anyMatch(LegacyNotificationPayload.class::isInstance));
    }

    @Test
    public void multiLayerNestedStructureSkipsLegacySubtype() {
        Gson writer = gsonWithAdapters(false, runtimeFactoryWithLegacy());

        List<Map<String, List<NotificationPayload>>> layers = new ArrayList<>();

        Map<String, List<NotificationPayload>> firstLayer = new LinkedHashMap<>();
        List<NotificationPayload> firstList = new ArrayList<>();
        firstList.add(new EmailNotificationPayload());
        firstList.add(null);
        firstList.add(new LegacyNotificationPayload());
        firstLayer.put("first", firstList);
        layers.add(firstLayer);

        Map<String, List<NotificationPayload>> secondLayer = new LinkedHashMap<>();
        List<NotificationPayload> secondList = new ArrayList<>();
        secondList.add(null);
        secondLayer.put("second", secondList);
        layers.add(secondLayer);

        String json = writer.toJson(new MultiLayerNotificationWrapper(layers));

        Gson reader = gsonWithSkipping(false, runtimeFactoryWithoutLegacy());
        List<Map<String, List<NotificationPayload>>> result = reader
                .fromJson(json, MultiLayerNotificationWrapper.class)
                .getMultiLayer();

        Assertions.assertEquals(2, result.size());

        List<NotificationPayload> firstResult = result.get(0).get("first");
        Assertions.assertEquals(2, firstResult.size());
        Assertions.assertInstanceOf(EmailNotificationPayload.class, firstResult.get(0));
        Assertions.assertNull(firstResult.get(1));

        List<NotificationPayload> secondResult = result.get(1).get("second");
        Assertions.assertEquals(1, secondResult.size());
        Assertions.assertNull(secondResult.get(0));
    }

    @Test
    public void ultraNestedStructureSkipsLegacySubtype() {
        Gson writer = gsonWithAdapters(false, runtimeFactoryWithLegacy());

        Map<String, List<Map<String, NotificationPayload>>> ultra = new LinkedHashMap<>();

        List<Map<String, NotificationPayload>> layerOne = new ArrayList<>();
        Map<String, NotificationPayload> firstMap = new LinkedHashMap<>();
        firstMap.put("email", new EmailNotificationPayload());
        firstMap.put("legacy", new LegacyNotificationPayload());
        layerOne.add(firstMap);

        Map<String, NotificationPayload> secondMap = new LinkedHashMap<>();
        secondMap.put("null", null);
        layerOne.add(secondMap);
        ultra.put("layer-one", layerOne);

        List<Map<String, NotificationPayload>> layerTwo = new ArrayList<>();
        Map<String, NotificationPayload> thirdMap = new LinkedHashMap<>();
        thirdMap.put("legacy-two", new LegacyNotificationPayload());
        thirdMap.put("email-two", new EmailNotificationPayload());
        layerTwo.add(thirdMap);
        ultra.put("layer-two", layerTwo);

        String json = writer.toJson(new UltraNestedNotificationWrapper(ultra));

        Gson reader = gsonWithSkipping(false, runtimeFactoryWithoutLegacy());
        Map<String, List<Map<String, NotificationPayload>>> result = reader
                .fromJson(json, UltraNestedNotificationWrapper.class)
                .getUltraNested();

        Assertions.assertEquals(2, result.get("layer-one").size());
        Map<String, NotificationPayload> firstResult = result.get("layer-one").get(0);
        Assertions.assertEquals(1, firstResult.size());
        Assertions.assertInstanceOf(EmailNotificationPayload.class, firstResult.get("email"));
        Assertions.assertFalse(firstResult.containsKey("legacy"));

        Map<String, NotificationPayload> secondResult = result.get("layer-one").get(1);
        Assertions.assertEquals(1, secondResult.size());
        Assertions.assertTrue(secondResult.containsKey("null"));
        Assertions.assertNull(secondResult.get("null"));

        Map<String, NotificationPayload> thirdResult = result.get("layer-two").get(0);
        Assertions.assertEquals(1, thirdResult.size());
        Assertions.assertInstanceOf(EmailNotificationPayload.class, thirdResult.get("email-two"));
        Assertions.assertFalse(thirdResult.containsKey("legacy-two"));
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

    private Gson gsonWithAdapters(boolean enableComplexMapKey,
                                  RuntimeTypeAdapterFactory<NotificationPayload> runtimeFactory) {
        GsonBuilder builder = new GsonBuilder()
                .addSerializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                .addDeserializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                // In the current GsonUtils implementation, serializeNulls is disabled by default,
                // and in practice values are not expected to be null.
                // However, enable this option to be safe for future code changes.
                .serializeNulls()
                .registerTypeAdapterFactory(runtimeFactory);
        if (enableComplexMapKey) {
            builder.enableComplexMapKeySerialization();
        }
        return builder.create();
    }

    private Gson gsonWithSkipping(boolean enableComplexMapKey,
                                  RuntimeTypeAdapterFactory<NotificationPayload> runtimeFactory) {
        GsonBuilder builder = new GsonBuilder()
                .addSerializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                .addDeserializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                // In the current GsonUtils implementation, serializeNulls is disabled by default,
                // and in practice values are not expected to be null.
                // However, enable this option to be safe for future code changes.
                .serializeNulls()
                .registerTypeAdapterFactory(new SubtypeSkippingTypeAdapterFactory())
                .registerTypeAdapterFactory(runtimeFactory);
        if (enableComplexMapKey) {
            builder.enableComplexMapKeySerialization();
        }
        return builder.create();
    }
}
