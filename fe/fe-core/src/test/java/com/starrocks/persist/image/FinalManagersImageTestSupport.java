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

package com.starrocks.persist.image;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import com.google.protobuf.Message;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.utframe.UtFrameUtils;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class FinalManagersImageTestSupport {
    private FinalManagersImageTestSupport() {
    }

    @FunctionalInterface
    interface SaveV2 {
        void save(ImageWriter writer) throws Exception;
    }

    @FunctionalInterface
    interface LoadV2 {
        void load(SRMetaBlockReader reader) throws Exception;
    }

    static void v2(SaveV2 save, LoadV2 load) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        save.save(image.getImageWriter());
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        load.load(reader);
        reader.close();
    }

    static Path entries(Path dir, SectionId id, List<? extends Message> entries)
            throws Exception {
        return LoadImageTestSupport.save(dir, writer -> {
            try (SectionWriter section = writer.beginSection(id)) {
                for (Message entry : entries) {
                    section.writeEntry(entry);
                }
            }
        });
    }

    static <T> T gson(T value, Class<T> type) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(value), type);
    }

    static void assertAllFieldsPresent(Object value) {
        JsonObject json = GsonUtils.GSON.toJsonTree(value).getAsJsonObject();
        for (Class<?> type = value.getClass(); type != null; type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                SerializedName annotation = field.getAnnotation(SerializedName.class);
                if (annotation != null) {
                    assertTrue(json.has(annotation.value()),
                            "fixture does not exercise " + type.getName() + "." + field.getName());
                }
            }
        }
    }
}
