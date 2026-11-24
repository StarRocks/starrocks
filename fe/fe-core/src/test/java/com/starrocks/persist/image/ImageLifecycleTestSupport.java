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

import com.starrocks.persist.ImageFiles;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.Storage;
import com.starrocks.persist.proto.GlobalStatePB;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

public final class ImageLifecycleTestSupport {
    private ImageLifecycleTestSupport() {
    }

    public static Path writeV3(Path root, long id) throws IOException {
        return writeV3(root, id,
                GlobalStatePB.newBuilder().setBatchEndId(123000).build());
    }

    public static Path writeV3(Path root, long id, GlobalStatePB state) throws IOException {
        Path directory = ImageFiles.directory(root.toString(), ImageFormatVersion.v3);
        Files.createDirectories(directory);
        Path file = ImageFiles.image(directory, id);
        try (OutputStream out = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(out, id)) {
            if (state != null) {
                try (SectionWriter section = writer.beginSection(SectionId.GLOBAL_STATE)) {
                    section.writeManager(state);
                }
            }
            writer.finish();
        }
        return file;
    }

    // File-level tests only need the v2 transport checksum, not a full manager image.
    public static Path writeV2(Path root, long id, byte[] bytes) throws IOException {
        Path directory = ImageFiles.directory(root.toString(), ImageFormatVersion.v2);
        Files.createDirectories(directory);
        Path file = ImageFiles.image(directory, id);
        Files.write(file, bytes);
        CRC32 crc = new CRC32();
        crc.update(bytes);
        Files.writeString(directory.resolve(Storage.CHECKSUM + "." + id), Long.toString(crc.getValue()));
        return file;
    }
}
