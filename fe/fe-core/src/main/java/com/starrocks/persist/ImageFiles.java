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

package com.starrocks.persist;

import com.starrocks.persist.image.ImageReaderV3;
import com.starrocks.persist.image.SectionId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;

/** File validation and publication shared by local checkpoints and image downloads. */
public final class ImageFiles {
    private static final Logger LOG = LogManager.getLogger(ImageFiles.class);

    private ImageFiles() {
    }

    public static List<ImageFormatVersion> formats(boolean writeV2) {
        return writeV2 ? List.of(ImageFormatVersion.v2, ImageFormatVersion.v3) : List.of(ImageFormatVersion.v3);
    }

    public static Path directory(String imageRoot, ImageFormatVersion format) {
        return format == ImageFormatVersion.v1 ? Path.of(imageRoot) : Path.of(imageRoot, format.name());
    }

    public static Path image(Path directory, long journalId) {
        return directory.resolve(Storage.IMAGE + "." + journalId);
    }

    public static boolean isComplete(String imageRoot, long journalId, boolean writeV2) {
        for (ImageFormatVersion format : formats(writeV2)) {
            if (!isValid(image(directory(imageRoot, format), journalId), format, journalId)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isValid(Path image, ImageFormatVersion format, long journalId) {
        if (!Files.isRegularFile(image)) {
            return false;
        }
        try {
            validate(image, format, journalId);
            return true;
        } catch (IOException e) {
            LOG.warn("invalid image {}: {}", image, e.getMessage());
            return false;
        }
    }

    public static void validate(Path image, ImageFormatVersion format, long journalId) throws IOException {
        if (!Files.isRegularFile(image)) {
            throw new IOException("image does not exist: " + image);
        }
        if (format == ImageFormatVersion.v3) {
            try (ImageReaderV3 reader = new ImageReaderV3(image)) {
                validateHeader(reader, journalId);
            }
        } else if (format == ImageFormatVersion.v2) {
            Path checksum = image.resolveSibling(Storage.CHECKSUM + "." + journalId);
            validateChecksum(image, Files.readString(checksum));
        }
    }

    public static void validateHeader(ImageReaderV3 reader, long journalId) throws IOException {
        if (reader.header().getJournalId() != journalId) {
            throw new IOException("image journal ID mismatch: expected " + journalId
                    + ", actual " + reader.header().getJournalId());
        }
        if (reader.sections().stream().noneMatch(s -> s.getType() == SectionId.GLOBAL_STATE.getValue())) {
            throw new IOException("image is missing GLOBAL_STATE section");
        }
    }

    public static void validateChecksum(Path image, String checksum) throws IOException {
        long expected;
        try {
            expected = Long.parseLong(checksum.trim());
        } catch (NumberFormatException | NullPointerException e) {
            throw new IOException("missing or invalid image checksum for " + image, e);
        }
        CRC32 crc = new CRC32();
        try (InputStream in = Files.newInputStream(image)) {
            byte[] buffer = new byte[1 << 16];
            int count;
            while ((count = in.read(buffer)) != -1) {
                crc.update(buffer, 0, count);
            }
        }
        if (crc.getValue() != expected) {
            throw new IOException("image checksum mismatch for " + image + ": expected " + expected
                    + ", actual " + crc.getValue());
        }
    }

    public static synchronized void publishImage(Path source, Path directory,
                                                 ImageFormatVersion format, long journalId) throws IOException {
        validate(source, format, journalId);
        Path target = image(directory, journalId);
        // Reuse an already valid image at this journal ID.
        if (isValid(target, format, journalId)) {
            return;
        }
        if (format == ImageFormatVersion.v2) {
            String checksum = Storage.CHECKSUM + "." + journalId;
            Path sourceChecksum = source.resolveSibling(checksum);
            Path targetChecksum = directory.resolve(checksum);
            if (!sourceChecksum.equals(targetChecksum)) {
                publish(sourceChecksum, targetChecksum);
            }
        }
        publish(source, target);
    }

    public static void publish(Path source, Path target) throws IOException {
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        // Persist the directory entry as well as the file contents before acknowledging publication.
        try (FileChannel directory = FileChannel.open(target.getParent(), StandardOpenOption.READ)) {
            directory.force(true);
        }
    }
}
