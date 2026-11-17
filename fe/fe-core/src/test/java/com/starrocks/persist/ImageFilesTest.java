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

import com.starrocks.persist.image.ImageLifecycleTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV2;
import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV3;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImageFilesTest {
    @TempDir
    Path root;

    @Test
    public void selectsJournalBeforeFormatAndIgnoresTemporaryFiles() throws Exception {
        assertFalse(new ImageLoader(root.toString()).getImageFile().exists());
        writeV2(root, 10, new byte[] {1, 2});
        writeV3(root, 9);
        assertEquals(ImageFormatVersion.v2, new ImageLoader(root.toString()).getImageFormatVersion());
        Path v3 = writeV3(root, 10);
        assertEquals(v3.toFile(), new ImageLoader(root.toString()).getImageFile());
        for (ImageFormatVersion format : ImageFiles.formats(true)) {
            Path directory = root.resolve(format.name());
            Files.createFile(directory.resolve("image.999.part"));
            Files.createFile(directory.resolve(Storage.IMAGE_NEW));
            Files.writeString(directory.resolve("checksum.999"), "1234");
        }
        Files.createDirectory(root.resolve("v3/image.1000"));
        Files.createFile(root.resolve("v3/image-unrelated.2000"));
        assertEquals(10, new ImageLoader(root.toString()).getImageJournalId());
        writeV2(root, 11, new byte[] {3});
        assertEquals(ImageFormatVersion.v2, new ImageLoader(root.toString()).getImageFormatVersion());
    }

    @Test
    public void missingImageZeroDoesNotCountAsComplete() throws Exception {
        assertFalse(ImageFiles.isComplete(root.toString(), 0, false));
        writeV3(root, 0);
        assertTrue(ImageFiles.isComplete(root.toString(), 0, false));
        assertFalse(ImageFiles.isComplete(root.toString(), 0, true));
    }

    @Test
    public void completenessDependsOnTheRequestedFormats() throws Exception {
        Path v3 = writeV3(root, 12);
        assertTrue(ImageFiles.isComplete(root.toString(), 12, false));
        assertFalse(ImageFiles.isComplete(root.toString(), 12, true));
        Path v2 = writeV2(root, 12, new byte[] {4, 5});
        assertTrue(ImageFiles.isComplete(root.toString(), 12, true));
        Files.write(v2, new byte[] {9});
        assertFalse(ImageFiles.isComplete(root.toString(), 12, true));
        assertTrue(ImageFiles.isComplete(root.toString(), 12, false));
        Files.write(v3, new byte[] {0});
        assertFalse(ImageFiles.isComplete(root.toString(), 12, false));
        assertEquals(v3.toFile(), new ImageLoader(root.toString()).getImageFile());
    }

    @Test
    public void rejectsWrongJournalAndMissingGlobalState() throws Exception {
        Path image = writeV3(root, 13);
        assertThrows(IOException.class, () -> ImageFiles.validate(image, ImageFormatVersion.v3, 14));
        Path incomplete = ImageLifecycleTestSupport.writeV3(root, 14, null);
        assertThrows(IOException.class, () -> ImageFiles.validate(incomplete, ImageFormatVersion.v3, 14));
    }

    @Test
    public void failedPublicationPreservesPreviousImage() throws Exception {
        Path previous = writeV3(root, 20);
        byte[] original = Files.readAllBytes(previous);
        Path invalid = root.resolve("bad-image");
        Files.write(invalid, new byte[] {1, 2, 3});
        assertThrows(IOException.class,
                () -> ImageFiles.publishImage(invalid, previous.getParent(), ImageFormatVersion.v3, 20));
        assertArrayEquals(original, Files.readAllBytes(previous));
        assertTrue(ImageFiles.isComplete(root.toString(), 20, false));
    }

    @Test
    public void duplicatePublicationKeepsImageAndChecksumTogether() throws Exception {
        byte[] original = {1, 2, 3};
        Path target = writeV2(root, 30, original);
        Path replacement = writeV2(root.resolve("staged"), 30, new byte[] {8, 9});
        ImageFiles.publishImage(replacement, target.getParent(), ImageFormatVersion.v2, 30);
        assertArrayEquals(original, Files.readAllBytes(target));
        assertTrue(ImageFiles.isValid(target, ImageFormatVersion.v2, 30));
    }

    @Test
    public void corruptDestinationCanBeReplacedAndVerified() throws Exception {
        Path target = writeV2(root, 31, new byte[] {1});
        Files.write(target, new byte[] {9});
        Path replacement = writeV2(root.resolve("staged"), 31, new byte[] {2, 3});
        ImageFiles.publishImage(replacement, target.getParent(), ImageFormatVersion.v2, 31);
        assertArrayEquals(new byte[] {2, 3}, Files.readAllBytes(target));
        assertTrue(ImageFiles.isValid(target, ImageFormatVersion.v2, 31));
    }

    @Test
    public void cleanupRetainsLatestImageOfEachFormat() throws Exception {
        writeV2(root, 1, new byte[] {1});
        writeV2(root, 2, new byte[] {2});
        writeV3(root, 2);
        writeV3(root, 3);
        new MetaCleaner(root.resolve("v2").toString()).clean();
        new MetaCleaner(root.resolve("v3").toString()).clean();
        assertTrue(Files.isRegularFile(root.resolve("v2/image.2")));
        assertTrue(Files.isRegularFile(root.resolve("v2/checksum.2")));
        assertTrue(Files.isRegularFile(root.resolve("v3/image.3")));
        assertFalse(Files.exists(root.resolve("v2/image.1")));
        assertFalse(Files.exists(root.resolve("v3/image.2")));
    }
    @Test
    public void publishesV2PartFileAlongsideItsChecksum() throws Exception {
        byte[] content = {1, 2, 3};
        Path target = writeV2(root, 40, content);
        Path part = target.resolveSibling("image.40.part");
        Files.move(target, part);
        assertEquals(0, new ImageLoader(root.toString()).getImageJournalId());
        ImageFiles.publishImage(part, target.getParent(), ImageFormatVersion.v2, 40);
        assertArrayEquals(content, Files.readAllBytes(target));
        assertFalse(Files.exists(part));
        assertTrue(ImageFiles.isValid(target, ImageFormatVersion.v2, 40));
        assertEquals(40, new ImageLoader(root.toString()).getImageJournalId());
    }

}
