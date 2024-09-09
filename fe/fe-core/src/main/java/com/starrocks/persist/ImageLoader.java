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

import com.google.gson.stream.JsonReader;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV1;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class ImageLoader {
    private static final Logger LOG = LogManager.getLogger(ImageLoader.class);

    private final String imageDir;
    private final ImageFormatVersion imageFormatVersion;
    private final File imageFile;
    private final long imageJournalId;
    private JsonReader jsonReader;
    private CheckedInputStream checkedInputStream;
    private BufferedInputStream bufferedInputStream;

    public ImageLoader(String imageDir) throws IOException {
        this.imageDir = imageDir;
        Storage storageV1 = new Storage(imageDir);
        Storage storageV2 = new Storage(imageDir + "/v2");
        if (storageV1.getImageJournalId() > storageV2.getImageJournalId()) {
            imageFile = storageV1.getCurrentImageFile();
            imageFormatVersion = ImageFormatVersion.v1;
            imageJournalId = storageV1.getImageJournalId();
        } else {
            imageFile = storageV2.getCurrentImageFile();
            imageFormatVersion = ImageFormatVersion.v2;
            imageJournalId = storageV2.getImageJournalId();
        }
    }

    public File getImageFile() {
        return imageFile;
    }

    public long getImageJournalId() {
        return imageJournalId;
    }

    public ImageFormatVersion getImageFormatVersion() {
        return imageFormatVersion;
    }

    public void setInputStream(InputStream inputStream) {
        this.bufferedInputStream = new BufferedInputStream(inputStream);
        this.checkedInputStream = new CheckedInputStream(inputStream, new CRC32());
        if (imageFormatVersion == ImageFormatVersion.v2) {
            this.jsonReader = new JsonReader(new InputStreamReader(this.checkedInputStream, StandardCharsets.UTF_8));
        }
    }

    public CheckedInputStream getCheckedInputStream() {
        return checkedInputStream;
    }

    public SRMetaBlockReader getBlockReader() throws IOException {
        if (imageFormatVersion == ImageFormatVersion.v1) {
            return new SRMetaBlockReaderV1(bufferedInputStream);
        } else {
            return new SRMetaBlockReaderV2(jsonReader);
        }
    }

    public void readTheRemainingBytes() {
        if (imageFormatVersion == ImageFormatVersion.v2) {
            byte[] bytes = new byte[8192];
            try {
                while (checkedInputStream.read(bytes) != -1) {
                }
            } catch (IOException e) {
                LOG.warn("read the remaining bytes failed", e);
            }
        }
    }

    public void checkCheckSum() throws IOException {
        if (imageFormatVersion == ImageFormatVersion.v2) {
            Path checksumPath = Path.of(imageDir, "v2", Storage.CHECKSUM + "." + imageJournalId);
            long expectedCheckSum = Long.parseLong(Files.readString(checksumPath));
            long realCheckSum = checkedInputStream.getChecksum().getValue();
            if (expectedCheckSum != realCheckSum) {
                throw new IOException(String.format("checksum mismatch! expect %d actual %d",
                        expectedCheckSum, realCheckSum));
            }
        }
    }
}
