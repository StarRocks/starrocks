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

import com.google.gson.stream.JsonWriter;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.persist.metablock.SRMetaBlockWriterV1;
import com.starrocks.persist.metablock.SRMetaBlockWriterV2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class ImageWriter {
    private final ImageFormatVersion imageFormatVersion;
    private final String imageDir;
    private final long imageJournalId;

    private OutputStream outputStream;
    private CheckedOutputStream checkedOutputStream;
    private JsonWriter jsonWriter;
    private DataOutputStream dataOutputStream;

    public ImageWriter(String imageDir, ImageFormatVersion imageFormatVersion, long imageJournalId) {
        this.imageDir = imageDir;
        this.imageFormatVersion = imageFormatVersion;
        this.imageJournalId = imageJournalId;
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
        this.checkedOutputStream = new CheckedOutputStream(outputStream, new CRC32());
        this.dataOutputStream = new DataOutputStream(checkedOutputStream);
        this.jsonWriter = new JsonWriter(new OutputStreamWriter(checkedOutputStream, StandardCharsets.UTF_8));
    }

    public SRMetaBlockWriter getBlockWriter(SRMetaBlockID id, int numJson) throws SRMetaBlockException {
        if (imageFormatVersion == ImageFormatVersion.v1) {
            return new SRMetaBlockWriterV1(outputStream, id, numJson);
        } else {
            return new SRMetaBlockWriterV2(jsonWriter, id, numJson);
        }
    }

    public DataOutputStream getDataOutputStream() {
        return dataOutputStream;
    }

    public void saveChecksum() throws IOException {
        if (imageFormatVersion == ImageFormatVersion.v2) {
            Path path = Path.of(imageDir, Storage.CHECKSUM + "." + imageJournalId);
            String checksum = String.valueOf(checkedOutputStream.getChecksum().getValue());
            Files.writeString(path, checksum);
        }
    }
}
