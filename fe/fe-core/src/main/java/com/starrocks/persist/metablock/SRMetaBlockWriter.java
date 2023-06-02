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


package com.starrocks.persist.metablock;

import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * Save object to output stream as the following format.
 * <p>
 * +------------------+
 * |     header       | {"numJson": 10, "name": "AuthenticationManager"}
 * +------------------+
 * |     Json 1       |
 * +------------------+
 * |     Json 2       |
 * +------------------+
 * |      ...         |
 * +------------------+
 * |     Json 10      |
 * +------------------+
 * |      footer      | {"checksum": xxx}
 * +------------------+
 * <p>
 * Usage see com.starrocks.persist.metablock.SRMetaBlockTest#testSimple()
 */
public class SRMetaBlockWriter {
    private final CheckedOutputStream checkedOutputStream;
    private final SRMetaBlockHeader header;
    private int numJsonWritten;

    @Deprecated
    public SRMetaBlockWriter(DataOutputStream dos, String name, int numJson) throws SRMetaBlockException {
        if (numJson <= 0) {
            throw new SRMetaBlockException(String.format("invalid numJson: %d", numJson));
        }
        this.checkedOutputStream = new CheckedOutputStream(dos, new CRC32());
        this.header = new SRMetaBlockHeader(name, numJson);
        this.numJsonWritten = 0;
    }

    public SRMetaBlockWriter(DataOutputStream dos, SRMetaBlockID id, int numJson) throws SRMetaBlockException {
        if (numJson <= 0) {
            throw new SRMetaBlockException(String.format("invalid numJson: %d", numJson));
        }
        this.checkedOutputStream = new CheckedOutputStream(dos, new CRC32());
        this.header = new SRMetaBlockHeader(id, numJson);
        this.numJsonWritten = 0;
    }

    public void writeJson(Object object) throws IOException, SRMetaBlockException {
        // always check if write more than expect
        if (numJsonWritten >= header.getNumJson()) {
            throw new SRMetaBlockException(String.format(
                    "About to write json more than expect %d, actual %d", header.getNumJson(), numJsonWritten));
        }
        if (numJsonWritten == 0) {
            // write header
            Text.writeStringWithChecksum(checkedOutputStream, GsonUtils.GSON.toJson(header));
        }
        Text.writeStringWithChecksum(checkedOutputStream, GsonUtils.GSON.toJson(object));
        numJsonWritten += 1;
    }

    public void close() throws IOException, SRMetaBlockException {
        // check if write as many json string as expect
        if (numJsonWritten != header.getNumJson()) {
            throw new SRMetaBlockException(String.format(
                    "Block json number mismatch: expect %d actual %d", header.getNumJson(), numJsonWritten));
        }
        // write footer, especially checksum
        SRMetaBlockFooter footer = new SRMetaBlockFooter(checkedOutputStream.getChecksum().getValue());
        Text.writeStringWithChecksum(checkedOutputStream, GsonUtils.GSON.toJson(footer));
    }
}