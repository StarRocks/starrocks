// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist.metablock;

import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class SRMetaBlockWriter {
    private CheckedOutputStream checkedOutputStream;
    private SRMetaBlockHeader header;
    private int numJsonWritten;

    public SRMetaBlockWriter(DataOutputStream dos, String name, int numJson) throws SRMetaBlockException {
        if (numJson <= 0) {
            throw new SRMetaBlockException(String.format("invalid numJson: %d", numJson));
        }
        this.checkedOutputStream = new CheckedOutputStream(dos, new CRC32());
        this.header = new SRMetaBlockHeader(name, numJson);
        this.numJsonWritten = 0;
    }

    public void writeJson(Object object) throws IOException, SRMetaBlockException {
        // always check if write more than expect
        if (numJsonWritten >= header.getNumJson()) {
            throw new SRMetaBlockException(String.format(
                    "About to write json more than expect: %d >= %d", numJsonWritten, header.getNumJson()));
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
