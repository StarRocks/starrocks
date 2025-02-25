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

package com.starrocks.paimon.reader;

import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonWriter {
    private static final Logger LOG = LogManager.getLogger(PaimonWriter.class);
    private final ClassLoader classLoader;
    private final BatchTableWrite batchTableWrite;

    public PaimonWriter(Map<String, String> params) {
        String encodedTable = params.get("native_table");
        this.classLoader = this.getClass().getClassLoader();
        Table table = ScannerHelper.decodeStringToObject(encodedTable);

        // todo: delete it when we support bucket level shuffle
        table.copy(new HashMap<String, String>() {
            {
                put("write-only", "true");
            }
        });
        this.batchTableWrite = table.newBatchWriteBuilder().withOverwrite().newWrite();
    }

    public void write(List<Object[]> objectArrays) throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            StarRocksRow starRocksRow = StarRocksRow.of(objectArrays);
            for (int row = 0; row < objectArrays.get(0).length; ) {
                batchTableWrite.write(starRocksRow);
                ++row;
                starRocksRow.setRowCount(row);
            }
        } catch (Exception e) {
            close();
            String msg = "Failed to write paimon data.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    public String commit() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            List<CommitMessage> commitMessageList = batchTableWrite.prepareCommit();
            CommitMessageSerializer commitMessageSerializer = new CommitMessageSerializer();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            commitMessageSerializer.serializeList(commitMessageList, new DataOutputViewStreamWrapper(bos));
            return bos.toString();
        } catch (Exception e) {
            close();
            String msg = "Failed to commit a paimon table.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            // do nothing now
        } catch (Exception e) {
            String msg = "Failed to close the paimon writer.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }
}
