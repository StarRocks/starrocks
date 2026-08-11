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

package com.starrocks.connector.bigquery;

import com.starrocks.connector.RemoteFileDesc;

/**
 * Represents a single BigQuery Storage Read API stream as a scan range.
 * One {@code BigQueryRemoteFileDesc} is created per ReadStream returned by
 * {@code CreateReadSession}.
 */
public class BigQueryRemoteFileDesc extends RemoteFileDesc {
    private final String readSessionName;
    private final String readStreamName;
    private final int streamIndex;
    /** True when this stream reads from a temp table created to materialise a view. */
    private final boolean isTempTable;

    private BigQueryRemoteFileDesc(String readSessionName, String readStreamName,
                                   int streamIndex, boolean isTempTable) {
        super(null, null, 0, 0, null);
        this.readSessionName = readSessionName;
        this.readStreamName = readStreamName;
        this.streamIndex = streamIndex;
        this.isTempTable = isTempTable;
    }

    public static BigQueryRemoteFileDesc createBigQueryRemoteFileDesc(
            String readSessionName, String readStreamName, int streamIndex) {
        return new BigQueryRemoteFileDesc(readSessionName, readStreamName, streamIndex, false);
    }

    public static BigQueryRemoteFileDesc createBigQueryRemoteFileDesc(
            String readSessionName, String readStreamName, int streamIndex, boolean isTempTable) {
        return new BigQueryRemoteFileDesc(readSessionName, readStreamName, streamIndex, isTempTable);
    }

    public String getReadSessionName() {
        return readSessionName;
    }

    public String getReadStreamName() {
        return readStreamName;
    }

    public int getStreamIndex() {
        return streamIndex;
    }

    public boolean isTempTable() {
        return isTempTable;
    }

    @Override
    public String toString() {
        return "BigQueryRemoteFileDesc{" +
                "readSessionName='" + readSessionName + '\'' +
                ", readStreamName='" + readStreamName + '\'' +
                ", streamIndex=" + streamIndex +
                ", isTempTable=" + isTempTable +
                '}';
    }
}
