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

package com.starrocks.load.streamload;

import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPartialUpdateMode;

import java.util.Optional;

/**
 * Interface to get parameters for a stream load. These
 * parameters determine the behaviour of the stream load.
 */
public interface StreamLoadParams {

    Optional<TFileFormatType> getFileFormatType();
    Optional<TFileType> getFileType();
    Optional<String> getFilePath();
    Optional<String> getColumns();
    Optional<String> getWhere();
    Optional<String> getPartitions();
    Optional<Boolean> getIsTempPartition();
    Optional<Boolean> getNegative();
    Optional<Double> getMaxFilterRatio();
    Optional<Integer> getTimeout();
    Optional<Boolean> getStrictMode();
    Optional<String> getTimezone();
    Optional<Long> getLoadMemLimit();
    Optional<String> getTransmissionCompressionType();
    Optional<Integer> getLoadDop();
    Optional<Boolean> getEnableReplicatedStorage();
    Optional<String> getMergeCondition();
    Optional<Long> getLogRejectedRecordNum();
    Optional<Boolean> getPartialUpdate();
    Optional<TPartialUpdateMode> getPartialUpdateMode();
    Optional<String> getPayloadCompressionType();
    Optional<String> getWarehouse();

    // Parameters for csv format ================================
    Optional<String> getColumnSeparator();
    Optional<String> getRowDelimiter();
    Optional<Long> getSkipHeader();
    Optional<Byte> getEnclose();
    Optional<Byte> getEscape();
    Optional<Boolean> getTrimSpace();

    // Parameters for json format ================================
    Optional<String> getJsonPaths();
    Optional<String> getJsonRoot();
    Optional<Boolean> getStripOuterArray();
}
