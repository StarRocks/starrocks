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

import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TStreamLoadPutRequest;

import java.util.Optional;

/**
 * An implementation of {@link StreamLoadParams} which is backed by a {@link TStreamLoadPutRequest}.
 * It's used when backend issues a stream load request through thrift rpc
 * {@link FrontendServiceImpl#streamLoadPut(TStreamLoadPutRequest)}
 */
public class StreamLoadThriftParams implements StreamLoadParams {

    private final TStreamLoadPutRequest request;

    public StreamLoadThriftParams(TStreamLoadPutRequest request) {
        this.request = request;
    }

    @Override
    public Optional<TFileFormatType> getFileFormatType() {
        return request.isSetFormatType() ? Optional.ofNullable(request.getFormatType()) : Optional.empty();
    }

    @Override
    public Optional<TFileType> getFileType() {
        return request.isSetFileType() ? Optional.ofNullable(request.getFileType()) : Optional.empty();
    }

    @Override
    public Optional<String> getFilePath() {
        return request.isSetPath() ? Optional.ofNullable(request.getPath()) : Optional.empty();
    }

    @Override
    public Optional<String> getColumns() {
        return request.isSetColumns() ? Optional.ofNullable(request.getColumns()) : Optional.empty();
    }

    @Override
    public Optional<String> getWhere() {
        return request.isSetWhere() ? Optional.ofNullable(request.getWhere()) : Optional.empty();
    }

    @Override
    public Optional<Boolean> getTrimSpace() {
        return request.isSetTrimSpace() ? Optional.of(request.isTrimSpace()) : Optional.empty();
    }

    @Override
    public Optional<String> getPartitions() {
        return request.isSetPartitions() ? Optional.ofNullable(request.getPartitions()) : Optional.empty();
    }

    @Override
    public Optional<Boolean> getIsTempPartition() {
        return request.isSetIsTempPartition() ? Optional.of(request.isTempPartition) : Optional.empty();
    }

    @Override
    public Optional<Boolean> getNegative() {
        return request.isSetNegative() ? Optional.of(request.isNegative()) : Optional.empty();
    }

    @Override
    public Optional<Double> getMaxFilterRatio() {
        return Optional.empty();
    }

    @Override
    public Optional<Integer> getTimeout() {
        return request.isSetTimeout() ? Optional.of(request.getTimeout()) : Optional.empty();
    }

    @Override
    public Optional<Boolean> getStrictMode() {
        return request.isSetStrictMode() ? Optional.of(request.isStrictMode()) : Optional.empty();
    }

    @Override
    public Optional<String> getTimezone() {
        return request.isSetTimezone() ? Optional.ofNullable(request.getTimezone()) : Optional.empty();
    }

    @Override
    public Optional<Long> getLoadMemLimit() {
        return request.isSetLoadMemLimit() ? Optional.of(request.getLoadMemLimit()) : Optional.empty();
    }

    @Override
    public Optional<String> getTransmissionCompressionType() {
        return request.isSetTransmission_compression_type()
                ? Optional.ofNullable(request.getTransmission_compression_type()) : Optional.empty();
    }

    @Override
    public Optional<Integer> getLoadDop() {
        return request.isSetLoad_dop() ? Optional.of(request.getLoad_dop()) : Optional.empty();
    }

    @Override
    public Optional<Boolean> getEnableReplicatedStorage() {
        return request.isSetEnable_replicated_storage() ? Optional.of(request.isEnable_replicated_storage()) : Optional.empty();
    }

    @Override
    public Optional<String> getMergeCondition() {
        return request.isSetMerge_condition() ? Optional.ofNullable(request.getMerge_condition()) : Optional.empty();
    }

    @Override
    public Optional<Long> getLogRejectedRecordNum() {
        return request.isSetLog_rejected_record_num() ? Optional.of(request.getLog_rejected_record_num()) : Optional.empty();
    }

    @Override
    public Optional<Boolean> getPartialUpdate() {
        return request.isSetPartial_update() ? Optional.of(request.isPartial_update()) : Optional.empty();
    }

    @Override
    public Optional<TPartialUpdateMode> getPartialUpdateMode() {
        return request.isSetPartial_update_mode() ? Optional.of(request.getPartial_update_mode()) : Optional.empty();
    }

    @Override
    public Optional<String> getPayloadCompressionType() {
        return request.isSetPayload_compression_type() ?
                Optional.ofNullable(request.getPayload_compression_type()) : Optional.empty();
    }

    @Override
    public Optional<String> getWarehouse() {
        return request.isSetWarehouse() ? Optional.ofNullable(request.getWarehouse()) : Optional.empty();
    }

    @Override
    public Optional<String> getColumnSeparator() {
        return request.isSetColumnSeparator() ? Optional.ofNullable(request.getColumnSeparator()) : Optional.empty();
    }

    @Override
    public Optional<String> getRowDelimiter() {
        return request.isSetRowDelimiter() ? Optional.ofNullable(request.getRowDelimiter()) : Optional.empty();
    }

    @Override
    public Optional<Long> getSkipHeader() {
        return request.isSetSkipHeader() ? Optional.of(request.getSkipHeader()) : Optional.empty();
    }

    @Override
    public Optional<Byte> getEnclose() {
        return request.isSetEnclose() ? Optional.of(request.getEnclose()) : Optional.empty();
    }

    @Override
    public Optional<Byte> getEscape() {
        return request.isSetEscape() ? Optional.of(request.getEscape()) : Optional.empty();
    }

    @Override
    public Optional<String> getJsonPaths() {
        return request.isSetJsonpaths() ? Optional.ofNullable(request.getJsonpaths()) : Optional.empty();
    }

    @Override
    public Optional<String> getJsonRoot() {
        return request.isSetJson_root() ? Optional.ofNullable(request.getJson_root()) : Optional.empty();
    }

    @Override
    public Optional<Boolean> getStripOuterArray() {
        return request.isSetStrip_outer_array() ? Optional.of(request.isStrip_outer_array()) : Optional.empty();
    }
}
