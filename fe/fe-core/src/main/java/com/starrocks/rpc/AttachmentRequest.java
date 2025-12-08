// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.rpc;

import com.baidu.bjf.remoting.protobuf.annotation.Ignore;
import com.github.luben.zstd.Zstd;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.transport.TTransportException;

public class AttachmentRequest {
    public enum CompressionType {
        LZ4("lz4"),
        ZSTD("zstd"),
        NONE("none");

        private final String value;

        CompressionType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static CompressionType fromConfig(String config) {
            if (ZSTD.value.equalsIgnoreCase(config)) {
                return ZSTD;
            }
            return LZ4;
        }
    }

    private static final class Lz4Holder {
        static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();
        static final LZ4Compressor COMPRESSOR = FACTORY.fastCompressor();
    }

    @Ignore
    protected byte[] serializedRequest;
    @Ignore
    protected byte[] serializedResult;
    @Ignore
    protected long uncompressedSize;
    @Ignore
    protected CompressionType compressionType = CompressionType.NONE;

    public static TSerializer getSerializer(String protocol) throws TTransportException {
        return ConfigurableSerDesFactory.getTSerializer(protocol);
    }

    public <T extends TBase<T, F>, F extends TFieldIdEnum> void setRequest(TBase<T, F> request, String protocol)
            throws TException {
        TSerializer serializer = getSerializer(protocol);
        try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeploySerializeTime")) {
            applyCompression(serializer.serialize(request));
        }
    }

    public <T extends TBase<T, F>, F extends TFieldIdEnum> void setRequest(TBase<T, F> request) throws TException {
        applyCompression(ConfigurableSerDesFactory.getTSerializer().serialize(request));
    }

    public void setRequest(byte[] request) {
        applyCompression(request);
    }

    private void applyCompression(byte[] data) {
        int threshold = Config.thrift_plan_fragment_compression_threshold_bytes;
        if (threshold > 0 && data.length >= threshold) {
            CompressionResult result = compress(data);
            if (result != null && isCompressionEffective(data.length, result.length)) {
                this.serializedRequest = result.toExactSizeArray();
                this.uncompressedSize = data.length;
                this.compressionType = result.type;
                return;
            }
        }
        this.serializedRequest = data;
        this.uncompressedSize = 0;
        this.compressionType = CompressionType.NONE;
    }

    private CompressionResult compress(byte[] data) {
        try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeployCompressTime")) {
            CompressionType type = CompressionType.fromConfig(Config.thrift_plan_fragment_compression_algorithm);
            if (type == CompressionType.ZSTD) {
                return compressZstd(data);
            }
            return compressLz4(data);
        }
    }

    private CompressionResult compressLz4(byte[] data) {
        int maxLen = Lz4Holder.COMPRESSOR.maxCompressedLength(data.length);
        byte[] buffer = new byte[maxLen];
        int len = Lz4Holder.COMPRESSOR.compress(data, 0, data.length, buffer, 0, maxLen);
        return new CompressionResult(buffer, len, CompressionType.LZ4);
    }

    private CompressionResult compressZstd(byte[] data) {
        int maxLen = (int) Zstd.compressBound(data.length);
        byte[] buffer = new byte[maxLen];
        int len = (int) Zstd.compressByteArray(buffer, 0, maxLen, data, 0, data.length, Zstd.defaultCompressionLevel());
        return new CompressionResult(buffer, len, CompressionType.ZSTD);
    }

    private boolean isCompressionEffective(int originalSize, int compressedSize) {
        double ratio = (double) originalSize / compressedSize;
        return ratio > Config.thrift_plan_fragment_compression_ratio_threshold;
    }

    private static final class CompressionResult {
        final byte[] buffer;
        final int length;
        final CompressionType type;

        CompressionResult(byte[] buffer, int length, CompressionType type) {
            this.buffer = buffer;
            this.length = length;
            this.type = type;
        }

        byte[] toExactSizeArray() {
            if (buffer.length == length) {
                return buffer;
            }
            byte[] result = new byte[length];
            System.arraycopy(buffer, 0, result, 0, length);
            return result;
        }
    }

    public byte[] getSerializedRequest() {
        return serializedRequest;
    }

    public long getUncompressedSize() {
        return uncompressedSize;
    }

    public String getCompressionType() {
        return compressionType.getValue();
    }

    public void setSerializedResult(byte[] result) {
        this.serializedResult = result;
    }

    public byte[] getSerializedResult() {
        return serializedResult;
    }

    public <T extends TBase<T, F>, F extends TFieldIdEnum> void getResult(TBase<T, F> result) throws TException {
        TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
        deserializer.deserialize(result, serializedResult);
    }

    public <T extends TBase<T, F>, F extends TFieldIdEnum> void getRequest(TBase<T, F> request) throws TException {
        TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
        deserializer.deserialize(request, serializedRequest);
    }
}
