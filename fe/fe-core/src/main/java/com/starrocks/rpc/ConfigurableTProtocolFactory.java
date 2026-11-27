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

package com.starrocks.rpc;

import com.starrocks.common.Config;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ConfigurableTProtocolFactory {

    public static TProtocolFactory getTProtocolFactory(ConfigurableSerDesFactory.Protocol protocol) {
        TProtocolFactory factory;
        TConfiguration configuration = buildTConfiguration();
        switch (protocol) {
            case JSON:
                factory = new ConfigurableTJsonFactory(configuration);
                break;
            case SIMPLE_JSON:
                factory = new ConfigurableTSimpleJSONFactory(configuration);
                break;
            case COMPACT:
                factory = new ConfigurableTCompactFactory(configuration);
                break;
            default:
                factory = new ConfigurableTBinaryFactory(configuration);
                break;
        }
        return factory;
    }

    public static int effectiveMaxMessageSizeConfig() {
        int configured = Config.thrift_max_message_size;
        return configured > 0 ? configured : Integer.MAX_VALUE;
    }

    public static long effectiveMaxMessageSizeBytes() {
        int configured = Config.thrift_max_message_size;
        return configured > 0 ? configured : Long.MAX_VALUE;
    }

    public static int effectiveMaxFrameSizeConfig() {
        int configured = Config.thrift_max_frame_size;
        return configured > 0 ? configured : Integer.MAX_VALUE;
    }

    public static int effectiveRecursionLimitConfig() {
        int configured = Config.thrift_max_recursion_depth;
        return configured > 0 ? configured : Integer.MAX_VALUE;
    }

    public static TConfiguration buildTConfiguration() {
        return new TConfiguration(effectiveMaxMessageSizeConfig(), effectiveMaxFrameSizeConfig(),
                effectiveRecursionLimitConfig());
    }

    private static void applyConfiguration(TTransport trans, TConfiguration configuration) {
        if (trans == null) {
            return;
        }
        TConfiguration target = trans.getConfiguration();
        if (target != null) {
            target.setMaxFrameSize(configuration.getMaxFrameSize());
            target.setMaxMessageSize(configuration.getMaxMessageSize());
            target.setRecursionLimit(configuration.getRecursionLimit());
        }
        try {
            trans.updateKnownMessageSize(0);
        } catch (TTransportException ignored) {
            // Keep protocol creation resilient if bytes were already consumed on a custom transport.
        }
    }

    public static class ConfigurableTCompactFactory implements TProtocolFactory {

        private static final long NO_LENGTH_LIMIT = -1;
        private final long stringLengthLimit;
        private final long containerLengthLimit;

        private final TConfiguration configuration;

        public ConfigurableTCompactFactory(TConfiguration configuration) {
            this.stringLengthLimit = NO_LENGTH_LIMIT;
            this.containerLengthLimit = NO_LENGTH_LIMIT;
            this.configuration = configuration;
        }

        @Override
        public TProtocol getProtocol(TTransport trans) {
            applyConfiguration(trans, configuration);
            return new TCompactProtocol(trans, stringLengthLimit, containerLengthLimit);
        }
    }

    public static class ConfigurableTBinaryFactory extends TBinaryProtocol.Factory {
        private final TConfiguration configuration;

        public ConfigurableTBinaryFactory(TConfiguration configuration) {
            super();
            this.configuration = configuration;
        }

        @Override
        public TProtocol getProtocol(TTransport trans) {
            applyConfiguration(trans, configuration);
            return new TBinaryProtocol(
                    trans, stringLengthLimit_, containerLengthLimit_, strictRead_, strictWrite_);
        }
    }

    public static class ConfigurableTJsonFactory extends TJSONProtocol.Factory {
        private final TConfiguration configuration;

        public ConfigurableTJsonFactory(TConfiguration configuration) {
            super();
            this.configuration = configuration;
        }

        @Override
        public TProtocol getProtocol(TTransport trans) {
            applyConfiguration(trans, configuration);
            return new TJSONProtocol(trans, fieldNamesAsString_);
        }
    }

    public static class ConfigurableTSimpleJSONFactory extends TSimpleJSONProtocol.Factory {

        private final TConfiguration configuration;

        public ConfigurableTSimpleJSONFactory(TConfiguration configuration) {
            super();
            this.configuration = configuration;
        }

        @Override
        public TProtocol getProtocol(TTransport trans) {
            applyConfiguration(trans, configuration);
            return new TSimpleJSONProtocol(trans);
        }
    }
}
