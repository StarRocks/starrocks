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

    public static TConfiguration buildTConfiguration() {
        return new TConfiguration(Config.thrift_max_message_size, Config.thrift_max_frame_size,
                Config.thrift_max_recursion_depth);
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
            trans.getConfiguration().setMaxFrameSize(configuration.getMaxFrameSize());
            trans.getConfiguration().setMaxMessageSize(configuration.getMaxMessageSize());
            trans.getConfiguration().setRecursionLimit(configuration.getRecursionLimit());
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
            trans.getConfiguration().setMaxFrameSize(configuration.getMaxFrameSize());
            trans.getConfiguration().setMaxMessageSize(configuration.getMaxMessageSize());
            trans.getConfiguration().setRecursionLimit(configuration.getRecursionLimit());
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
            trans.getConfiguration().setMaxFrameSize(configuration.getMaxFrameSize());
            trans.getConfiguration().setMaxMessageSize(configuration.getMaxMessageSize());
            trans.getConfiguration().setRecursionLimit(configuration.getRecursionLimit());
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
            trans.getConfiguration().setMaxFrameSize(configuration.getMaxFrameSize());
            trans.getConfiguration().setMaxMessageSize(configuration.getMaxMessageSize());
            trans.getConfiguration().setRecursionLimit(configuration.getRecursionLimit());
            return new TSimpleJSONProtocol(trans);
        }
    }
}
