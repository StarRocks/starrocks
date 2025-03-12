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
package com.starrocks.common;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.message.ParameterizedMessage;

/**
 * A logger factory that adds a prefix to all log messages.
 */
public class StarRocksLoggerFactory {
    private final String prefix;

    public StarRocksLoggerFactory(String prefix) {
        this.prefix = prefix == null ? "" : prefix;
    }

    public StarRocksLoggerFactory() {
        this("");
    }

    public Logger getLogger(Class<?> clazz) {
        if (Strings.isNullOrEmpty(prefix)) {
            return LogManager.getLogger(clazz);
        } else {
            return LogManager.getLogger(clazz, new PrefixedMessageFactory(prefix));
        }
    }

    public class PrefixedMessageFactory implements MessageFactory {
        private final String prefix;

        public PrefixedMessageFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Message newMessage(String message, Object... params) {
            return new ParameterizedMessage("[" + prefix + "] " + message, params);
        }

        @Override
        public Message newMessage(String message) {
            return new ParameterizedMessage("[" + prefix + "] " + message);
        }

        @Override
        public Message newMessage(Object message) {
            return new ParameterizedMessage("[" + prefix + "] " + message.toString());
        }
    }
}
