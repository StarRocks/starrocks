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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.AbstractMessageFactory;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.parquet.Strings;

import java.util.Objects;

/**
 * A logger factory that adds a prefix to all log messages.
 */
public class StarRocksLoggerFactory {
    public static final StarRocksLoggerFactory INSTANCE = new StarRocksLoggerFactory();

    public StarRocksLoggerFactory() {
    }

    /**
     * @param clazz The class to get the logger for
     * @param prefix The prefix to be used for the logger
     * @return The logger for the class with the given identifier by using the specific message factory.
     */
    public Logger getLogger(Class<?> clazz, String prefix) {
        if (!Config.enable_mv_refresh_extra_prefix_logging || Strings.isNullOrEmpty(prefix)) {
            return LogManager.getLogger(clazz);
        } else {
            final LoggerContext loggerContext = LogManager.getContext(LogManager.class.getClassLoader(), false);
            // The message factory is used only when creating a logger, subsequent use does not change
            // the logger but will log a warning if mismatched.
            // For more details see {@code LoggerContext#getLogger}
            final PrefixedMessageFactory messageFactory = new PrefixedMessageFactory(prefix);
            return loggerContext.getLogger(factoryClassKey(clazz, prefix), messageFactory);
        }
    }

    /**
     * Format an identifier key for the logger to be used in the logger context.
     * @param clazz The class to get the logger for
     * @param prefix The prefix to be used for the logger
     * @return An identifier key for the class to be used in the logger context.
     */
    private String factoryClassKey(Class<?> clazz, String prefix) {
        return String.format("%s.%s", clazz.getName(), prefix);
    }

    /**
     * A message factory that adds a prefix to all associated log messages.
     */
    public static class PrefixedMessageFactory extends AbstractMessageFactory {
        private final String prefix;

        public PrefixedMessageFactory(String prefix) {
            this.prefix = prefix;
        }

        private String format(String message) {
            return prefix + message;
        }

        @Override
        public Message newMessage(CharSequence message) {
            return new ParameterizedMessage(format(message.toString()));
        }

        @Override
        public Message newMessage(Object message) {
            return new ParameterizedMessage(format(Objects.toString(message)));
        }

        @Override
        public Message newMessage(String message) {
            return new ParameterizedMessage(format(message));
        }

        @Override
        public Message newMessage(final String message, final Object... params) {
            return new ParameterizedMessage(format(message), params);
        }

        @Override
        public Message newMessage(final String message, final Object p0) {
            return new ParameterizedMessage(format(message), p0);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1) {
            return new ParameterizedMessage(format(message), p0, p1);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1, final Object p2) {
            return new ParameterizedMessage(format(message), p0, p1, p2);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1, final Object p2,
                                  final Object p3) {
            return new ParameterizedMessage(format(message), p0, p1, p2, p3);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1, final Object p2,
                                  final Object p3, final Object p4) {
            return new ParameterizedMessage(format(message), p0, p1, p2, p3, p4);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1, final Object p2,
                                  final Object p3, final Object p4, final Object p5) {
            return new ParameterizedMessage(format(message), p0, p1, p2, p3, p4, p5);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1, final Object p2,
                                  final Object p3, final Object p4, final Object p5,
                                  final Object p6) {
            return new ParameterizedMessage(format(message), p0, p1, p2, p3, p4, p5, p6);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1, final Object p2,
                                  final Object p3, final Object p4, final Object p5,
                                  final Object p6, final Object p7) {
            return new ParameterizedMessage(format(message), p0, p1, p2, p3, p4, p5, p6, p7);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1, final Object p2,
                                  final Object p3, final Object p4, final Object p5,
                                  final Object p6, final Object p7, final Object p8) {
            return new ParameterizedMessage(format(message), p0, p1, p2, p3, p4, p5, p6, p7, p8);
        }

        @Override
        public Message newMessage(final String message, final Object p0, final Object p1, final Object p2,
                                  final Object p3, final Object p4, final Object p5,
                                  final Object p6, final Object p7, final Object p8, final Object p9) {
            return new ParameterizedMessage(format(message), p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(prefix);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o == null || !(o instanceof PrefixedMessageFactory)) {
                return false;
            }
            return ((PrefixedMessageFactory) o).prefix.equals(this.prefix);
        }
    }
}
