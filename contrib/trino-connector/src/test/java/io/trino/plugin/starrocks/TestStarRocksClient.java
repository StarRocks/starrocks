/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.trino.plugin.starrocks;

import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStarRocksClient
{
    private static final ConnectorSession SESSION = new ConnectorSession()
    {
        @Override
        public String getQueryId()
        {
            return "test";
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.empty();
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return ConnectorIdentity.ofUser("test");
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return TimeZoneKey.UTC_KEY;
        }

        @Override
        public Locale getLocale()
        {
            return Locale.ENGLISH;
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return Optional.empty();
        }

        @Override
        public Instant getStart()
        {
            return Instant.EPOCH;
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            throw new UnsupportedOperationException();
        }
    };

    private static final ConnectorExpressionRewriter<ParameterizedExpression> REWRITER =
            StarRocksClient.createConnectorExpressionRewriter(identifier -> "`" + identifier + "`");

    @Test
    public void testSafeNonNumericComparisonsAreRewritten()
    {
        List<Type> types = List.of(BOOLEAN, DATE, createTimeType(6), createTimestampType(6), VARBINARY);
        Map<FunctionName, String> operators = Map.of(
                EQUAL_OPERATOR_FUNCTION_NAME, "=",
                NOT_EQUAL_OPERATOR_FUNCTION_NAME, "<>",
                LESS_THAN_OPERATOR_FUNCTION_NAME, "<",
                LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, "<=",
                GREATER_THAN_OPERATOR_FUNCTION_NAME, ">",
                GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ">=");

        for (Type type : types) {
            for (Map.Entry<FunctionName, String> operator : operators.entrySet()) {
                Optional<ParameterizedExpression> rewritten = rewrite(type, operator.getKey());
                assertTrue(rewritten.isPresent(), "Expected rewrite for %s and %s".formatted(type, operator.getKey()));
                assertEquals("(`left`) %s (`right`)".formatted(operator.getValue()), rewritten.orElseThrow().expression());
            }
        }
    }

    @Test
    public void testExistingNumericComparisonIsRewritten()
    {
        assertTrue(rewrite(DOUBLE, EQUAL_OPERATOR_FUNCTION_NAME).isPresent());
    }

    @Test
    public void testUnsafeComparisonsAreNotRewritten()
    {
        assertTrue(rewrite(REAL, EQUAL_OPERATOR_FUNCTION_NAME).isEmpty());
        assertTrue(rewrite(createVarcharType(10), EQUAL_OPERATOR_FUNCTION_NAME).isEmpty());
        assertTrue(rewrite(DATE, IDENTICAL_OPERATOR_FUNCTION_NAME).isEmpty());
    }

    private static Optional<ParameterizedExpression> rewrite(Type type, FunctionName functionName)
    {
        Variable left = new Variable("left", type);
        Variable right = new Variable("right", type);
        Call comparison = new Call(BOOLEAN, functionName, List.of(left, right));
        return REWRITER.rewrite(
                SESSION,
                comparison,
                Map.of("left", column("left", type), "right", column("right", type)));
    }

    private static JdbcColumnHandle column(String name, Type type)
    {
        JdbcTypeHandle jdbcType = new JdbcTypeHandle(
                Types.OTHER,
                Optional.of(type.getBaseName()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new JdbcColumnHandle(name, jdbcType, type);
    }
}
