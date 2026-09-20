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

package com.starrocks.jdbcbridge;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JDBCScannerNumericTest {
    @Test
    public void testExactValues() {
        for (String text : List.of("0", "-0.000000000000000000000", "1", "-1.2", "1E-18",
                "99999999999999999999.999999999999999999", "-99999999999999999999.999999999999999999",
                "1.1234567890123456780000", "1E+19")) {
            BigDecimal value = new BigDecimal(text);
            BigDecimal actual = JDBCScanner.requireDecimal128(value);
            Assertions.assertEquals(0, actual.compareTo(value), text);
            Assertions.assertEquals(18, actual.scale());
        }
        Assertions.assertNull(JDBCScanner.requireDecimal128(null));
    }

    @Test
    public void testRejectLossyValues() {
        for (String text : List.of("100000000000000000000", "-100000000000000000000", "1E-19", "-1E-19",
                "1.1234567890123456789", "99999999999999999999999999999999999999")) {
            Assertions.assertThrows(ArithmeticException.class,
                    () -> JDBCScanner.requireDecimal128(new BigDecimal(text)), text);
        }
    }

    @Test
    public void testOnlyExplicitlyFlaggedColumnsUseStrictRead() throws Exception {
        BigDecimal tooWide = new BigDecimal("1E+30");
        for (String driver : List.of("org.postgresql.Driver", "com.mysql.cj.jdbc.Driver")) {
            JDBCScanner scanner = scanner(driver, driver.contains("postgresql") ? new int[] {0} : new int[0],
                    new BigDecimal("1.20"), tooWide);
            List<Object[]> chunk = scanner.getNextChunk();
            Assertions.assertEquals(new BigDecimal(driver.contains("postgresql")
                    ? "1.200000000000000000" : "1.20"), chunk.get(0)[0]);
            Assertions.assertEquals(tooWide, chunk.get(1)[0]);
        }
    }

    @Test
    public void testExplicitFlagWorksWithWrapperDriverName() throws Exception {
        JDBCScanner scanner = scanner("example.WrapperDriver", new int[] {0}, new BigDecimal("1E-19"));
        Assertions.assertThrows(SQLException.class, scanner::getNextChunk);
    }

    @Test
    public void testScannerFailsRatherThanReturningNullOrRounding() throws Exception {
        JDBCScanner scanner = scanner("org.postgresql.Driver", new int[] {0}, new BigDecimal("1E-19"));
        SQLException error = Assertions.assertThrows(SQLException.class, scanner::getNextChunk);
        Assertions.assertEquals("22003", error.getSQLState());
        Assertions.assertTrue(error.getMessage().contains("column 1"));
        Assertions.assertTrue(error.getMessage().contains("DECIMAL(38,18)"));
    }

    @Test
    public void testNullAndUnflaggedAggregateResult() throws Exception {
        JDBCScanner scanner = scanner("org.postgresql.Driver", new int[] {0}, null, new BigDecimal("1E+40"));
        List<Object[]> chunk = scanner.getNextChunk();
        Assertions.assertNull(chunk.get(0)[0]);
        Assertions.assertEquals(new BigDecimal("1E+40"), chunk.get(1)[0]);
    }

    private static JDBCScanner scanner(String driver, int[] strict, BigDecimal... values) throws Exception {
        JDBCScanContext context = new JDBCScanContext();
        context.setDriverClassName(driver);
        context.setStatementFetchSize(1);
        context.setStrictNumericColumns(strict);
        JDBCScanner scanner = new JDBCScanner("unused", context);
        ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(
                JDBCScannerNumericTest.class.getClassLoader(), new Class<?>[] {ResultSetMetaData.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getColumnCount")) {
                        return values.length;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(
                JDBCScannerNumericTest.class.getClassLoader(), new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getObject") || method.getName().equals("getBigDecimal")) {
                        return values[(Integer) args[0] - 1];
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        List<Object[]> columns = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            columns.add(new BigDecimal[1]);
        }
        set(scanner, "resultSetMetaData", metadata);
        set(scanner, "resultSet", resultSet);
        set(scanner, "resultChunk", columns);
        return scanner;
    }

    private static void set(Object object, String name, Object value) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(object, value);
    }
}
