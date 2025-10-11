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

package com.starrocks.connector.hive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class HiveFieldDelimiterTest {
    private static final Logger LOG = LogManager.getLogger(HiveFieldDelimiterTest.class);

    @Test
    public void testConvert_NullOrEmpty() {
        // Test null input
        try {
            HiveFieldDelimiter.convert(null);
            fail("Expected exception for null input");
        } catch (StarRocksConnectorException e) {
            assertEquals("Delimiter cannot be empty or null", e.getMessage());
        }

        // Test empty input
        try {
            HiveFieldDelimiter.convert("");
            fail("Expected exception for empty input");
        } catch (StarRocksConnectorException e) {
            assertEquals("Delimiter cannot be empty or null", e.getMessage());
        }
    }

    @Test
    public void testConvertHexDelimiter() {
        // Test valid hex formats
        assertEquals("\u0001", HiveFieldDelimiter.convert("\\x01"));
        assertEquals("\u0001", HiveFieldDelimiter.convert("\\X01"));
        assertEquals("\u0001", HiveFieldDelimiter.convert("0x01"));
        assertEquals("\u0001", HiveFieldDelimiter.convert("0X01"));

        // Test multiple bytes
        assertEquals("A", HiveFieldDelimiter.convert("\\x41"));
        assertEquals("AB", HiveFieldDelimiter.convert("\\x4142"));

        // Test odd length hex (should pad with zero)
        assertEquals("\n", HiveFieldDelimiter.convert("\\xA"));
        assertEquals("\n", HiveFieldDelimiter.convert("\\xa"));

        // Test invalid hex formats
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("\\x"));
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("\\xGG"));
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("0x"));
    }

    @Test
    public void testConvertOctalDelimiter() {
        // Test valid octal formats
        assertEquals("\u0001", HiveFieldDelimiter.convert("\\1"));
        assertEquals("\u0001", HiveFieldDelimiter.convert("\\01"));
        assertEquals("\u0001", HiveFieldDelimiter.convert("\\001"));
        assertEquals("\t", HiveFieldDelimiter.convert("\\11"));
        assertEquals("A", HiveFieldDelimiter.convert("\\101"));

        // Test invalid octal formats
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("\\"));
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("\\8"));
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("\\400")); // > 255
    }

    @Test
    public void testConvertUnicodeDelimiter() {
        // Test valid unicode formats
        assertEquals("A", HiveFieldDelimiter.convert("\\u0041"));
        assertEquals("A", HiveFieldDelimiter.convert("\\U0041"));
        assertEquals("€", HiveFieldDelimiter.convert("\\u20AC"));
        assertEquals("😀", HiveFieldDelimiter.convert("\\U0001F600"));

        // Test invalid unicode formats
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("\\u"));
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("\\u004"));
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("\\uGGGG"));
    }

    @Test
    public void testConvertSpecialEscape() {
        // Test special escape sequences
        assertEquals("\u0007", HiveFieldDelimiter.convert("\\a")); // alert/bell
        assertEquals("\b", HiveFieldDelimiter.convert("\\b"));     // backspace
        assertEquals("\f", HiveFieldDelimiter.convert("\\f"));     // form feed
        assertEquals("\n", HiveFieldDelimiter.convert("\\n"));     // newline
        assertEquals("\r", HiveFieldDelimiter.convert("\\r"));     // carriage return
        assertEquals("\t", HiveFieldDelimiter.convert("\\t"));     // tab
        assertEquals("\u000B", HiveFieldDelimiter.convert("\\v")); // vertical tab
        assertEquals("\\", HiveFieldDelimiter.convert("\\\\"));    // backslash
        assertEquals("'", HiveFieldDelimiter.convert("\\'"));      // single quote
        assertEquals("\"", HiveFieldDelimiter.convert("\\\""));    // double quote
        assertEquals("?", HiveFieldDelimiter.convert("\\?"));      // question mark
        assertEquals("\0", HiveFieldDelimiter.convert("\\0"));     // null

        // Test single-digit octal in special escape
        assertEquals("\u0001", HiveFieldDelimiter.convert("\\1"));
        assertEquals("\u0002", HiveFieldDelimiter.convert("\\2"));
        assertEquals("\u0007", HiveFieldDelimiter.convert("\\7"));

        // Test regular character escape
        assertEquals("A", HiveFieldDelimiter.convert("\\A"));
        assertEquals("B", HiveFieldDelimiter.convert("\\B"));
    }

    @Test
    public void testConvertNumericDelimiter() {
        // Test valid numeric formats
        assertEquals("\u0000", HiveFieldDelimiter.convert("0"));
        assertEquals("\u0001", HiveFieldDelimiter.convert("1"));
        assertEquals("\t", HiveFieldDelimiter.convert("9"));
        assertEquals("A", HiveFieldDelimiter.convert("65"));

        // Test invalid numeric formats
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("-1"));
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("256"));
        assertThrows(StarRocksConnectorException.class, () -> HiveFieldDelimiter.convert("999"));
    }

    @Test
    public void testConvertMixedEscapedCharacters() {
        // Test mixed escape sequences
        assertEquals("\t\n\r", HiveFieldDelimiter.convert("\\t\\n\\r"));
        assertEquals("A\tB\nC", HiveFieldDelimiter.convert("A\\tB\\nC"));

        // Test octal in mixed context
        assertEquals("\u0001\u0002\u0003", HiveFieldDelimiter.convert("\\1\\2\\3"));
        assertEquals("A\u0001B", HiveFieldDelimiter.convert("A\\1B"));

        // Test hex in mixed context
        assertEquals("A\nB", HiveFieldDelimiter.convert("A\\x0AB"));
        assertEquals("\u0001\u0002", HiveFieldDelimiter.convert("\\x01\\x02"));

        // Test unicode in mixed context
        assertEquals("A€B", HiveFieldDelimiter.convert("A\\u20ACB"));
        assertEquals("😀😃", HiveFieldDelimiter.convert("\\U0001F600\\U0001F603"));

        // Test complex mixed sequences
        assertEquals("Column1\tColumn2\n", HiveFieldDelimiter.convert("Column1\\tColumn2\\n"));
        assertEquals("\u0001\t\u0002\n\u0003", HiveFieldDelimiter.convert("\\1\\t\\2\\n\\3"));

        // Test invalid sequences in mixed context (should handle gracefully)
        assertEquals("\\xGG", HiveFieldDelimiter.convert("\\xGG")); // invalid hex becomes literal
        assertEquals("\\u004", HiveFieldDelimiter.convert("\\u004")); // invalid unicode becomes literal
    }

    @Test
    public void testRealWorldDelimiters() {
        // Test common Hive delimiters
        assertEquals("\t", HiveFieldDelimiter.convert("\\t"));
        assertEquals("\n", HiveFieldDelimiter.convert("\\n"));
        assertEquals("\u0001", HiveFieldDelimiter.convert("\\x01"));
        assertEquals("\u0002", HiveFieldDelimiter.convert("\\x02"));
        assertEquals("|", HiveFieldDelimiter.convert("|"));
        assertEquals(",", HiveFieldDelimiter.convert(","));

        // Test ASCII control characters
        assertEquals("\u0001", HiveFieldDelimiter.convert("1")); // SOH
        assertEquals("\u0002", HiveFieldDelimiter.convert("2")); // STX
        assertEquals("\u0003", HiveFieldDelimiter.convert("3")); // ETX

        // Test combination of literal and escape
        assertEquals("field1\tfield2", HiveFieldDelimiter.convert("field1\\tfield2"));
    }

    @Test
    public void testEdgeCases() {
        // Test single character
        assertEquals("a", HiveFieldDelimiter.convert("a"));
        assertEquals("\\", HiveFieldDelimiter.convert("\\"));

        // Test multiple backslashes
        assertEquals("\\\\", HiveFieldDelimiter.convert("\\\\\\\\"));

        // Test incomplete escape sequences
        assertEquals("\\", HiveFieldDelimiter.convert("\\"));
        assertEquals("\\x", HiveFieldDelimiter.convert("\\x"));
        assertEquals("\\u", HiveFieldDelimiter.convert("\\u"));

        // Test very long hex strings
        assertEquals("Hello", HiveFieldDelimiter.convert("\\x48656C6C6F"));

        // Test boundary values
        assertEquals("\u0000", HiveFieldDelimiter.convert("0"));
    }

    // Helper method to verify exception message
    private void assertExceptionMessage(String input, String expectedMessage) {
        try {
            HiveFieldDelimiter.convert(input);
            fail("Expected exception for input: " + input);
        } catch (StarRocksConnectorException e) {
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    // Mock exception class for testing
    private static class StarRocksConnectorException extends RuntimeException {
        public StarRocksConnectorException(String message) {
            super(message);
        }
    }
}
