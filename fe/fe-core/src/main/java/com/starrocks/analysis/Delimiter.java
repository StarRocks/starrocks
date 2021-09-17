// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;

import java.io.StringWriter;

public class Delimiter {
    private static final String HEX_STRING = "0123456789ABCDEF";

    private static byte[] hexStrToBytes(String hexStr) {
        String upperHexStr = hexStr.toUpperCase();
        int length = upperHexStr.length() / 2;
        char[] hexChars = upperHexStr.toCharArray();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            bytes[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return bytes;
    }

    private static byte charToByte(char c) {
        return (byte) HEX_STRING.indexOf(c);
    }

    public static String convertDelimiter(String originStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(originStr)) {
            throw new AnalysisException("Delimiter cannot be empty or null");
        }

        StringWriter writer = new StringWriter();

        for (int i = 0; i < originStr.length(); i++) {
            char ch = originStr.charAt(i);
            boolean outputOneChar = true;
            if (ch == '\\') {
               char nextChar = (i == originStr.length() - 1) ? '\\' : originStr.charAt(i + 1);
               switch (nextChar) {
                   case '\\':
                       ch = '\\';
                       break;
                   case 'b':
                       ch = '\b';
                       break;
                   case 'f':
                       ch = '\f';
                       break;
                   case 'n':
                       ch = '\n';
                       break;
                   case 'r':
                       ch = '\r';
                       break;
                   case 't':
                       ch = '\t';
                       break;
                   case 'x':
                   case 'X': {
                       outputOneChar = false;
                       i = parseHexString(originStr, writer, i);
                       break;
                   }
                   case '\"':
                       ch = '\"';
                       break;
                   case '\'':
                       ch = '\'';
                       break;
                   default:
                       writer.append(ch);
                       continue;
               }
               if (outputOneChar) {
                   writer.append(ch);
                   i++;
               }
               // compatible previous 0x / 0X prefix
            } else if (ch == '0' && i != originStr.length() - 1
                    && (originStr.charAt(i + 1) == 'x' || originStr.charAt(i + 1) == 'X')) {
                i = parseHexString(originStr, writer, i);
            } else {
                writer.append(ch);
            }
        }

        return writer.toString();
    }

    /**
        \t Insert a tab
        \b Insert a backspace
        \n Insert a newline
        \r Insert a carriage
        \f Insert a formed
        \' Insert a single quote character
        \" Insert a double quote character
        \\ Insert a backslash
        \x Insert a hex escape e.g. \x48 represent h
     */
    private static int parseHexString(String originStr, StringWriter writer, int offset) throws AnalysisException {
        if (offset + 4 > originStr.length()) {
            writer.append(originStr, offset, originStr.length());
            return originStr.length();
        }
        String hexStr = originStr.substring(offset + 2, offset + 4);
        for (char hexChar : hexStr.toUpperCase().toCharArray()) {
            if (HEX_STRING.indexOf(hexChar) == -1) {
                throw new AnalysisException("Invalid delimiter '" + originStr + "': invalid hex format");
            }
        }
        for (byte b : hexStrToBytes(hexStr)) {
            writer.append((char) b);
        }
        return offset + 3;
    }

}
