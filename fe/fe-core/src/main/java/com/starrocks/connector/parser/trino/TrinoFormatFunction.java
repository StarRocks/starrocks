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

package com.starrocks.connector.parser.trino;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Trino's format(format, args...), which formats with Java's String.format, so that its output depends on the Java
 * type of each argument. The Trino parser turns a call with a literal format into trino_format(format, args...),
 * and the analyzer rewrites that into a concat() of the formatted pieces once the argument types are known.
 *
 * <p>Only conversions whose output can be reproduced exactly are supported:
 * <ul>
 *   <li>%s of a VARCHAR, integer, BOOLEAN or DATE value</li>
 *   <li>%d of an integer value, with an optional width and '-' or '0' flag</li>
 *   <li>%f of a DECIMAL value, with an optional precision (6 by default), and %,.2f</li>
 *   <li>%% and %n</li>
 * </ul>
 * Arguments can be referenced by index, as in %2$s. A NULL argument is formatted as "null", as in Java. Anything else
 * fails, for example %f of a DOUBLE value, whose digits Java computes from the shortest decimal representation.
 */
public final class TrinoFormatFunction {
    // The format specifier syntax of java.util.Formatter.
    private static final Pattern SPECIFIER =
            Pattern.compile("%(\\d+\\$)?([-#+ 0,(<]*)?(\\d+)?(\\.\\d+)?([tT])?([a-zA-Z%])");

    private static final int MAX_DECIMAL128_PRECISION = 38;
    private static final int DEFAULT_PRECISION = 6;

    private TrinoFormatFunction() {
    }

    static final class Specifier {
        // 1-based index of the argument after the format, or 0 for %% and %n
        final int argumentIndex;
        final String flags;
        // -1 when absent
        final int width;
        final int precision;
        final char conversion;
        final String text;

        Specifier(int argumentIndex, String flags, int width, int precision, char conversion, String text) {
            this.argumentIndex = argumentIndex;
            this.flags = flags;
            this.width = width;
            this.precision = precision;
            this.conversion = conversion;
            this.text = text;
        }
    }

    /**
     * Splits the format into literal strings and specifiers. Throws IllegalArgumentException for a specifier that is
     * not supported or that references a missing argument.
     */
    static List<Object> parse(String format, int argumentCount) {
        List<Object> pieces = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        Matcher matcher = SPECIFIER.matcher(format);
        int ordinaryIndex = 0;
        int i = 0;
        while (i < format.length()) {
            char c = format.charAt(i);
            if (c != '%') {
                literal.append(c);
                i++;
                continue;
            }
            if (!matcher.find(i) || matcher.start() != i) {
                throw new IllegalArgumentException("invalid format specifier at " + format.substring(i));
            }
            String text = matcher.group();
            String index = matcher.group(1);
            String flags = matcher.group(2) == null ? "" : matcher.group(2);
            int width = matcher.group(3) == null ? -1 : parseNumber(matcher.group(3), text);
            int precision = matcher.group(4) == null ? -1 : parseNumber(matcher.group(4).substring(1), text);
            char conversion = matcher.group(6).charAt(0);
            if (matcher.group(5) != null) {
                throw unsupported(text);
            }
            i = matcher.end();

            if (conversion == '%' || conversion == 'n') {
                if (index != null || !flags.isEmpty() || width != -1 || precision != -1) {
                    throw unsupported(text);
                }
                literal.append(conversion == '%' ? "%" : "\n");
                continue;
            }
            if (flags.contains("<")) {
                throw unsupported(text);
            }
            int argumentIndex;
            if (index != null) {
                argumentIndex = parseNumber(index.substring(0, index.length() - 1), text);
            } else {
                argumentIndex = ++ordinaryIndex;
            }
            if (argumentIndex < 1 || argumentIndex > argumentCount) {
                throw new IllegalArgumentException("no argument for format specifier " + text);
            }
            checkSupported(flags, width, precision, conversion, text);

            if (literal.length() > 0) {
                pieces.add(literal.toString());
                literal.setLength(0);
            }
            pieces.add(new Specifier(argumentIndex, flags, width, precision, conversion, text));
        }
        if (literal.length() > 0) {
            pieces.add(literal.toString());
        }
        return pieces;
    }

    private static void checkSupported(String flags, int width, int precision, char conversion, String text) {
        switch (conversion) {
            case 's':
                if (!flags.isEmpty() || width != -1 || precision != -1) {
                    throw unsupported(text);
                }
                return;
            case 'd':
                // Java requires a width for '-' and '0', and does not allow both.
                if (precision != -1 || !(flags.isEmpty() || flags.equals("-") || flags.equals("0")) ||
                        (!flags.isEmpty() && width == -1)) {
                    throw unsupported(text);
                }
                return;
            case 'f':
                if (width != -1 || !(flags.isEmpty() || flags.equals(",")) ||
                        (flags.equals(",") && precision != 2)) {
                    throw unsupported(text);
                }
                return;
            default:
                throw unsupported(text);
        }
    }

    private static int parseNumber(String digits, String text) {
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            throw unsupported(text);
        }
    }

    private static IllegalArgumentException unsupported(String text) {
        return new IllegalArgumentException("unsupported format specifier " + text);
    }

    /**
     * Returns the analyzed arguments of concat() for trino_format(format, args...), whose children are analyzed.
     * New expressions are passed to {@code analyzer} bottom-up, before they are used.
     */
    public static List<Expr> rewrite(FunctionCallExpr node, UnaryOperator<Expr> analyzer) {
        if (node.getChildren().isEmpty() || !(node.getChild(0) instanceof StringLiteral)) {
            throw new SemanticException("format() requires a literal format string", node.getPos());
        }
        String format = ((StringLiteral) node.getChild(0)).getStringValue();
        List<Expr> arguments = node.getChildren().subList(1, node.getChildren().size());
        List<Object> pieces;
        try {
            pieces = parse(format, arguments.size());
        } catch (IllegalArgumentException e) {
            throw new SemanticException("format() " + e.getMessage(), node.getPos());
        }

        List<Expr> result = new ArrayList<>();
        for (Object piece : pieces) {
            if (piece instanceof String) {
                result.add(new StringLiteral((String) piece));
            } else {
                Specifier specifier = (Specifier) piece;
                Expr argument = arguments.get(specifier.argumentIndex - 1);
                result.add(format(specifier, argument, analyzer, node));
            }
        }
        if (result.isEmpty()) {
            result.add(new StringLiteral(""));
        }
        return result;
    }

    private static Expr format(Specifier specifier, Expr argument, UnaryOperator<Expr> analyzer,
                               FunctionCallExpr node) {
        Type type = argument.getType();
        // Java formats a null argument as "null", cut to the precision and padded to the width.
        String nullText = "null";
        if (specifier.precision != -1 && specifier.precision < nullText.length()) {
            nullText = nullText.substring(0, specifier.precision);
        }
        nullText = justify(nullText, specifier);
        if (type.isNull()) {
            return new StringLiteral(nullText);
        }
        Expr formatted;
        switch (specifier.conversion) {
            case 's':
                formatted = formatString(argument, analyzer);
                break;
            case 'd':
                formatted = isInteger(type) ? formatInteger(specifier, argument, analyzer) : null;
                break;
            default:
                formatted = formatDecimal(specifier, argument, analyzer);
                break;
        }
        if (formatted == null) {
            throw new SemanticException(String.format("format() does not support %s for %s values",
                    specifier.text, type.toSql()), node.getPos());
        }
        return call(analyzer, FunctionSet.IFNULL, formatted, new StringLiteral(nullText));
    }

    // String.valueOf() of the Java value of the argument
    private static Expr formatString(Expr argument, UnaryOperator<Expr> analyzer) {
        Type type = argument.getType();
        if (type.isVarchar()) {
            return argument;
        } else if (isInteger(type) || type.isDate()) {
            return toVarchar(argument, analyzer);
        } else if (type.isBoolean()) {
            return call(analyzer, FunctionSet.IF, argument, new StringLiteral("true"), new StringLiteral("false"));
        }
        return null;
    }

    private static Expr formatInteger(Specifier specifier, Expr argument, UnaryOperator<Expr> analyzer) {
        Expr digits = toVarchar(argument, analyzer);
        if (specifier.width == -1) {
            return digits;
        }
        Expr padded;
        if (specifier.flags.equals("0")) {
            // Java pads with zeros after the sign: -5 with %03d is -05.
            Expr negative = analyzer.apply(new BinaryPredicate(BinaryType.LT, argument, new IntLiteral(0)));
            Expr magnitude = call(analyzer, FunctionSet.SUBSTR, digits, new IntLiteral(2));
            Expr negativePadded = call(analyzer, FunctionSet.CONCAT, new StringLiteral("-"),
                    call(analyzer, FunctionSet.LPAD, magnitude, new IntLiteral(specifier.width - 1),
                            new StringLiteral("0")));
            Expr positivePadded = call(analyzer, FunctionSet.LPAD, digits, new IntLiteral(specifier.width),
                    new StringLiteral("0"));
            padded = call(analyzer, FunctionSet.IF, negative, negativePadded, positivePadded);
        } else {
            padded = call(analyzer, specifier.flags.equals("-") ? FunctionSet.RPAD : FunctionSet.LPAD, digits,
                    new IntLiteral(specifier.width), new StringLiteral(" "));
        }
        // lpad and rpad truncate a longer string, while Java never truncates.
        Expr fits = analyzer.apply(new BinaryPredicate(BinaryType.GE,
                call(analyzer, FunctionSet.CHAR_LENGTH, digits), new IntLiteral(specifier.width)));
        return call(analyzer, FunctionSet.IF, fits, digits, padded);
    }

    // %.Nf of a BigDecimal rounds the magnitude half up to N digits and prints them all, after the sign of the
    // unrounded value: -0.001 with %.2f is -0.00.
    private static Expr formatDecimal(Specifier specifier, Expr argument, UnaryOperator<Expr> analyzer) {
        Type type = argument.getType();
        if (!type.isDecimalV3() || type.isDecimal256()) {
            return null;
        }
        // if() and subtraction, unlike abs(), are folded for a constant argument.
        Expr negative = analyzer.apply(new BinaryPredicate(BinaryType.LT, argument, new IntLiteral(0)));
        Expr negated = analyzer.apply(new ArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, new IntLiteral(0), argument));
        Expr magnitude = call(analyzer, FunctionSet.IF, negative, negated, argument);
        Expr digits;
        if (specifier.flags.equals(",")) {
            // money_format rounds half up to 2 digits and groups the integer digits with commas.
            digits = call(analyzer, FunctionSet.MONEY_FORMAT, magnitude);
        } else {
            digits = roundToVarchar(specifier, magnitude, analyzer);
            if (digits == null) {
                return null;
            }
        }
        return call(analyzer, FunctionSet.CONCAT,
                call(analyzer, FunctionSet.IF, negative, new StringLiteral("-"), new StringLiteral("")), digits);
    }

    private static Expr roundToVarchar(Specifier specifier, Expr argument, UnaryOperator<Expr> analyzer) {
        Type type = argument.getType();
        int precision = specifier.precision == -1 ? DEFAULT_PRECISION : specifier.precision;
        ScalarType decimalType = (ScalarType) type;
        int integerDigits = decimalType.getScalarPrecision() - decimalType.getScalarScale();
        // One more integer digit for a carry when rounding.
        int targetPrecision = Math.max(1, integerDigits + precision +
                (precision < decimalType.getScalarScale() ? 1 : 0));
        if (targetPrecision > MAX_DECIMAL128_PRECISION) {
            return null;
        }
        Type targetType = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, targetPrecision, precision);
        Expr rounded = analyzer.apply(new CastExpr(new TypeDef(targetType), argument));
        return toVarchar(rounded, analyzer);
    }

    private static boolean isInteger(Type type) {
        return type.isIntegerType() || type.isLargeint();
    }

    private static Expr toVarchar(Expr argument, UnaryOperator<Expr> analyzer) {
        return analyzer.apply(new CastExpr(new TypeDef(VarcharType.VARCHAR), argument));
    }

    private static Expr call(UnaryOperator<Expr> analyzer, String name, Expr... arguments) {
        return analyzer.apply(new FunctionCallExpr(name, Lists.newArrayList(arguments)));
    }

    // Java pads with spaces to the width, on the left unless the '-' flag is given.
    private static String justify(String text, Specifier specifier) {
        if (specifier.width <= text.length()) {
            return text;
        }
        String padding = " ".repeat(specifier.width - text.length());
        return specifier.flags.contains("-") ? text + padding : padding + text;
    }
}
