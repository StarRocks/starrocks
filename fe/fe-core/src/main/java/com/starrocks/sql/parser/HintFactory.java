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

package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.SetVarHint;
import com.starrocks.analysis.UserVariableHint;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.UserVariable;
import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.starrocks.analysis.SetVarHint.SET_VAR;
import static com.starrocks.analysis.UserVariableHint.SET_USER_VARIABLE;
import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class HintFactory {


    public static HintNode buildHintNode(Token token, SessionVariable sessionVariable) {
        String text = token.getText();
        // remove /*+ */
        text = text.substring(3, text.length() - 2);
        text = trimWithSpace(text);
        HintNode node;
        if (SetVarHint.LEAST_LEN < text.length()
                && SET_VAR.equalsIgnoreCase(text.substring(0, SET_VAR.length()))) {
            text = text.substring(SET_VAR.length());
            node = buildSetVarHint(text, token);
            if (node == null) {
                throw new ParsingException(PARSER_ERROR_MSG.invalidHintValue(token.getText()), new NodePosition(token));
            }
            return node;
        } else if (UserVariableHint.LEAST_LEN < text.length() &&
                SET_USER_VARIABLE.equalsIgnoreCase(text.substring(0, SET_USER_VARIABLE.length()))) {
            text = text.substring(SET_USER_VARIABLE.length());
            node = buildUserVariableHint(text, token, sessionVariable);
            if (node == null) {
                throw new ParsingException(PARSER_ERROR_MSG.invalidHintValue(token.getText()), new NodePosition(token));
            }
            return node;
        } else {
            // unsupported hint format, just regard it as a comment
            return null;
        }
    }


    private static String trimWithSpace(String text) {
        int length = text.length();
        int len = length;
        int st = 0;
        while (st < len && isWhiteSpace(text.charAt(st))) {
            st++;
        }

        while (st < len && isWhiteSpace(text.charAt(len - 1))) {
            len--;
        }

        return st < len ? text.substring(st, len) : "";
    }

    private static SetVarHint buildSetVarHint(String text, Token token) {
        int length = text.length();
        int idx = 0;
        List<String> splitRes = Lists.newArrayList();
        char inStringStart = '-';
        StringBuilder sb = new StringBuilder();

        boolean expectSplitSymbol = false;
        boolean hasStart = false;
        boolean hasStop = false;
        while (idx < length) {
            char character = text.charAt(idx);
            if (character == '\"' || character == '\'') {
                inStringStart = character;
                idx++;
                while (idx < length && ((text.charAt(idx) != inStringStart) || text.charAt(idx - 1) == '\\')) {
                    sb.append(text.charAt(idx));
                    idx++;
                }
                expectSplitSymbol = true;
            } else if (isWhiteSpace(character)) {
                // do nothing just skip
            } else if (character == '(' && !hasStart) {
                hasStart = true;
            } else if (character == ')' && !hasStop) {
                hasStop = true;
            } else if (character == '=' || character == ',') {
                if (sb.length() != 0) {
                    splitRes.add(sb.toString());
                    sb = new StringBuilder();
                    expectSplitSymbol = false;
                } else {
                    return null;
                }
            } else if (expectSplitSymbol || character == '(' || character == ')') {
                return null;
            } else {
                sb.append(character);
            }
            idx++;
        }
        if (sb.length() != 0) {
            splitRes.add(sb.toString());
        }

        if (splitRes.isEmpty() || (splitRes.size() % 2 != 0)) {
            return null;
        }

        Map<String, String> valueMap = Maps.newHashMap();
        int pos = 0;
        int size = splitRes.size();
        while (pos < size - 1) {
            String key = splitRes.get(pos);
            String value = splitRes.get(pos + 1);
            valueMap.put(key.toLowerCase(Locale.ROOT), value);
            pos += 2;
        }

        return pos == size ?
                new SetVarHint(new NodePosition(token), valueMap, token.getText()) : null;
    }

    private static UserVariableHint buildUserVariableHint(String text, Token token, SessionVariable sessionVariable) {
        text = trimWithSpace(text);

        Map<String, UserVariable> userVariables = Maps.newHashMap();
        if (text.startsWith("(") && text.endsWith(")")) {
            List<Expr> exprs;
            try {
                exprs = SqlParser.parseSqlToExprs(text.substring(1, text.length() - 1), sessionVariable);
            } catch (Exception e) {
                return null;
            }

            for (Expr expr : exprs) {
                if (!(expr instanceof BinaryPredicate)) {
                    return null;
                }

                BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
                if (binaryPredicate.getOp() != BinaryType.EQ) {
                    return null;
                }

                if (binaryPredicate.getChild(0) instanceof VariableExpr) {
                    VariableExpr variableExpr = (VariableExpr) binaryPredicate.getChild(0);
                    userVariables.put(variableExpr.getName(),
                            new UserVariable(variableExpr.getName(), binaryPredicate.getChild(1),
                                    true, binaryPredicate.getPos()));

                } else {
                    return null;
                }
            }

        } else {
            return null;
        }
        return new UserVariableHint(new NodePosition(token), userVariables, token.getText());
    }



    private static boolean isWhiteSpace(char c) {
        return c == ' ' || c == '\r' || c == '\n' || c == '\t' || c == '\u3000';
    }
}
