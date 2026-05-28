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

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TMatchExpr;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MatchExpr extends Expr {
    public MatchExpr(Expr e1, Expr e2) {
        this(e1, e2, NodePosition.ZERO);
    }

    public MatchExpr(MatchOperator matchOperator, Expr e1, Expr e2) {
        this(matchOperator, e1, e2, NodePosition.ZERO);
    }

    public MatchExpr(Expr e1, Expr e2, NodePosition position) {
        this(MatchOperator.MATCH, e1, e2, position);
    }

    public MatchExpr(MatchOperator operator, Expr e1, Expr e2, NodePosition pos) {
        this(operator, e1, e2, 0, pos);
    }

    public MatchExpr(MatchOperator operator, Expr e1, Expr e2, int slop, NodePosition pos) {
        super(pos);
        Preconditions.checkNotNull(operator);
        this.matchOperator = operator;
        this.slop = slop;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        setType(Type.BOOLEAN);
    }

    protected MatchExpr(MatchExpr other) {
        super(other);
        this.matchOperator = other.matchOperator;
        this.slop = other.slop;
        this.children.clear();
        for (Expr child : other.getChildren()) {
            Preconditions.checkNotNull(child);
            this.children.add(child.clone());
        }
        setType(Type.BOOLEAN);
    }

    public enum MatchOperator {
        MATCH("MATCH", TExprOpcode.MATCH),
        MATCH_ANY("MATCH_ANY", TExprOpcode.MATCH_ANY),
        MATCH_ALL("MATCH_ALL", TExprOpcode.MATCH_ALL),
        MATCH_PHRASE("MATCH_PHRASE", TExprOpcode.MATCH_PHRASE);

        private final String name;
        private final TExprOpcode opcode;

        MatchOperator(String name, TExprOpcode opCode) {
            this.name = name;
            this.opcode = opCode;
        }

        public String getName() {
            return name;
        }

        public TExprOpcode getOpcode() {
            return opcode;
        }
    }

    public MatchOperator getMatchOperator() {
        return matchOperator;
    }

    public int getSlop() {
        return slop;
    }

    private final MatchOperator matchOperator;
    private final int slop;

    @Override
    public Expr clone() {
        return new MatchExpr(this);
    }

    @Override
    public String toSqlImpl() {
        // For MATCH_PHRASE with non-zero slop, re-synthesize the "~N" suffix into the
        // pattern literal so that toSql() round-trips back to a parseable form.
        String left = getChild(0).toSql();
        String right = getChild(1).toSql();
        if (matchOperator == MatchOperator.MATCH_PHRASE && slop > 0 && getChild(1) instanceof StringLiteral) {
            String value = ((StringLiteral) getChild(1)).getValue();
            // Re-wrap with single quotes; embed slop as ~N suffix separated by space.
            String separator = value.isEmpty() ? "" : " ";
            right = "'" + escapeSingleQuotes(value) + separator + "~" + slop + "'";
        }
        return left + " " + matchOperator.getName() + " " + right;
    }

    private static String escapeSingleQuotes(String s) {
        return s.replace("'", "''");
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.MATCH_EXPR;
        msg.setOpcode(matchOperator.getOpcode());
        if (matchOperator == MatchOperator.MATCH_PHRASE && slop != 0) {
            msg.setMatch_expr(new TMatchExpr().setSlop(slop));
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), matchOperator, slop);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MatchExpr)) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        MatchExpr other = (MatchExpr) obj;
        return matchOperator == other.matchOperator && slop == other.slop;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitMatchExpr(this, context);
    }

    // ------------------------------------------------------------------
    // MATCH_PHRASE pattern parsing
    // ------------------------------------------------------------------

    /** Result of splitting a phrase pattern into (text, slop). */
    public static final class PhrasePattern {
        private final String text;
        private final int slop;

        public PhrasePattern(String text, int slop) {
            this.text = text;
            this.slop = slop;
        }

        public String getText() {
            return text;
        }

        public int getSlop() {
            return slop;
        }
    }

    /**
     * Maximum slop accepted by MATCH_PHRASE. CLucene's PhraseQuery accepts any
     * positive int, but huge slop values cause O(n*slop) blowup; cap to roughly
     * INT_MAX/2 to prevent silly misuses while leaving plenty of headroom.
     */
    private static final int MAX_SLOP = Integer.MAX_VALUE / 2;

    /**
     * Match pattern endings of the form "...&lt;ws&gt;~N" or "~N" at the start.
     * The leading "(?:^|\\s+)" allows either start-of-string or one-or-more
     * whitespace to precede the slop marker; this keeps "foo~3" (no space)
     * a literal while letting both "~3" and "foo ~3" carry slop.
     */
    private static final Pattern PHRASE_SLOP_PATTERN = Pattern.compile("(?:^|\\s+)~(\\d+)\\s*$");

    /**
     * Parse a raw MATCH_PHRASE pattern string into (actual text, slop). If no
     * trailing "~N" marker is present, returns the original text with slop=0.
     * Throws {@link SemanticException} when the slop integer is out of range.
     *
     * <p>Examples (see add-tantivy-fe-ddl-sql spec D8 for the full table):
     * <pre>
     *   "a b c"        -> text="a b c",   slop=0
     *   "a b c ~3"     -> text="a b c",   slop=3
     *   "a b c~3"      -> text="a b c~3", slop=0   (no whitespace before ~)
     *   "a b c ~"      -> text="a b c ~", slop=0   (~ without digits)
     *   "~3"           -> text="",        slop=3   (caller will reject empty text)
     *   "a b c ~3 ~5"  -> text="a b c ~3", slop=5  (only the trailing one wins)
     * </pre>
     */
    public static PhrasePattern parsePhrasePattern(String raw) {
        if (raw == null) {
            return new PhrasePattern("", 0);
        }
        Matcher m = PHRASE_SLOP_PATTERN.matcher(raw);
        if (!m.find()) {
            return new PhrasePattern(raw, 0);
        }
        long slopLong;
        try {
            slopLong = Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            // Should not happen — the regex only captures \d+. Keep defensively as no-op.
            return new PhrasePattern(raw, 0);
        }
        if (slopLong < 0 || slopLong > MAX_SLOP) {
            throw new SemanticException(
                    "MATCH_PHRASE slop out of range [0, " + MAX_SLOP + "]: " + m.group(1));
        }
        // m.start() is the start of the leading whitespace (or 0 for ~N at start).
        // Strip trailing whitespace from the kept text since the slop marker
        // ate the separating whitespace already.
        String text = raw.substring(0, m.start());
        // stripTrailing() not strictly needed since the regex consumed the
        // separator, but defensive against mixed whitespace classes.
        if (!text.isEmpty()) {
            text = stripTrailingWhitespace(text);
        }
        return new PhrasePattern(text, (int) slopLong);
    }

    private static String stripTrailingWhitespace(String s) {
        int end = s.length();
        while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(0, end);
    }
}
