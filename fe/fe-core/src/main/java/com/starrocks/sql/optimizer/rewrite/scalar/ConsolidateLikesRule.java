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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableSet;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsolidateLikesRule extends OnlyOnceScalarOperatorRewriteRule {

    public static final ConsolidateLikesRule INSTANCE = new ConsolidateLikesRule();
    private static final Set<Character> REGEX_META_CHARS = "^$.*+?|(){}[]".chars()
            .mapToObj(c -> (char) c).collect(ImmutableSet.toImmutableSet());
    private static final char LIKE_META_DASH = '_';
    private static final char LIKE_META_PERCENT = '%';
    private static final char BACKSLASH = '\\';
    private static final String REGEX_META_DOT = ".";
    private static final String REGEX_META_DOT_ASTERISK = ".*";
    private static final char C_ESCAPED_NL = '\n';
    private static final char C_ESCAPED_CR = '\r';
    private static final char C_ESCAPED_TAB = '\t';
    private static final String C_ESCAPED_NL_S = "\\n";
    private static final String C_ESCAPED_CR_S = "\\r";
    private static final String C_ESCAPED_TAB_S = "\\t";
    // Hyperscan rejects regular expression patterns larger than 16,000 UTF-8 bytes
    // (Grey::limitPatternLength in hyperscan's grey.cpp); exceeding it falls back to a much
    // slower RE2 path. Bail out to raw LIKE predicates instead of building an oversized pattern.
    private static final int HYPERSCAN_MAX_PATTERN_BYTES = 16_000;

    private ConsolidateLikesRule() {
    }

    private static boolean isPrintableChar(char c) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of(c);
        return (!Character.isISOControl(c)) &&
                c != KeyEvent.CHAR_UNDEFINED &&
                block != null &&
                block != Character.UnicodeBlock.SPECIALS;
    }

    private static Optional<String> toRegexPattern(String likePattern) {
        // '' like '' return 1;
        // '' regexp '' return 0 in SR, report Illegal regex expression in MySQL,
        // so we convert like pattern '' to regex '^$'
        if (likePattern.isEmpty()) {
            return Optional.of("^$");
        }
        char[] chars = likePattern.toCharArray();
        char lastChar = chars[chars.length - 1];
        String prefix = likePattern.substring(0, likePattern.length() - 1);
        // column like "prefix%" has performance regression, so do not optimize it.
        if ((lastChar == LIKE_META_DASH || lastChar == LIKE_META_PERCENT) &&
                prefix.chars().noneMatch(ch -> ch == LIKE_META_DASH || ch == LIKE_META_PERCENT)) {
            return Optional.empty();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < chars.length; ++i) {
            char ch = chars[i];
            if (ch == BACKSLASH) {
                sb.append(ch);
                ++i;
                if (i < chars.length) {
                    char ch1 = chars[i];
                    if (ch1 == BACKSLASH || ch1 == LIKE_META_DASH || ch1 == LIKE_META_PERCENT) {
                        sb.append(chars[i]);
                    } else {
                        // TODO(by satanson): \c may be a regex meta char, it is error-prone, so
                        //  do not convert it into regex.
                        return Optional.empty();
                    }
                } else {
                    sb.append(BACKSLASH);
                }
            } else if (ch == LIKE_META_DASH) {
                sb.append(REGEX_META_DOT);
            } else if (ch == LIKE_META_PERCENT) {
                sb.append(REGEX_META_DOT_ASTERISK);
            } else if (ch == C_ESCAPED_NL) {
                sb.append(C_ESCAPED_NL_S);
            } else if (ch == C_ESCAPED_CR) {
                sb.append(C_ESCAPED_CR_S);
            } else if (ch == C_ESCAPED_TAB) {
                sb.append(C_ESCAPED_TAB_S);
            } else if (REGEX_META_CHARS.contains(ch)) {
                sb.append(BACKSLASH).append(ch);
            } else if (isPrintableChar(ch)) {
                sb.append(ch);
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(sb.toString());
    }

    private static Optional<List<ScalarOperator>> consolidate(List<LikePredicateOperator> likeOps, int consolidateMin) {
        if (likeOps.size() < consolidateMin) {
            return Optional.empty();
        }
        ColumnRefOperator columnRef = Utils.mustCast(likeOps.get(0).getChild(0), ColumnRefOperator.class);
        List<Pair<LikePredicateOperator, Optional<String>>> likeAndRegexLists =
                likeOps.stream()
                        .map(likeOp -> Pair.create(likeOp,
                                Utils.mustCast(likeOp.getChild(1), ConstantOperator.class).getVarchar()))
                        .map(p -> Pair.create(p.first, toRegexPattern(p.second)))
                        .collect(Collectors.toList());

        Map<Boolean, List<Pair<LikePredicateOperator, Optional<String>>>> likeAndRegexGroups =
                likeAndRegexLists.stream().collect(Collectors.partitioningBy(p -> p.second.isPresent()));

        List<Pair<LikePredicateOperator, Optional<String>>> regexList = likeAndRegexGroups.get(true);
        if (regexList.size() < consolidateMin) {
            return Optional.empty();
        }
        String regexp = regexList.stream()
                .map(p -> Objects.requireNonNull(p.second.orElse(null)))
                .map(re -> "(" + re + ")")
                .collect(Collectors.joining("|"));

        String anchoredRegexp = "^(" + regexp + ")$";
        if (Utf8.encodedLength(anchoredRegexp) > HYPERSCAN_MAX_PATTERN_BYTES) {
            return Optional.empty();
        }

        ConstantOperator regexPattern = ConstantOperator.createVarchar(anchoredRegexp);

        ScalarOperator regexOp =
                new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, columnRef, regexPattern);
        List<ScalarOperator> newDisjuncts = Stream.concat(
                Stream.of(regexOp),
                likeAndRegexGroups.get(false).stream().map(p -> p.first)
        ).collect(Collectors.toList());
        return Optional.of(newDisjuncts);
    }

    private static Optional<List<ScalarOperator>> handleDisjuncts(
            List<ScalarOperator> disjuncts, int consolidateMin) {
        if (disjuncts.size() < consolidateMin) {
            return Optional.empty();
        }
        if (disjuncts.stream().filter(ScalarOperatorUtil::isSimpleLike).count() < consolidateMin) {
            return Optional.empty();
        }

        Map<Boolean, List<ScalarOperator>> disjunctGroups = disjuncts.stream()
                .collect(Collectors.partitioningBy(ScalarOperatorUtil::isSimpleLike));
        List<ScalarOperator> likeOps = disjunctGroups.get(true);
        List<ScalarOperator> otherDisjuncts = disjunctGroups.get(false);

        Map<ColumnRefOperator, List<LikePredicateOperator>> likeOpGroups = likeOps.stream()
                .map(disj -> Utils.mustCast(disj, LikePredicateOperator.class))
                .collect(Collectors.groupingBy(likeOp ->
                        Utils.mustCast(likeOp.getChild(0), ColumnRefOperator.class)));

        List<Pair<List<LikePredicateOperator>, Optional<List<ScalarOperator>>>> consolidatedDisjuncts =
                likeOpGroups.values()
                        .stream()
                        .map(likePredicateOperators -> Pair.create(
                                likePredicateOperators,
                                consolidate(likePredicateOperators, consolidateMin)))
                        .collect(Collectors.toList());
        if (consolidatedDisjuncts.stream().noneMatch(p -> p.second.isPresent())) {
            return Optional.empty();
        }

        List<ScalarOperator> newDisjuncts = Stream.concat(
                        otherDisjuncts.stream(),
                        consolidatedDisjuncts
                                .stream()
                                .map(p -> p.second.orElseGet(() -> new ArrayList<ScalarOperator>(p.first)))
                                .flatMap(Collection::stream))
                .collect(Collectors.toList());
        return Optional.of(newDisjuncts);
    }

    private static Optional<List<ScalarOperator>> handleConjuncts(
            List<ScalarOperator> conjuncts, int consolidateMin) {
        if (conjuncts.size() < consolidateMin) {
            return Optional.empty();
        }
        if (conjuncts.stream().filter(ScalarOperatorUtil::isSimpleNotLike).count() < consolidateMin) {
            return Optional.empty();
        }

        Map<Boolean, List<ScalarOperator>> conjunctGroups = conjuncts.stream()
                .collect(Collectors.partitioningBy(ScalarOperatorUtil::isSimpleNotLike));
        List<ScalarOperator> notLikeOps = conjunctGroups.get(true);
        List<ScalarOperator> otherConjuncts = conjunctGroups.get(false);

        Map<ColumnRefOperator, List<LikePredicateOperator>> likeOpGroups = notLikeOps.stream()
                .map(conj -> Utils.mustCast(conj, CompoundPredicateOperator.class).getChild(0))
                .map(likeOp -> Utils.mustCast(likeOp, LikePredicateOperator.class))
                .collect(Collectors.groupingBy(likeOp ->
                        Utils.mustCast(likeOp.getChild(0), ColumnRefOperator.class)));

        List<Pair<List<ScalarOperator>, Optional<List<ScalarOperator>>>> consolidatedConjuncts =
                likeOpGroups.values()
                        .stream()
                        .map(likePredicateOperators -> Pair.create(
                                likePredicateOperators.stream().map(CompoundPredicateOperator::not)
                                        .collect(Collectors.toList()),
                                consolidate(likePredicateOperators, consolidateMin)
                                        .map(ops -> ops.stream().map(CompoundPredicateOperator::not)
                                                .collect(Collectors.toList()))))
                        .collect(Collectors.toList());

        if (consolidatedConjuncts.stream().noneMatch(p -> p.second.isPresent())) {
            return Optional.empty();
        }

        List<ScalarOperator> newConjuncts = Stream.concat(
                        otherConjuncts.stream(),
                        consolidatedConjuncts
                                .stream()
                                .map(p -> p.second.orElseGet(() -> new ArrayList<ScalarOperator>(p.first)))
                                .flatMap(Collection::stream))
                .collect(Collectors.toList());
        return Optional.of(newConjuncts);
    }

    @Override
    public ScalarOperator apply(ScalarOperator root, ScalarOperatorRewriteContext context) {
        int consolidateMin = Optional.ofNullable(ConnectContext.get())
                .map(ConnectContext::getSessionVariable)
                .map(SessionVariable::getLikePredicateConsolidateMin)
                .orElse(0);
        if (root == null || consolidateMin < 1) {
            return root;
        }
        return rewrite(root, consolidateMin, context);
    }

    /**
     * Consolidate each maximal OR/AND group exactly once. The scalar rewriter's normal
     * top-down traversal would visit a group rejected for exceeding hyperscan's pattern-length
     * limit and then its smaller same-connective children, consolidating a partial subgroup
     * instead of leaving the whole rejected group as plain LIKE predicates.
     */
    private static ScalarOperator rewrite(ScalarOperator operator, int consolidateMin,
                                          ScalarOperatorRewriteContext context) {
        if (!(operator instanceof CompoundPredicateOperator)) {
            return rewriteChildren(operator, consolidateMin, context);
        }

        CompoundPredicateOperator predicate = (CompoundPredicateOperator) operator;
        if (predicate.isOr()) {
            List<ScalarOperator> disjuncts = Utils.extractDisjunctive(predicate);
            return rewriteDisjuncts(predicate, disjuncts, consolidateMin, context);
        } else if (predicate.isAnd()) {
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
            return rewriteConjuncts(predicate, conjuncts, consolidateMin, context);
        }
        return rewriteChildren(predicate, consolidateMin, context);
    }

    private static ScalarOperator rewriteDisjuncts(CompoundPredicateOperator predicate,
                                                    List<ScalarOperator> disjuncts,
                                                    int consolidateMin,
                                                    ScalarOperatorRewriteContext context) {
        return handleDisjuncts(disjuncts, consolidateMin)
                .map(ops -> rewriteConsolidatedGroup(changed(Utils.compoundOr(ops), context), true,
                        consolidateMin, context))
                .orElseGet(() -> rewriteSameCompoundChildren(predicate, true, consolidateMin,
                        context));
    }

    private static ScalarOperator rewriteConjuncts(CompoundPredicateOperator predicate,
                                                    List<ScalarOperator> conjuncts,
                                                    int consolidateMin,
                                                    ScalarOperatorRewriteContext context) {
        return handleConjuncts(conjuncts, consolidateMin)
                .map(ops -> rewriteConsolidatedGroup(changed(Utils.compoundAnd(ops), context), false,
                        consolidateMin, context))
                .orElseGet(() -> rewriteSameCompoundChildren(predicate, false, consolidateMin,
                        context));
    }

    private static ScalarOperator rewriteChildren(ScalarOperator operator, int consolidateMin,
                                                   ScalarOperatorRewriteContext context) {
        for (int i = 0; i < operator.getChildren().size(); i++) {
            ScalarOperator child = operator.getChild(i);
            ScalarOperator rewrittenChild = rewrite(child, consolidateMin, context);
            if (rewrittenChild != child) {
                operator.setChild(i, rewrittenChild);
                context.change();
            }
        }
        return operator;
    }

    private static ScalarOperator rewriteConsolidatedGroup(ScalarOperator operator, boolean isOr,
                                                            int consolidateMin,
                                                            ScalarOperatorRewriteContext context) {
        if (operator instanceof CompoundPredicateOperator && isSameCompoundType(
                (CompoundPredicateOperator) operator, isOr)) {
            return rewriteSameCompoundChildren((CompoundPredicateOperator) operator, isOr,
                    consolidateMin, context);
        }
        return rewrite(operator, consolidateMin, context);
    }

    private static ScalarOperator rewriteSameCompoundChildren(CompoundPredicateOperator predicate, boolean isOr,
                                                               int consolidateMin,
                                                               ScalarOperatorRewriteContext context) {
        for (int i = 0; i < predicate.getChildren().size(); i++) {
            ScalarOperator child = predicate.getChild(i);
            ScalarOperator rewrittenChild;
            if (child instanceof CompoundPredicateOperator &&
                    isSameCompoundType((CompoundPredicateOperator) child, isOr)) {
                rewrittenChild = rewriteSameCompoundChildren((CompoundPredicateOperator) child, isOr,
                        consolidateMin, context);
            } else {
                rewrittenChild = rewrite(child, consolidateMin, context);
            }
            if (rewrittenChild != child) {
                predicate.setChild(i, rewrittenChild);
                context.change();
            }
        }
        return predicate;
    }

    private static boolean isSameCompoundType(CompoundPredicateOperator predicate, boolean isOr) {
        return isOr ? predicate.isOr() : predicate.isAnd();
    }

    private static ScalarOperator changed(ScalarOperator operator, ScalarOperatorRewriteContext context) {
        context.change();
        return operator;
    }
}