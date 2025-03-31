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

public class ConsolidateLikesRule extends TopDownScalarOperatorRewriteRule {

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

        ConstantOperator regexPattern = ConstantOperator.createVarchar("^(" + regexp + ")$");

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
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        int consolidateMin = Optional.ofNullable(ConnectContext.get())
                .map(ConnectContext::getSessionVariable)
                .map(SessionVariable::getLikePredicateConsolidateMin)
                .orElse(0);
        if (consolidateMin < 1) {
            return predicate;
        }
        if (predicate.isOr()) {
            List<ScalarOperator> disjuncts = Utils.extractDisjunctive(predicate);
            return handleDisjuncts(disjuncts, consolidateMin).map(Utils::compoundOr).orElse(predicate);
        } else if (predicate.isAnd()) {
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
            return handleConjuncts(conjuncts, consolidateMin).map(Utils::compoundAnd).orElse(predicate);
        } else {
            return predicate;
        }
    }
}