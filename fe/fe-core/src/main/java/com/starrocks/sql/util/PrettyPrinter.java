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

package com.starrocks.sql.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class PrettyPrinter {
    private static final AppendStep NL_APPEND_STEP = new AppendStep() {
        @Override
        public void advance(StringBuilder sb) {
            sb.append("\n");
        }
    };
    private static final ThreadLocal<StringBuilder> TLS_STRING_BUILDER = new ThreadLocal<>();
    private static final int STRING_BUILDER_CAPACITY = 128 * 1024;
    private static final String SPACES = Strings.repeat(" ", 512);
    private static final AppendStep[] NSPACES_ARRAY = new AppendStep[128];
    private static final Map<Character, String> ESCAPE_MAP =
            ImmutableMap.<Character, String>builder()
                    .put('\\', "\\\\")
                    .put('\'', "\\'")
                    .put('\"', "\\\"")
                    .put('\n', "\\n")
                    .put('\r', "\\r")
                    .put('\t', "\\t")
                    .put('\f', "\\f")
                    .put('\b', "\\b")
                    .build();
    private static final String[] ESCAPE_TABLE;
    public static int DEFAULT_INDENT_SHIFT = 2;

    static {
        for (int i = 0; i < NSPACES_ARRAY.length; ++i) {
            NSPACES_ARRAY[i] = nspaces(i);
        }
    }

    static {
        int tableSz = ESCAPE_MAP.keySet().stream().map(Integer::valueOf).max(Integer::compareTo).get() + 1;
        Preconditions.checkArgument(tableSz <= 128);
        ESCAPE_TABLE = new String[tableSz];
        for (int i = 0; i < tableSz; ++i) {
            Character ch = (char) i;
            ESCAPE_TABLE[i] = ESCAPE_MAP.getOrDefault(ch, ch.toString());
        }
    }

    private final Stack<Integer> indentStack;
    boolean hasIndented = false;
    private TieredList.Builder<AppendStep> stepBuilder = TieredList.newGenesisTier();
    private int indentSize = 0;
    private transient TieredList<AppendStep> steps = null;
    private transient String result = null;

    public PrettyPrinter() {
        this.indentStack = new Stack<>();
    }

    private PrettyPrinter(PrettyPrinter that) {
        Preconditions.checkArgument(that.stepBuilder.isSealed());
        this.stepBuilder = that.getSteps().newTier();
        this.indentStack = that.indentStack;
        this.indentSize = that.indentSize;
        this.hasIndented = that.hasIndented;
    }

    private static AppendStep nspaces(int n) {
        Preconditions.checkArgument(n < SPACES.length());
        return sb -> {
            sb.append(SPACES, 0, n);
        };
    }

    private static String escape(String s, char quote, char otherQuote) {
        if (StringUtils.isEmpty(s)) {
            return "" + quote + quote;
        }
        PrettyPrinter printer = new PrettyPrinter();
        int snippetStart = 0;
        int snippetEnd = 0;
        printer.add(quote);
        while (snippetEnd < s.length()) {
            char c = s.charAt(snippetEnd);
            if (c < ESCAPE_TABLE.length && c != otherQuote && (c == '\\' || c == quote || Character.isISOControl(c))) {
                printer.add(s.substring(snippetStart, snippetEnd));
                printer.add(ESCAPE_TABLE[c]);
                snippetStart = snippetEnd + 1;
                snippetEnd = snippetStart;
            } else {
                ++snippetEnd;
            }
        }
        printer.add(s.substring(snippetStart, snippetEnd));
        printer.add(quote);
        return printer.getResult();
    }

    public static PrettyPrinter escapedDoubleQuoted(String s) {
        return new PrettyPrinter().addEscapedDoubleQuoted(s);
    }

    public static PrettyPrinter escapedSingleQuoted(String s) {
        return new PrettyPrinter().addEscapedSingleQuoted(s);
    }

    private StringBuilder getStringBuilder() {
        if (TLS_STRING_BUILDER.get() == null) {
            TLS_STRING_BUILDER.set(new StringBuilder(STRING_BUILDER_CAPACITY));
        }
        return TLS_STRING_BUILDER.get();
    }

    private TieredList<AppendStep> getSteps() {
        if (steps == null || !stepBuilder.isSealed()) {
            steps = stepBuilder.build();
            stepBuilder.seal();
        }
        return steps;
    }

    public String getResult() {
        if (result == null || !stepBuilder.isSealed()) {
            StringBuilder sb = getStringBuilder();
            int start = sb.length();
            getSteps().forEach(step -> step.advance(sb));
            String s = sb.substring(start);
            if (start == 0) {
                if (sb.capacity() > STRING_BUILDER_CAPACITY) {
                    sb.setLength(0);
                    sb.trimToSize();
                    sb.ensureCapacity(STRING_BUILDER_CAPACITY);
                } else {
                    sb.setLength(0);
                }
            } else {
                sb.setLength(start);
            }
            result = s;
        }
        return result;
    }

    public PrettyPrinter newPrettyPrinter() {
        return new PrettyPrinter(this);
    }

    @CanIgnoreReturnValue
    public PrettyPrinter pushIndent() {
        indentStack.push(DEFAULT_INDENT_SHIFT);
        indentSize += DEFAULT_INDENT_SHIFT;
        return this;
    }

    @CanIgnoreReturnValue
    public PrettyPrinter pushIndent(int indentShift) {
        indentStack.push(indentShift);
        indentSize += indentShift;
        return this;
    }

    @CanIgnoreReturnValue
    public PrettyPrinter indentEnclose(Action action) {
        pushIndent();
        action.perform();
        return popIndent();
    }

    @CanIgnoreReturnValue
    public PrettyPrinter indentEnclose(Function<PrettyPrinter, PrettyPrinter> func) {
        return func.apply(pushIndent()).popIndent();
    }

    @CanIgnoreReturnValue
    public PrettyPrinter indentEnclose(int n, Action action) {
        pushIndent(n);
        action.perform();
        return popIndent();
    }

    public PrettyPrinter popIndent() {
        if (indentStack.isEmpty()) {
            Preconditions.checkArgument(indentSize == 0);
            return this;
        }
        int indentShift = indentStack.pop();
        indentSize -= indentShift;
        Preconditions.checkArgument(indentSize >= 0);
        return this;
    }

    @CanIgnoreReturnValue
    private PrettyPrinter indent() {
        if (indentSize <= 0) {
            return this;
        }
        stepBuilder.add(spacesWithoutIndent(indentSize));
        return this;
    }

    private AppendStep spacesWithoutIndent(int n) {
        if (n < NSPACES_ARRAY.length) {
            return NSPACES_ARRAY[n];
        }
        return sb -> {
            int num = n;
            while (num >= SPACES.length()) {
                sb.append(SPACES);
                num -= SPACES.length();
            }
            final int numRestSpaces = num;
            sb.append(SPACES, 0, numRestSpaces);
        };
    }

    public PrettyPrinter newLine() {
        stepBuilder.add(NL_APPEND_STEP);
        hasIndented = false;
        return this;
    }

    private PrettyPrinter addNewStepExceptNewLine(AppendStep step) {
        if (!hasIndented) {
            hasIndented = true;
            indent();
        }
        stepBuilder.add(step);
        return this;
    }

    public <T> PrettyPrinter add(T s) {
        return addNewStepExceptNewLine(sb -> sb.append(s));
    }

    public <T> PrettyPrinter addDoubleQuoted(T s) {
        return add("\"").add(s).add("\"");
    }

    public <T> PrettyPrinter addSingleQuoted(T s) {
        return add("'").add(s).add("'");
    }

    public <T> PrettyPrinter addBacktickQuoted(T s) {
        return add("`").add(s).add("`");
    }

    private <T> PrettyPrinter addItemsImpl(String delimiter, List<T> items,
                                           BiFunction<PrettyPrinter, T, PrettyPrinter> printItem) {
        if (items.isEmpty()) {
            return this;
        }
        printItem.apply(this, items.get(0));
        items.stream().skip(1).forEach(item -> printItem.apply(this.add(delimiter), item));
        return this;
    }

    public <T> PrettyPrinter addItems(String delimiter, List<T> items) {
        return addItemsImpl(delimiter, items, PrettyPrinter::add);
    }

    public <T> PrettyPrinter addItemsBacktickQuoted(String delimiter, List<T> items) {
        return addItemsImpl(delimiter, items, PrettyPrinter::addBacktickQuoted);
    }

    public <T> PrettyPrinter addItemsWithNlDel(String postNlDelimiter, List<T> items) {
        return addItemsWithNl(null, postNlDelimiter, items);
    }

    public <T> PrettyPrinter addItemsWithDelNl(String preNlDelimiter, List<T> items) {
        return addItemsWithNl(preNlDelimiter, null, items);
    }

    public <T> PrettyPrinter addItemsWithNl(String preNlDelimiter, String postNlDelimiter, List<T> items) {
        if (items.isEmpty()) {
            return this;
        }
        add(items.get(0));
        items.stream().skip(1).forEach(item -> {
            if (preNlDelimiter != null && !preNlDelimiter.isEmpty()) {
                this.add(preNlDelimiter);
            }
            this.newLine();
            if (postNlDelimiter != null && !postNlDelimiter.isEmpty()) {
                this.add(postNlDelimiter);
            }
            this.add(item);
        });
        return this;
    }

    @CanIgnoreReturnValue
    @SafeVarargs
    public final <T> PrettyPrinter addItems(String delimiter, T... items) {
        return addItems(delimiter, Arrays.asList(items));
    }

    @CanIgnoreReturnValue
    @SafeVarargs
    public final <T> PrettyPrinter addItemsWithNlDel(String delimiter, T... items) {
        return addItemsWithNlDel(delimiter, Arrays.asList(items));
    }

    public final PrettyPrinter spaces(int n) {
        return addNewStepExceptNewLine(spacesWithoutIndent(n));
    }

    public PrettyPrinter addSuperStepWithIndent(PrettyPrinter nestedPrinter) {
        return addSuperStepWithIndentImpl(nestedPrinter, true);
    }

    private PrettyPrinter addSuperStepWithIndentImpl(PrettyPrinter nestedPrinter, boolean indentAtFirst) {

        AppendStep indentStep = spacesWithoutIndent(indentSize);
        TieredList<AppendStep> nestedSteps = nestedPrinter.getSteps();
        if (nestedSteps.isEmpty()) {
            return this;
        }
        if (indentAtFirst) {
            this.stepBuilder.add(indentStep);
        }
        final int[] numSteps = new int[] {nestedSteps.size()};
        Supplier<Integer> countDown = () -> --numSteps[0];
        // TieredList.get costs more time than TieredList.iterator.
        for (AppendStep step : nestedSteps) {
            boolean isLastStep = countDown.get() == 0;
            this.stepBuilder.add(step);
            if (step == NL_APPEND_STEP && !isLastStep) {
                this.stepBuilder.add(indentStep);
            }
        }
        hasIndented = true;
        return this;
    }

    @CanIgnoreReturnValue
    public PrettyPrinter addSuperStep(PrettyPrinter nestedPrinter) {
        TieredList<AppendStep> steps = this.stepBuilder.build();
        this.stepBuilder = steps.concat(nestedPrinter.getSteps()).newTier();
        return this;
    }

    @CanIgnoreReturnValue
    public PrettyPrinter addSuperSteps(String delimiter, List<PrettyPrinter> printers) {
        return addSuperStepsImpl(printers, p -> p.add(delimiter));
    }

    @CanIgnoreReturnValue
    public PrettyPrinter addSuperStepsWithDelNl(String delimiter, List<PrettyPrinter> printers) {
        return addSuperStepsImpl(printers, p -> p.add(delimiter).newLine());
    }

    @CanIgnoreReturnValue
    public PrettyPrinter addSuperStepsWithNlDel(String delimiter, List<PrettyPrinter> printers) {
        return addSuperStepsImpl(printers, p -> p.newLine().add(delimiter));
    }

    @CanIgnoreReturnValue
    private PrettyPrinter addSuperStepsImpl(List<PrettyPrinter> printers, Consumer<PrettyPrinter> separator) {
        this.addSuperStepWithIndent(printers.get(0));
        printers.stream().skip(1).forEach(p -> {
            PrettyPrinter p1 = new PrettyPrinter();
            separator.accept(p1);
            p1.addSuperStep(p);
            this.addSuperStepWithIndentImpl(p1, false);
        });
        return this;
    }

    public <T> PrettyPrinter addNameToArray(String name, List<T> items) {
        this.add(name).add(": ");
        if (items.isEmpty()) {
            this.add("[]");
        } else {
            this.add("[").newLine().indentEnclose(() -> {
                this.addItemsWithDelNl(",", items);
            }).newLine().add("]");
        }
        return this;
    }

    public PrettyPrinter addNameToSuperStepArray(String name, List<PrettyPrinter> items) {
        this.add(name).add(": ");
        if (items.isEmpty()) {
            this.add("[]");
        } else {
            this.add("[").newLine().indentEnclose(() -> {
                this.addSuperStepsWithDelNl(",", items);
            }).newLine().add("]");
        }
        return this;
    }

    public PrettyPrinter addNameToObject(String name, PrettyPrinter object) {
        return this.add(name).add(": ").addSuperStepWithIndent(object);
    }

    public PrettyPrinter addObject(List<PrettyPrinter> items) {
        return this.add("{").newLine().indentEnclose(() -> {
            this.addSuperStepsWithDelNl(",", items);
        }).newLine().add("}");
    }

    public <T> PrettyPrinter addKeyValue(String name, T value) {
        return this.add(name).add(": ").add(value);
    }

    public PrettyPrinter addEscapedDoubleQuoted(String s) {
        return add(escape(s, '"', '\''));
    }

    public PrettyPrinter addEscapedSingleQuoted(String s) {
        return add(escape(s, '\'', '"'));
    }

    public interface AppendStep {
        void advance(StringBuilder sb);
    }

    public interface Action {
        void perform();
    }
}
