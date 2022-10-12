// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.common;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;

import java.util.concurrent.TimeUnit;

public class NoopSpan implements Span {
    @Override
    public <T> Span setAttribute(AttributeKey<T> key, T value) {
        return this;
    }

    @Override
    public Span addEvent(String name, Attributes attributes) {
        return this;
    }

    @Override
    public Span addEvent(String name, Attributes attributes, long timestamp, TimeUnit unit) {
        return this;
    }

    @Override
    public Span setStatus(StatusCode statusCode, String description) {
        return this;
    }

    @Override
    public Span recordException(Throwable exception, Attributes additionalAttributes) {
        return this;
    }

    @Override
    public Span updateName(String name) {
        return this;
    }

    @Override
    public void end() {
    }

    @Override
    public void end(long timestamp, TimeUnit unit) {

    }

    @Override
    public SpanContext getSpanContext() {
        Context parentContext = Context.current();
        return Span.fromContext(parentContext).getSpanContext();
    }

    @Override
    public boolean isRecording() {
        return false;
    }
}
