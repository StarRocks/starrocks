package com.starrocks.common;

import com.starrocks.common.io.ParamsKey;

public enum NgramBfIndexParamsKey implements ParamsKey {
    /**
     * gram num, if gram num is 4, "apple" -> ["appl","pple"]
     */
    GRAM_NUM(String.valueOf(FeConstants.DEFAULT_GRAM_NUM), true),

    /**
     * bloom filter's false positive possibility
     */
    BLOOM_FILTER_FPP(String.valueOf(FeConstants.DEFAULT_BLOOM_FILTER_FPP), true);

    private final String defaultValue;
    private boolean needDefault = false;

    NgramBfIndexParamsKey(String defaultValue, boolean needDefault) {
        this.defaultValue = defaultValue;
        this.needDefault = needDefault;
    }

    @Override
    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean needDefault() {
        return needDefault;
    }
}
