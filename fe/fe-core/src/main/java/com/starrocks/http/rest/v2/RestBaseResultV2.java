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

package com.starrocks.http.rest.v2;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.http.rest.RestBaseResult;

import java.util.List;
import java.util.Objects;

public class RestBaseResultV2<T> extends RestBaseResult {

    private static final Gson GSON = new GsonBuilder()
            .setExclusionStrategies(new ExclusionStrategy() {
                @Override
                public boolean shouldSkipField(FieldAttributes f) {
                    return f.getAnnotation(Legacy.class) != null;
                }

                @Override
                public boolean shouldSkipClass(Class<?> clazz) {
                    return false;
                }
            }).create();

    @SerializedName("result")
    private T result;

    public RestBaseResultV2() {
    }

    public RestBaseResultV2(String msg) {
        super(msg);
    }

    public static <R> RestBaseResultV2<R> ok(R result) {
        RestBaseResultV2<R> obj = new RestBaseResultV2<>();
        obj.result = result;
        return obj;
    }

    public RestBaseResultV2(T result) {
        super();
        this.result = result;
    }

    public RestBaseResultV2(int code, String message) {
        this(Objects.toString(code), message);
    }

    public RestBaseResultV2(String code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String toJson() {
        return GSON.toJson(this);
    }

    public T getResult() {
        return result;
    }

    /**
     * Paged result.
     */
    public static class PagedResult<T> {

        @SerializedName("pageNum")
        private Integer pageNum;

        @SerializedName("pageSize")
        private Integer pageSize;

        /**
         * Total pages.
         */
        @SerializedName("pages")
        private Integer pages;

        /**
         * Total elements.
         */
        @SerializedName("total")
        private Integer total;

        @SerializedName("items")
        private List<T> items;

        public PagedResult() {
        }

        public Integer getPageNum() {
            return pageNum;
        }

        public void setPageNum(Integer pageNum) {
            this.pageNum = pageNum;
        }

        public Integer getPageSize() {
            return pageSize;
        }

        public void setPageSize(Integer pageSize) {
            this.pageSize = pageSize;
        }

        public Integer getPages() {
            return pages;
        }

        public void setPages(Integer pages) {
            this.pages = pages;
        }

        public Integer getTotal() {
            return total;
        }

        public void setTotal(Integer total) {
            this.total = total;
        }

        public List<T> getItems() {
            return items;
        }

        public void setItems(List<T> items) {
            this.items = items;
        }
    }
}
