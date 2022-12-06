/*
 * This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 */

{% macro starrocks__current_timestamp() -%}
  current_timestamp()
{%- endmacro %}
