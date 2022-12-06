/*
 * This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 */

{% macro starrocks__snapshot_hash_arguments(args) -%}
    md5(concat_ws('|', {%- for arg in args -%}
        coalesce(cast({{ arg }} as char), '')
        {% if not loop.last %}, {% endif %}
    {%- endfor -%}))
{%- endmacro %}
