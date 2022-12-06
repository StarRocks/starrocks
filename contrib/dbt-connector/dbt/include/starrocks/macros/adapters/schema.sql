/*
 * This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 */

{% macro starrocks__drop_schema(relation) -%}
  {% call statement('drop_schema') %}
    drop schema if exists {{ relation.without_identifier() }}
  {% endcall %}
{%- endmacro %}
