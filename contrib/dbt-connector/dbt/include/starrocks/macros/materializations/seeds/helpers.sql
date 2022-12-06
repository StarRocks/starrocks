/*
 * This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 */

{% macro starrocks__create_csv_table(model, agate_table) -%}
  {% set column_override = model['config'].get('column_types', {}) %}
  {% set quote_seed_column = model['config'].get('quote_columns', None) %}
  {% set engine = config.get('engine', 'OLAP') %}

  {% set sql %}
    create table {{ this.render() }}
    (
        {% for col_name in agate_table.column_names %}
        {% set inferred_type = adapter.convert_type(agate_table, loop.index0) %}
        {% set type = column_override.get(col_name, inferred_type) %}
        {% set column_name = (col_name | string) %}
        {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
    {% if engine == 'OLAP' %}
      {{ starrocks__olap_table(False) }}
    {% else %}
      {{ starrocks__other_table() }}
    {% endif %}
  {% endset %}

  {% call statement('_') %}
    {{ sql }}
  {% endcall %}

  {{ return(sql) }}

{%- endmacro %}
