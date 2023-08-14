/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https:*www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

{% macro starrocks__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) %}
    {%- if relation.is_materialized_view -%}
        drop materialized view if exists {{ relation }};
    {%- else -%}
        drop {{ relation.type }} if exists {{ relation }};
    {%- endif -%}
  {% endcall %}
{%- endmacro %}

{% macro starrocks__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') %}
    {% if from_relation.is_materialized_view and to_relation.is_materialized_view %}
        alter materialized view {{ from_relation }} rename {{ to_relation.table }}
    {%- elif from_relation.is_table and to_relation.is_table %}
       alter table {{ from_relation }} rename {{ to_relation.table }}
    {% elif from_relation.is_view and to_relation.is_view %}
      {% set results = run_query("select VIEW_DEFINITION as sql from information_schema.views where TABLE_SCHEMA='"
           + from_relation.schema + "' and TABLE_NAME='" + from_relation.table + "'") %}
      create view {{ to_relation }} as {{ results[0]['sql'] }}
      {% call statement('drop_view') %}
        drop view if exists {{ from_relation }}
      {% endcall %}
    {%- else -%}
      {%- set msg -%}
          unsupported rename from {{ from_relation.type }} to {{ to_relation.type }}
      {%- endset %}
      {{ exceptions.raise_compiler_error(msg) }}
    {% endif %}
  {% endcall %}
{%- endmacro %}
