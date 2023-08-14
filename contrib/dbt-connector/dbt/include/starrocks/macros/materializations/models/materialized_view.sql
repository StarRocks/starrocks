{% macro starrocks__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ starrocks__get_drop_relation_sql(existing_relation) }}
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}

{% macro starrocks__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }};
{%- endmacro %}

{% macro starrocks__get_create_materialized_view_as_sql(relation, sql) %}
    create materialized view {{ relation }} refresh manual
    as
    {{ sql }};
{% endmacro %}

{% macro starrocks__get_drop_relation_sql(relation) %}
    {% call statement(name="main") %}
        {%- if relation.is_materialized_view -%}
            {{ starrocks__drop_materialized_view(relation) }}
        {%- else -%}
            drop {{ relation.type }} if exists {{ relation }};
        {%- endif -%}
    {% endcall %}
{% endmacro %}

{% macro starrocks__get_materialized_view_configuration_changes(existing_relation, new_config) %}
{% endmacro %}

{% macro starrocks__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}

    {{ get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) }}

{% endmacro %}