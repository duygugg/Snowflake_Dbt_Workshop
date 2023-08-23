{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}
        {% if  var('dbt_artifacts_database')  ==  "TESTS" %}

            {{ var('dbt_artifacts_database') }}

        {% else %}

            {{ default_database }}

        {% endif %}

    {%- else -%}

        {{ custom_database_name | trim }}

    {%- endif -%}

{%- endmacro %}
