-- By default, custom schema name will be combined with target.schema name
-- Override this schema to based on requirements

{% macro generate_schema_name(custom_schema_name, node) -%}
{# By default, dbt generates a custom schema name with the combination of default and user defined schema name #}
{# In order to create a USER ONLY defined schema, we don't define a custom schema name, #}

{# Yet we define a variable on dbt_project.yml and on each dbt run command, we assign a value to a variable #}
{# As you can see below, we pass this variable as a default_schema name #}

    {#%- set default_schema = target.schema -%#}
    
    {%- if custom_schema_name is none -%}
        {{ var('schema_name') }}
        {{ log("Setting Default Schema: {0}".format(target.schema)) }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
        {{ log("Setting Custom Schema: {0}".format(custom_schema_name | trim)) }}
    {%- endif -%}

{%- endmacro %}