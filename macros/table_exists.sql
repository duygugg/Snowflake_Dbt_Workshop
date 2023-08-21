{% macro table_exists (database_name,schema_name,table_name) %}
    {% set table_exists = adapter.get_relation(
      database =  database_name ,
      schema = schema_name ,
      identifier =  table_name ) 
    %}
        {% if not table_exists %}
            0
        {% else %}
            1
        {% endif %}


{% endmacro %}