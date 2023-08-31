{% macro table_exists (database_name,schema_name,table_name) %}
    {% set table_exists = adapter.get_relation(
      database =  database_name ,
      schema = schema_name ,
      identifier =  table_name ) 
    %}
    {{log(database_name ~ ","  ~ schema_name ~ "," ~ table_name)}}
    {{log("table_exists?" ~ table_exists)}}
        {% if not table_exists %}
            {{log("table_exists not eixsts YALL!!")}}
            {{return(0)}}
        {% else %}
            {{log("table_exists YALL!!")}}
            {{return(1)}}
        {% endif %}

{% endmacro %}