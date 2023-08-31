{% macro deletle_rows_failed_test(target_data,table,primary_key)%}
    {% set failed_test_table %}
        {{table}}_failed_tests
    {% endset %}
    {{log(failed_test_table)}}

    {% set table_exists = adapter.get_relation(
      database =  "TESTS" ,
      schema = "TASTYBYTES" ,
      identifier =  "stg_tastybytes_orderdetails_failed_tests" ) 
    %}

    {{log("primary_key:" ~ primary_key)}}
   
    {% if table_exists %}
        {{log("!!!! SAY YES TEST FAILED ROWS TABLE EXISTS")}}
        {% set query %}
            DELETE 
            FROM 
                {{target_data}} as t1
            USING TESTS.TASTYBYTES.{{failed_test_table}} as t2
            WHERE t1.{{primary_key}} = t2.{{primary_key}}
            
        {% endset %}

        {{log( "post hook query started running!!")}}
        {% do run_query(query) %}
    {% else %}
        {{log("table doesnt exist!!!")}}
    {% endif %}
{% endmacro %}