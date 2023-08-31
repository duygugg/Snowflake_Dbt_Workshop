{% macro load_failed_rows(target_data,table)%}

    {% set table_exists = adapter.get_relation(
      database =  "TESTS" ,
      schema = "TASTYBYTES" ,
      identifier =  "elementary_test_results"  ) 
    %}
  

    {% set create_table %}
        create or replace table TESTS.TASTYBYTES.{{table}}_FAILED_TESTS  (
            order_item_key                            VARCHAR(32)        ,
            order_key                                 NUMBER(38,0),
            order_detail_id                           NUMBER(38,0),
            discount_id                               VARCHAR(16777216),
            menu_item_key                             NUMBER(38,0),
            line_number                               NUMBER(38,0),
            quantity                                  NUMBER(5,0),
            order_item_discount_amount                NUMBER(38,4),
            unit_price                                VARCHAR(16777216),
            order_item_total_price                    NUMBER(38,4),   
            table_name                 VARCHAR(16777216),
            test_applied_column        VARCHAR(16777216),
            test_severtiy              VARCHAR(16777216)
    
        )
    {% endset %}

    {% do run_query(create_table) %}

    {% if table_exists %}
        {{log("!!!! SAY YES ELEMENTARY EXISTS")}}
        {% set tables_list = [] %}
        
            {%- for row in run_query(
                               "
                                SELECT
                                    distinct table_name
                                FROM TESTS.TASTYBYTES.elementary_test_results
                                ") 
            -%}
                {{ tables_list.append(row.values()[0]) if tables_list.append(row.values()[0]) is not none }}
                {{ log("   ROW VALUES" ~ row.values()[0]  ) }}

            {%- endfor -%}



        {% if table in tables_list %}
            {{ log("Our table exists on elementary_test_result: " ~ table) }}

            {% set test_list = [] %}
            {%- for  row in run_query( 
                                "
                                SELECT
                                    distinct test_short_name
                                FROM TESTS.TASTYBYTES.elementary_test_results
                                ") 
            -%}
                {{ test_list.append(row.values()[0]) if test_list.append(row.values()[0]) is not none }}
            {%- endfor -%}

            


            {% if 'unique' in test_list %}
                {{log("found the test !")}}
                {% set query_1 %}
                    select 
                        distinct column_name        
                    from tests.tastybytes.elementary_test_results 
                    where table_name = 'stg_tastybytes_orderdetails'       
                {% endset %}

                {% set column_compare = run_query(query_1).columns[0][0] %}

                {% set not_unique -%}
                    with cte as(
                    select distinct 
                        t1.*,
                        'stg_tastybytes_orderdetails' as table_name_t1,
                        t2.column_name as test_applied_column,
                        t2.severity as test_severtiy
                    from 
                        {{target_data}} as t1
                    left join 
                        tests.tastybytes.elementary_test_results as t2
                    on 
                        table_name_t1 = t2.table_name
                    )


                    select 
                        * 
                    from cte 
                    
                    where 
                        {{column_compare}}  in ( 
                            select 
                                {{column_compare}} 
                            from 
                                cte 
                            group by 
                                 {{column_compare}} 
                            HAVING 
                                COUNT(*) > 1
                        )
                {% endset %}
                {{log("this table :" ~ table ~ ", has this test that is applied to : " ~ unique)}}
                
                {% set insert_into_unique %}
                    insert into TESTS.TASTYBYTES.{{table}}_FAILED_TESTS
                    {{not_unique}}
                {% endset %}

                {% do run_query(insert_into_unique) %}
       
            {% endif %}

            {% set not_null -%}
            {% endset %}

        {% endif %}
    
    {% else %}
        {{ log("TEST ARE NOT APPLIED" ~ table  ) }}
    {% endif %}

    {{log("macro ended")}}

  

{% endmacro %}