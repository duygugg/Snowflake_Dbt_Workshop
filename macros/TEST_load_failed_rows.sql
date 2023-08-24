{% macro TEST_load_failed_rows(target_data,table)%}

    {% set table_exists = adapter.get_relation(
      database =  "TESTS" ,
      schema = "TASTYBYTES" ,
      identifier =  "elementary_test_results"  ) 
    %}
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
                {% set unique -%}
                    with column_name as (
                        select t2.column_name,t1.* 
                        from {{target_data}} as t1
                        left join tests.tastybytes.elementary_test_result as t2
                        on {{table}} = t2.table_name
                    )
                    ,target_counts as (
                        select column_name, count(*) as count
                        from column_name
                        group by column_name
                    ),
                    non_unique_values as (
                        select column_name
                        from target_counts
                        where count > 1
                    )
                    select * from {{ target_data }}
                    where column_name not in (select column_name from non_unique_values)
                {% endset %}
                {{log("this table :" ~ table ~ ", has this test that is applied to : " ~ unique)}}
       
            {% endif %}

            {% set not_null -%}
            {% endset %}


        {% endif %}
    
    {% else %}
        {{ log("TEST ARE NOT APPLIED" ~ table  ) }}
    {% endif %}

    {% if not_null | length %}
        {{log("test not_null not empty")}}

    {% else %}
        {{log("test not_null  empty")}}

    {% endif %}

{% endmacro %}