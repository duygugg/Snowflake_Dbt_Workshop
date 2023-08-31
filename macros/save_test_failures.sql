{% macro save_test_failures(results) %}

  {%- set test_results = [] -%}
  {%- for result in results -%}
    {%- if result.node.resource_type == 'test' and result.status == 'fail' and (
          result.node.config.get('store_failures') or flags.STORE_FAILURES
      )
    -%}
      {%- do test_results.append(result) -%}
    {%- endif -%}
  {%- endfor -%}
  
  {%- set central_tbl -%} {{ target.schema }}.test_failure_central {%- endset -%}
  
  {{ log("Centralizing test failures in " + central_tbl, info = true) if execute }}

  create or replace table {{ central_tbl }} (test_name STRING, test_failures_json JSON, test_ts timestamp) as (
  
  {% for result in test_results %}

    select
      '{{ result.node.name }}' as test_name,
      to_json(t) as test_failures_json,
      current_timestamp() as test_ts
      
    from {{ result.node.relation_name }} as t

    {{ "union all" if not loop.last }}
  {% endfor %}
  
  )

{% endmacro %}