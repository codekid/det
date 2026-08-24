{% macro det_dedupe_latest_run(
    relation,
    partition_by=["__row_hash"],
    order_by=["__extract_run_datetime desc"]
) %}
{#
  Keep one row per identity key. Defaults match DET's append-only bronze:
  latest __extract_run_datetime wins per __row_hash.

  `partition_by` / `order_by` are SQL fragments (column names or "col desc").
  `relation` is a CTE name or relation (e.g. base, or ref('stg_x')).
#}
select * {% if target.name == 'bigquery' %}except (__rn){% else %}exclude (__rn){% endif %}
from (
    select
        *,
        row_number() over (
            partition by {{ partition_by | join(", ") }}
            order by {{ order_by | join(", ") }}
        ) as __rn
    from {{ relation }}
) __deduped
where __rn = 1
{% endmacro %}
