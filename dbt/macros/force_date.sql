{#-
    Targeted reprocessing: `--vars '{"force_date": "YYYY-MM-DD"}'` makes the incremental models of
    carts, cart_items and product_daily_revenue rebuild exactly that day instead of reading the
    ingestion watermark. Without the var, nothing changes.
-#}

{% macro force_date() %}
    {{ return(var('force_date', none)) }}
{% endmacro %}


{#- Watermark filter, or the forced day when force_date is set. Used inside is_incremental(). -#}
{% macro incremental_filter(date_column, ts_column) %}
    {% if force_date() %}
    {{ date_column }} = '{{ force_date() }}'::date
    {% else %}
    {{ ts_column }} > (
        select coalesce(max(ingested_at), '-infinity'::timestamptz) from {{ this }}
    )
    {% endif %}
{% endmacro %}


{#-
    on-run-start hook: fails the whole invocation before any model runs when force_date is set but
    malformed, combined with --full-refresh, or absent from bronze. No silent no-ops.
-#}
{% macro validate_force_date() %}
    {% set value = force_date() %}
    {% if value is none or not execute %}
        {{ return('') }}
    {% endif %}

    {#- The format check also keeps the value safe to interpolate in SQL. An impossible date
        (e.g. 2026-02-30) is rejected by Postgres in the existence query below, still before any
        model runs. -#}
    {% if not modules.re.fullmatch('\d{4}-\d{2}-\d{2}', value | string) %}
        {{ exceptions.raise_compiler_error("force_date must be YYYY-MM-DD, got '" ~ value ~ "'") }}
    {% endif %}

    {% if flags.FULL_REFRESH %}
        {{ exceptions.raise_compiler_error("force_date can't be combined with --full-refresh") }}
    {% endif %}

    {% set found = run_query(
        "select count(*) from " ~ source('bronze', 'carts') ~ " where audit_logical_date = '" ~ value ~ "'::date"
    ).columns[0].values()[0] %}
    {% if found == 0 %}
        {{ exceptions.raise_compiler_error("force_date " ~ value ~ " has no data in bronze.carts") }}
    {% endif %}

    {{ log("force_date=" ~ value ~ ": reprocessing that day only (" ~ found ~ " carts in bronze)", info=True) }}
    {{ return('') }}
{% endmacro %}
