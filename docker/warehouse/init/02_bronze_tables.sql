-- One API record = one row. Raw payload in `data`, untouched.
-- Loads are idempotent per audit_logical_date (delete + insert in one transaction).

create table if not exists bronze.products (
    id                        integer     not null,
    data                      jsonb       not null,
    audit_event_timestamp     timestamptz,
    audit_ingestion_timestamp timestamptz not null,
    audit_logical_date        date        not null,
    audit_process_name        text        not null,
    primary key (id, audit_logical_date)
);

create table if not exists bronze.carts (
    id                        integer     not null,
    data                      jsonb       not null,
    audit_event_timestamp     timestamptz,
    audit_ingestion_timestamp timestamptz not null,
    audit_logical_date        date        not null,
    audit_process_name        text        not null,
    primary key (id, audit_logical_date)
);

-- Idempotent reload: delete by audit_logical_date.
create index if not exists products_audit_logical_date_idx on bronze.products (audit_logical_date);
create index if not exists carts_audit_logical_date_idx on bronze.carts (audit_logical_date);

-- dbt incremental watermark: silver reads only rows ingested after its latest ingested_at.
-- (silver.products is a full table rebuild, so bronze.products doesn't need it.)
create index if not exists carts_audit_ingestion_timestamp_idx on bronze.carts (audit_ingestion_timestamp);
