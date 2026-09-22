CREATE TABLE IF NOT EXISTS public.metadata_ingestion (
    id            BIGSERIAL NOT NULL,
    run_id        UUID NOT NULL,
    job_name      VARCHAR(100) NOT NULL,
    appname       VARCHAR(100) NOT NULL,
    status        VARCHAR(50) NOT NULL,
    service_name  VARCHAR(50) NOT NULL,

    success_count INT8 NOT NULL DEFAULT 0,
    error_count   INT8 NOT NULL DEFAULT 0,

    start_dttm    TIMESTAMP NULL,
    end_dttm      TIMESTAMP NULL,

    log_dttm      TIMESTAMP DEFAULT now() NOT NULL,

    CONSTRAINT metadata_ingestion_pk
        PRIMARY KEY (id, log_dttm),

    CONSTRAINT ck_metadata_ingestion_status
        CHECK (status IN ('QUEUE', 'RUNNING', 'DONE', 'FAILED', 'SKIPPED'))
)
PARTITION BY RANGE (log_dttm);

CREATE INDEX IF NOT EXISTS idx_metadata_ingestion_run
    ON public.metadata_ingestion (run_id);

CREATE INDEX IF NOT EXISTS idx_metadata_ingestion_run_job
    ON public.metadata_ingestion (run_id, job_name);

CREATE INDEX IF NOT EXISTS idx_metadata_ingestion_service
    ON public.metadata_ingestion (service_name, log_dttm);

CREATE TABLE IF NOT EXISTS public.metadata_ingestion_default
    PARTITION OF public.metadata_ingestion
    DEFAULT;