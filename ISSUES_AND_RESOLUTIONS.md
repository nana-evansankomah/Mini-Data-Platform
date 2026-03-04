# Codebase Review: Issues Encountered and Resolutions
Date: 2026-03-03

## Scope
Reviewed and fixed application code, DAG/runtime behavior, CI workflows, tests, and local run configuration.

## Issues and Fixes

1. Retry logging crashed on retry paths
- Area: `airflow/plugins/etl/*.py`, `airflow/plugins/utils/*.py`
- Symptom: Tenacity retry hooks raised `TypeError: level must be an integer`.
- Root cause: `before_sleep_log(logger, "WARNING")` used a string instead of logging level constant.
- Resolution: Replaced all string levels with `logging.WARNING` and imported `logging` where needed.

2. Unit test for loader failed despite valid logic
- Area: `tests/unit/test_load.py`
- Symptom: Mocked DB cursor failed inside `psycopg2.extras.execute_values`.
- Root cause: Test mocked connection but not `execute_values`, so psycopg2 internals expected real cursor attributes.
- Resolution: Patched `etl.load.execute_values` in the test and asserted it is called.

3. Lint failures from unused imports
- Area: `airflow/plugins/utils/pg_client.py`, unit tests
- Symptom: `ruff` reported multiple `F401` errors.
- Resolution: Removed unused imports.

4. CI data-flow job was unreliable/flaky
- Area: `.github/workflows/ci.yml` (`data-flow-validation`)
- Symptom: Job uploaded data and only slept 120s, but DAG schedule is every 15 minutes.
- Root cause: Workflow relied on scheduler timing rather than explicit trigger.
- Resolution: Added explicit DAG trigger via Airflow API and polling until completion; added `requests` dependency in that job.

5. Airflow admin user was not being created
- Area: `docker-compose.yml` (`airflow-init` entrypoint)
- Symptom: Airflow API returned forbidden/failed behavior; `airflow users list` showed no users.
- Root cause: Multiline `airflow users create` command was split into separate shell commands.
- Resolution: Rewrote `airflow-init` entrypoint to an argument-safe `bash -c` command where user creation runs correctly.

6. Airflow stable REST API returned `403` for basic auth
- Area: `docker-compose.yml` (`x-airflow-common` environment)
- Symptom: API calls to `/api/v1/dags/...` returned `403`.
- Root cause: API auth backend for basic auth was not explicitly configured.
- Resolution: Added `AIRFLOW__API__AUTH_BACKENDS: airflow.api.auth.backend.basic_auth`.

7. Summary refresh SQL failed at runtime (`date = text`)
- Area: `airflow/plugins/etl/load.py`
- Symptom: `refresh_summary` failed with PostgreSQL type error.
- Root cause: Date filters were converted to strings and compared directly to `DATE` columns.
- Resolution: Normalized dates to Python `date` objects and used `ANY(%s::date[])` casts in SQL.

8. Process task retried with stale file keys and caused noisy re-failures
- Area: `airflow/dags/sales_etl_dag.py`
- Symptom: After failure/quarantine, retries attempted downloads for moved keys (`NoSuchKey`).
- Root cause: Airflow task-level retries on `process_files` conflicted with file movement side effects.
- Resolution: Set `retries=0` for `process_files` operator (step-level retry remains handled by Tenacity in ETL modules).

9. Integration test raced Airflow startup
- Area: `tests/integration/test_end_to_end.py`
- Symptom: Connection aborted/timeouts when triggering DAG right after upload.
- Resolution: Added `wait_for_airflow()` health polling before DAG trigger.

10. Integration assertions were too broad and could read unrelated runs
- Area: `tests/integration/test_end_to_end.py`
- Symptom: Audit assertion could pick unrelated latest row.
- Resolution: Scoped assertions to the exact `dag_run_id` and `file_key`; fact-table check now filters by `source_file`.

11. Local docker startup failed when host ports were already occupied
- Area: `docker-compose.yml`, `.env.example`, `README.md`
- Symptom: `docker compose up` failed when `5432` (or other service ports) were in use.
- Resolution: Made host ports configurable via env vars:
  - `POSTGRES_HOST_PORT`
  - `MINIO_API_PORT`
  - `MINIO_CONSOLE_PORT`
  - `AIRFLOW_HOST_PORT`
  - `METABASE_HOST_PORT`
- Added variables to `.env.example` and documented in `README.md`.

12. Pytest integration marker not registered
- Area: `pytest.ini`
- Symptom: `PytestUnknownMarkWarning` for `@pytest.mark.integration`.
- Resolution: Added `pytest.ini` marker registration.

13. Repeated local pytest cache permission warnings
- Area: `pytest.ini`, `.gitignore`
- Symptom: cache directory warnings in this environment.
- Resolution: Disabled cache plugin via `addopts = -p no:cacheprovider` and ignored pytest cache artifacts in `.gitignore`.

## Validation Performed

1. Lint
- Command: `.venv\Scripts\python -m ruff check airflow scripts tests`
- Result: Pass

2. Unit tests
- Command: `.venv\Scripts\python -m pytest tests\unit -q`
- Result: `17 passed`

3. Integration test
- Command: `pytest tests/integration/test_end_to_end.py -q` (with env overrides for non-conflicting local ports)
- Result: `1 passed`

## Files Updated

- `.github/workflows/ci.yml`
- `docker-compose.yml`
- `.env.example`
- `README.md`
- `.gitignore`
- `airflow/dags/sales_etl_dag.py`
- `airflow/plugins/etl/audit.py`
- `airflow/plugins/etl/extract.py`
- `airflow/plugins/etl/file_mover.py`
- `airflow/plugins/etl/load.py`
- `airflow/plugins/utils/minio_client.py`
- `airflow/plugins/utils/pg_client.py`
- `tests/unit/test_load.py`
- `tests/unit/test_transform.py`
- `tests/unit/test_validate.py`
- `tests/integration/test_end_to_end.py`
- `pytest.ini`
