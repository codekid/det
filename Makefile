.PHONY: install unhide test lint run-local dbt all clean airflow-up airflow-down airflow-logs airflow-ps

INTERVAL_START ?= 2026-08-06
INTERVAL_END ?=

# Absolute: this path is baked into the persisted bronze view, so a relative one
# would only resolve for clients whose working directory is dbt/.
export DET_LAKE_PATH ?= $(CURDIR)/data/lake

install:
	uv pip install -e ".[dev,dbt,mcp,postgres,iceberg]"
	@$(MAKE) --no-print-directory unhide

# Python 3.12+ silently ignores .pth files carrying the macOS UF_HIDDEN flag, which
# breaks the editable install with a bare "No module named 'det'" (also breaks det-mcp).
# Only clear flags on the editable .pth files — recursive chflags on .venv hits SIP/perms noise.
unhide:
	@command -v chflags >/dev/null && \
		find .venv/lib -name '__editable__*.pth' -exec chflags nohidden {} + 2>/dev/null || true

test:
	uv run pytest

lint:
	uv run ruff check .

# Same pipeline config as production, pointed at local fixtures via --set.
# thin cannot Iceberg — opt in to JSONL for this smoke path.
run-local:
	uv run det run \
		--pipeline noaa.storm_events \
		--interval-start $(INTERVAL_START) $(if $(INTERVAL_END),--interval-end $(INTERVAL_END)) \
		--set source.overrides.local_csv_dir=fixtures/storm_events \
		--set source.overrides.filename_substr=details \
		--set ingestion.library=thin \
		--set destination.type=filesystem

# dbt reads bronze in place; det dbt sets DET_LAKE_PATH.
dbt:
	uv run det dbt

all: run-local dbt

clean:
	rm -rf data dbt/target dbt/logs

# Local Airflow UI (LocalExecutor). http://localhost:8080 — airflow / airflow
airflow-up:
	@test -f airflow/.env || cp airflow/.env.example airflow/.env
	cd airflow && docker compose up -d --build

airflow-down:
	cd airflow && docker compose down

airflow-logs:
	cd airflow && docker compose logs -f

airflow-ps:
	cd airflow && docker compose ps
