.PHONY: setup run test clean

setup:
	python -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

run:
	. .venv/bin/activate && python -m src.pipeline.run_pipeline --start-date 2025-12-01 --days 7 --rows-per-day 5000

test:
	. .venv/bin/activate && pytest -q

clean:
	rm -rf data .pytest_cache
