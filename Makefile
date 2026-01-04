.PHONY: help install test lint format clean run generate

help:
	@echo "Available commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make test       - Run tests"
	@echo "  make lint       - Run linting"
	@echo "  make format     - Format code"
	@echo "  make clean      - Clean temporary files"
	@echo "  make run        - Run pipeline"
	@echo "  make generate   - Generate sample data"

install:
	uv pip install -e ".[dev]"

test:
	pytest tests/ -v --cov=src --cov-report=html

lint:
	black --check src tests
	ruff check src tests
	mypy src

format:
	black src tests
	ruff check src tests --fix

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name ".coverage" -delete
	rm -rf .pytest_cache htmlcov .mypy_cache .ruff_cache build dist *.egg-info

run:
	python scripts/run_pipeline.py

generate:
	python scripts/generate_sample_data.py --rows 1000

ci: install lint test
