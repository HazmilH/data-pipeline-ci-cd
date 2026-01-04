#!/bin/bash
set -e

echo "===================================="
echo "Data Pipeline CI/CD - Complete Setup"
echo "===================================="

# 1. Install dependencies
echo "📦 Installing dependencies..."
uv pip install -e ".[dev]"

# 2. Run tests
echo "🧪 Running tests..."
uv run pytest tests/unit/ -v

# 3. Generate sample data
echo "📊 Generating sample data..."
# uv run python scripts/generate_sample_data.py --rows 100
uv run scripts/generate_sample_data.py --rows 100

# 4. Run pipeline
echo "⚙️  Running pipeline..."
# uv run python scripts/run_pipeline.py
uv run scripts/run_pipeline.py

# 5. Run CI tests
echo "🔄 Running CI simulation..."
uv run python -m data_pipeline.cli test

echo "===================================="
echo "✅ All tasks completed successfully!"
echo "===================================="
