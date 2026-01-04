"""
Main Data Pipeline Implementation
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataPipeline:
    """Simple ETL pipeline for CI/CD demonstration."""

    def __init__(self, db_url: Optional[str] = None):
        """
        Initialize pipeline.

        Args:
            db_url: Database URL (default: sqlite:///:memory:)
        """
        self.db_url = db_url or os.getenv("DATABASE_URL", "sqlite:///:memory:")
        self.engine = create_engine(self.db_url)
        logger.info(f"Initialized pipeline with: {self.db_url}")

    def extract(self, file_path: str) -> pd.DataFrame:
        """Extract data from CSV file."""
        logger.info(f"Extracting from: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            df = pd.read_csv(file_path)
            logger.info(f"Extracted {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean data."""
        logger.info(f"Transforming {len(df)} rows")

        # Clean column names
        df = df.copy()
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Handle missing values
        if "amount" in df.columns:
            df["amount"] = df["amount"].fillna(df["amount"].median())

        # Add calculated columns
        if "amount" in df.columns and "quantity" in df.columns:
            df["total_value"] = df["amount"] * df["quantity"]

        # Add metadata
        df["processed_at"] = datetime.now()

        logger.info("Transformation complete")
        return df

    def load(self, df: pd.DataFrame, table_name: str) -> bool:
        """Load data to database."""
        logger.info(f"Loading to table: {table_name}")

        try:
            df.to_sql(table_name, self.engine, if_exists="replace", index=False)
            logger.info(f"Loaded {len(df)} rows")
            return True
        except Exception as e:
            logger.error(f"Load failed: {e}")
            return False

    def run_pipeline(
        self, input_file: str, output_table: str = "processed_data"
    ) -> Dict[str, Any]:
        """Run complete ETL pipeline."""
        start_time = datetime.now()

        try:
            # Extract
            df_raw = self.extract(input_file)

            # Transform
            df_transformed = self.transform(df_raw)

            # Load
            success = self.load(df_transformed, output_table)

            # Validate
            if success:
                is_valid, message = self.validate_output(output_table)
            else:
                is_valid, message = False, "Load failed"

            return {
                "success": is_valid,
                "input_file": input_file,
                "output_table": output_table,
                "rows_processed": len(df_raw),
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
                "message": message,
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

    def validate_output(self, table_name: str) -> Tuple[bool, str]:
        """Validate pipeline output."""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = pd.read_sql(query, self.engine)
            count = result.iloc[0]["count"]

            if count > 0:
                return True, f"Validation passed: {count} rows"
            else:
                return False, "Validation failed: No data"
        except Exception as e:
            return False, f"Validation error: {e}"

    def run_test_pipeline(self) -> Dict[str, Any]:
        """Run test pipeline (for CI/CD)."""
        import tempfile

        # Create test data
        test_data = pd.DataFrame(
            {
                "customer_id": [1, 2, 3, 4, 5],
                "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
                "quantity": [2, 1, 3, 2, 1],
                "product": ["A", "B", "A", "C", "B"],
            }
        )

        # Save to temp file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            test_data.to_csv(f.name, index=False)
            temp_file = f.name

        try:
            return self.run_pipeline(temp_file, "test_output")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


if __name__ == "__main__":
    # Example usage
    pipeline = DataPipeline()
    results = pipeline.run_test_pipeline()
    print(json.dumps(results, indent=2))
