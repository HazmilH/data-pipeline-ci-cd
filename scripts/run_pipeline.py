#!/usr/bin/env python3
"""
Run the data pipeline
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from data_pipeline import DataPipeline


def main():
    """Run the pipeline with sample data."""
    print("🚀 Starting Data Pipeline...")
    
    # Generate data if not exists
    data_file = 'data/raw/sample_data.csv'
    if not Path(data_file).exists():
        print("📝 Generating sample data...")
        import subprocess
        subprocess.run(['python', 'scripts/generate_sample_data.py', '--rows', '100'])
    
    # Run pipeline
    print("⚙️  Running ETL pipeline...")
    pipeline = DataPipeline(db_url='sqlite:///data/pipeline.db')
    results = pipeline.run_pipeline(data_file, 'processed_transactions')
    
    # Print results
    print("\n" + "="*50)
    print("PIPELINE RESULTS")
    print("="*50)
    print(f"Status: {'✅ SUCCESS' if results['success'] else '❌ FAILED'}")
    print(f"Input: {results.get('input_file', 'N/A')}")
    print(f"Output: {results.get('output_table', 'N/A')}")
    print(f"Rows: {results.get('rows_processed', 0):,}")
    print(f"Duration: {results.get('duration_seconds', 0):.2f} seconds")
    
    if 'message' in results:
        print(f"Message: {results['message']}")
    
    if 'error' in results:
        print(f"Error: {results['error']}")
    
    print("="*50)
    
    return 0 if results['success'] else 1


if __name__ == "__main__":
    sys.exit(main())
