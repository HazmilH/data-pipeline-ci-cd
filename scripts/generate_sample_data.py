#!/usr/bin/env python3
"""
Generate sample data for testing
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import argparse
from pathlib import Path


def generate_sample_data(num_rows: int = 1000, output_path: str = 'data/raw/sample_data.csv'):
    """Generate sample transaction data."""
    # Create directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Generate dates
    start_date = datetime.now() - timedelta(days=365)
    dates = pd.date_range(start=start_date, periods=num_rows, freq='D')
    
    # Create DataFrame
    df = pd.DataFrame({
        'transaction_date': dates,
        'transaction_id': range(100000, 100000 + num_rows),
        'customer_id': np.random.randint(1000, 9999, num_rows),
        'product_id': np.random.choice(['PROD001', 'PROD002', 'PROD003'], num_rows),
        'amount': np.random.uniform(10, 1000, num_rows).round(2),
        'quantity': np.random.randint(1, 10, num_rows),
        'region': np.random.choice(['North', 'South', 'East', 'West'], num_rows),
    })
    
    # Add some null values (10% chance)
    mask = np.random.random(num_rows) > 0.9
    df.loc[mask, 'amount'] = np.nan
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"✅ Generated {num_rows} rows")
    print(f"📁 Saved to: {output_path}")
    print(f"📊 Shape: {df.shape}")
    print(f"📈 Memory: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
    
    return df


def main():
    parser = argparse.ArgumentParser(description='Generate sample data')
    parser.add_argument('--rows', type=int, default=1000, help='Number of rows')
    parser.add_argument('--output', type=str, default='data/raw/sample_data.csv',
                       help='Output file path')
    
    args = parser.parse_args()
    generate_sample_data(args.rows, args.output)


if __name__ == "__main__":
    main()
