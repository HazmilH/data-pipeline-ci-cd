#!/usr/bin/env python3
"""
Command Line Interface for Data Pipeline
"""
import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser(description="Data Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run command
    run_parser = subparsers.add_parser("run", help="Run ETL pipeline")
    run_parser.add_argument("--input", required=True, help="Input CSV file")
    run_parser.add_argument("--output", default="processed_data", help="Output table")
    run_parser.add_argument("--db-url", help="Database URL")

    # Test command
    test_parser = subparsers.add_parser("test", help="Run test pipeline")
    test_parser.add_argument("--db-url", help="Database URL")

    args = parser.parse_args()

    from data_pipeline import DataPipeline

    pipeline = DataPipeline(db_url=args.db_url)

    if args.command == "run":
        results = pipeline.run_pipeline(args.input, args.output)
        print(json.dumps(results, indent=2))
        sys.exit(0 if results["success"] else 1)

    elif args.command == "test":
        results = pipeline.run_test_pipeline()
        print(json.dumps(results, indent=2))
        sys.exit(0 if results["success"] else 1)


if __name__ == "__main__":
    main()
