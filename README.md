# Data Pipeline CI/CD

A complete data engineering pipeline with CI/CD implementation for learning purposes.

## 🚀 Features

- **ETL Pipeline**: Extract, Transform, Load data from CSV to database
- **Unit & Integration Tests**: Comprehensive test suite
- **CI/CD Pipeline**: GitHub Actions for automated testing
- **Docker Support**: Containerized deployment
- **Sample Data Generator**: Script to create test data
- **CLI Interface**: Easy command-line interface

## 📁 Project Structure
  
data-pipeline-ci-cd/  
├── src/  
│ └── data_pipeline/  
│ ├── init.py  
│ ├── pipeline.py # Main ETL pipeline  
│ └── cli.py # Command line interface  
├── tests/  
│ ├── unit/  
│ │ └── test_pipeline.py # Unit tests  
│ └── integration/ # Integration tests  
├── scripts/  
│ ├── generate_sample_data.py  
│ └── run_pipeline.py  
├── data/  
│ ├── raw/ # Input data  
│ └── processed/ # Output data  
├── .github/workflows/  
│ └── ci.yml # CI/CD pipeline  
├── docker/  
│ └── Dockerfile  
├── pyproject.toml  
├── requirements.txt  
└── README.md  