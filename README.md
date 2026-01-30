# ETL Validation Agent 🤖

> **AI-powered Data Pipeline Validation System**

An intelligent agent for validating ETL (Extract, Transform & Load) jobs using natural language business rules. This enterprise-grade system automates the manual QA process by generating and executing comprehensive test cases against source and target databases.

## 🎯 Overview

The ETL Validation Agent transforms how data quality is validated:

| **Current Process (Manual)** | **Future Process (Automated)** |
|------------------------------|-------------------------------|
| QA manually writes test cases | QA provides business rules in plain English |
| Manual SQL query writing | Agent generates SQL automatically |
| Manual query execution | Parallel execution for large datasets |
| Screenshots as proof | Detailed reports with execution proofs |
| Time-consuming & error-prone | Fast, consistent & comprehensive |

## ✨ Features

- **🗣️ Natural Language Rules**: Define validation rules in plain English
- **🔍 Automatic Schema Analysis**: Extracts and analyzes source/target database schemas
- **🤖 AI-Powered Query Generation**: GPT-4 generates appropriate SQL test cases
- **⚡ Parallel Processing**: Handles lakhs of records with PostgreSQL parallel queries
- **📊 Comprehensive Reports**: Pass/fail results with proof of execution
- **💡 AI Analysis**: Intelligent root cause analysis and recommendations
- **🌐 REST API**: Full-featured API for integration
- **💻 CLI Interface**: Command-line tool for automation

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     ETL Validation Agent                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │  FastAPI    │  │    CLI      │  │   Validation Agent      │ │
│  │  REST API   │  │  Interface  │  │   (Orchestrator)        │ │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘ │
│         │                │                      │               │
│  ┌──────┴────────────────┴──────────────────────┴─────────────┐ │
│  │                    Service Layer                           │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │ │
│  │  │Schema Service│  │ LLM Service  │  │ Executor Service │ │ │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘ │ │
│  └──────────────────────────────────────────────────────────────┘ │
│         │                      │                      │         │
│  ┌──────┴──────────────────────┴──────────────────────┴───────┐ │
│  │                      Core Layer                            │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │ │
│  │  │   Database   │  │    Config    │  │   Exceptions     │ │ │
│  │  │   Manager    │  │   Settings   │  │                  │ │ │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘ │ │
│  └──────────────────────────────────────────────────────────────┘ │
│         │                                              │         │
│  ┌──────┴──────────────────────────────────────────────┴───────┐ │
│  │     Source DB (PostgreSQL)      Target DB (PostgreSQL)     │ │
│  └──────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL databases (source and target)
- OpenAI API key

### Installation

```bash
# Clone the repository
git clone https://github.com/iqvia/etl-validation-agent.git
cd etl-validation-agent

# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

### Configuration

Create a `.env` file:

```env
# Database connections
SOURCE_DB_URI="postgresql://user:password@host:5432/source_db"
TARGET_DB_URI="postgresql://user:password@host:5432/target_db"

# OpenAI configuration
LLM_PROVIDER="openai"
OPENAI_API_KEY="sk-your-api-key"
OPENAI_MODEL="gpt-4.1"

# Optional settings
LOG_LEVEL="INFO"
MAX_PARALLEL_WORKERS=8
BATCH_SIZE=10000
```

### Running the Agent

#### Option 1: API Server

```bash
# Start the FastAPI server
python main.py serve

# Or with uv
uv run python main.py serve
```

Access the API documentation at `http://localhost:8000/docs`

#### Option 2: CLI

```bash
# Run validation with business rules
etl-validator validate --text "
1. All customer records should exist in target
2. Email addresses should be lowercase
3. Order totals should match between source and target
"

# Or from a file
etl-validator validate --rules business_rules.txt --output report.md

# Execute ad-hoc query
etl-validator query "SELECT COUNT(*) FROM customers" --db target

# Generate SQL from description
etl-validator generate-sql "Count active customers created this month"
```

## 📝 Business Rules Examples

```text
1. Data Completeness:
   - All records from source.orders should exist in target.orders
   - No NULL values allowed in customer_email column

2. Data Transformation:
   - Email addresses must be lowercase in target
   - Phone numbers should be formatted as +1-XXX-XXX-XXXX
   - Dates should be converted to UTC timezone

3. Data Consistency:
   - Total order amounts should match: source vs target
   - Customer counts should be equal across databases
   
4. Referential Integrity:
   - All order.customer_id should exist in customers.id
   
5. Aggregation Rules:
   - Sum of daily_sales should match monthly_sales total
```

## 🔌 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/validate` | Run full validation |
| POST | `/api/v1/validate/stream` | Streaming validation with progress |
| POST | `/api/v1/validate/quick` | Quick single-rule validation |
| POST | `/api/v1/query/execute` | Execute ad-hoc SQL |
| POST | `/api/v1/query/generate` | Generate SQL from description |
| GET | `/api/v1/schema/source` | Get source schema |
| GET | `/api/v1/schema/target` | Get target schema |
| GET | `/api/v1/schema/compare` | Compare schemas |
| GET | `/api/v1/databases/info` | Get database info |
| GET | `/health` | Health check |

### API Example

```python
import requests

# Run validation
response = requests.post(
    "http://localhost:8000/api/v1/validate",
    json={
        "business_rules": """
        1. All customer records should exist in target
        2. Email addresses should be lowercase
        """,
        "validation_name": "Customer Data Validation"
    }
)

result = response.json()
print(f"Status: {result['report']['overall_status']}")
print(f"Passed: {result['report']['summary']['passed']}")
print(f"Failed: {result['report']['summary']['failed']}")
```

## 📊 Sample Output

```markdown
# ETL Validation Report

**Report ID:** abc123
**Generated:** 2026-01-30T10:30:00Z

## Overview

- **Source Database:** source
- **Target Database:** target
- **Overall Status:** PASSED

## Execution Summary

| Metric | Value |
|--------|-------|
| Total Tests | 15 |
| Passed | 14 |
| Failed | 1 |
| Pass Rate | 93.3% |
| Total Duration | 2450ms |

## Scenarios Covered

- ✅ **Row Count Validation**: Verified record counts match
- ✅ **Data Transformation**: Checked email lowercase conversion
- ✅ **Null Check**: Validated no NULL values in required fields
- ❌ **Referential Integrity**: Found orphan records

## AI Analysis

The validation completed with 93.3% pass rate. One test failed 
due to 3 orphan order records referencing non-existent customers.
This appears to be a data sync timing issue.

## Recommendations

1. Investigate orphan records in orders table
2. Add foreign key constraint to prevent future issues
3. Consider adding a reconciliation job
```

## 🛠️ Development

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
pytest

# Run with coverage
pytest --cov=src/etl_validator

# Type checking
mypy src/

# Linting
ruff check src/

# Formatting
black src/
```

## 📁 Project Structure

```
etl-validation-agent/
├── src/
│   └── etl_validator/
│       ├── __init__.py
│       ├── cli.py              # CLI interface
│       ├── core/
│       │   ├── config.py       # Configuration
│       │   ├── database.py     # Database manager
│       │   └── exceptions.py   # Custom exceptions
│       ├── models/
│       │   ├── schema.py       # Schema models
│       │   ├── rules.py        # Business rules
│       │   ├── test_case.py    # Test cases
│       │   └── results.py      # Results & reports
│       ├── services/
│       │   ├── llm_service.py  # AI/LLM integration
│       │   ├── schema_service.py
│       │   ├── executor_service.py
│       │   └── validation_orchestrator.py
│       ├── agents/
│       │   └── validation_agent.py
│       ├── api/
│       │   ├── app.py          # FastAPI app
│       │   ├── routes.py       # API routes
│       │   └── dependencies.py
│       └── utils/
│           ├── logger.py
│           └── helpers.py
├── tests/
├── main.py
├── pyproject.toml
├── .env
└── README.md
```

## ⚙️ Configuration Options

| Variable | Description | Default |
|----------|-------------|---------|
| `SOURCE_DB_URI` | Source PostgreSQL connection | Required |
| `TARGET_DB_URI` | Target PostgreSQL connection | Required |
| `OPENAI_API_KEY` | OpenAI API key | Required |
| `OPENAI_MODEL` | Model to use | gpt-4.1 |
| `MAX_PARALLEL_WORKERS` | Parallel query workers | 8 |
| `BATCH_SIZE` | Batch size for large data | 10000 |
| `QUERY_TIMEOUT` | Query timeout (seconds) | 300 |
| `LOG_LEVEL` | Logging level | INFO |

## 🔒 Security

- Database credentials stored as SecretStr (masked in logs)
- API rate limiting built-in
- Optional API key authentication
- CORS configuration for web clients

## 📈 Performance

- **Parallel Query Execution**: Configurable worker pool
- **Connection Pooling**: Efficient database connections
- **Batch Processing**: Handles large datasets in chunks
- **Async I/O**: Non-blocking operations throughout

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- OpenAI for GPT-4 API
- FastAPI for the excellent web framework
- asyncpg for high-performance PostgreSQL access

---

**Built with ❤️ by Adarsh**
