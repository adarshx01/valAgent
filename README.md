# ValAgent - Enterprise Data Validation Agent- --------------------     latest testetinga sdfansldfnaklsjdnlivarjsnligvsjrndfbstrhrs

<p align="center">
  <img src="https://img.shields.io/badge/python-3.12+-blue.svg" alt="Python 3.12+">
  <img src="https://img.shields.io/badge/FastAPI-0.109+-green.svg" alt="FastAPI">
  <img src="https://img.shields.io/badge/PostgreSQL-15+-blue.svg" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License">
</p>

**ValAgent** is an intelligent, enterprise-grade Data Validation Agent that automates ETL (Extract, Transform, Load) pipeline testing using natural language business rules. Quality Analysts can express validation requirements in plain English, and the AI agent automatically generates and executes comprehensive SQL tests against source and target databases.

## 🎯 Key Features

- **🗣️ Natural Language Rules**: Express validation rules in plain English - no SQL knowledge required
- **🤖 AI-Powered SQL Generation**: Automatically generates optimized SQL queries from business rules
- **📊 Comprehensive Testing**: Covers row counts, data accuracy, referential integrity, null checks, and more
- **📈 Detailed Reports**: Get proof of execution with sample data, statistics, and evidence
- **🔄 Schema Comparison**: Automatically compare source and target database structures
- **🌐 Modern Web Interface**: Beautiful, responsive UI for managing validations
- **📡 REST API**: Full API access for integration with CI/CD pipelines
- **🏢 Enterprise Ready**: Connection pooling, async execution, and scalable architecture

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL databases (source and target)
- OpenAI API key (or other supported LLM provider)

### Installation

```bash
# Clone the repository
git clone https://github.com/iqvia/valagent.git
cd valagent

# Install with uv (recommended)
uv sync

# Or install with pip
pip install -e .
```

### Configuration

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Edit `.env` with your settings:

```bash
# Database connections
SOURCE_DB_URI=postgresql://user:password@localhost:5432/source_db
TARGET_DB_URI=postgresql://user:password@localhost:5432/target_db

# LLM Configuration
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-your-api-key-here
OPENAI_MODEL=gpt-4o
```

### Running the Application

```bash
# Start the server
uv run valagent serve

# Or with uvicorn directly
uv run uvicorn valagent.api.app:app --reload

# Access the application
# Web UI: http://localhost:8000
# API Docs: http://localhost:8000/docs
```

## 📖 Usage

### Web Interface

1. Open http://localhost:8000 in your browser
2. Enter your business rules in natural language:
   ```
   All customer records from source should exist in target
   Total sales amount should match between source and target
   No duplicate customer IDs should exist in target
   ```
3. Click "Run Validation" and view detailed results

### API Usage

```python
import httpx

# Create and run a validation
response = httpx.post(
    "http://localhost:8000/api/v1/validations/quick",
    json={
        "name": "Customer Data Validation",
        "business_rules": [
            "All customers from source should exist in target",
            "Customer email addresses should not be null in target",
            "Total order count should match between databases"
        ]
    }
)

result = response.json()
print(f"Status: {result['status']}")
print(f"Passed: {result['passed_tests']}/{result['total_tests']}")
```

### Natural Language Queries

Ask questions about your data in plain English:

```python
response = httpx.post(
    "http://localhost:8000/api/v1/query/natural",
    json={
        "query": "How many customers are in each database? Show me the top 10 orders by amount."
    }
)
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Web Interface                             │
│                    (Alpine.js + Tailwind)                        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                         FastAPI Server                           │
│                    REST API + WebSocket                          │
└─────────────────────────────────────────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
┌───────────────────┐ ┌─────────────────┐ ┌─────────────────────┐
│  Validation Engine │ │   LLM Service   │ │  Report Generator   │
│                    │ │  (SQL Gen)      │ │  (HTML/JSON/MD)     │
└───────────────────┘ └─────────────────┘ └─────────────────────┘
            │                   │
            ▼                   ▼
┌───────────────────┐ ┌─────────────────┐
│  Database Manager  │ │  LLM Provider   │
│  (Async Pooling)   │ │  (OpenAI, etc.) │
└───────────────────┘ └─────────────────┘
            │
    ┌───────┴───────┐
    ▼               ▼
┌────────┐     ┌────────┐
│Source  │     │Target  │
│   DB   │     │   DB   │
└────────┘     └────────┘
```

## 📁 Project Structure

```
valagent/
├── src/valagent/
│   ├── api/               # FastAPI routes and schemas
│   │   ├── app.py         # Application factory
│   │   ├── routes.py      # API endpoints
│   │   └── schemas.py     # Pydantic models
│   ├── config/            # Configuration management
│   │   └── settings.py    # Pydantic settings
│   ├── database/          # Database layer
│   │   ├── connection.py  # Connection pooling
│   │   ├── models.py      # Data models
│   │   └── repository.py  # Persistence layer
│   ├── engine/            # Validation engine
│   │   ├── engine.py      # Main orchestrator
│   │   ├── executor.py    # Query execution
│   │   └── comparator.py  # Result comparison
│   ├── llm/               # LLM integration
│   │   ├── client.py      # Multi-provider client
│   │   ├── prompts.py     # Prompt templates
│   │   └── sql_generator.py # SQL generation
│   ├── reports/           # Report generation
│   │   ├── generator.py   # Multi-format reports
│   │   └── templates.py   # Report templates
│   └── static/            # Frontend assets
│       └── index.html     # Web UI
├── tests/                 # Test suite
├── .env.example           # Environment template
├── pyproject.toml         # Project configuration
└── README.md              # This file
```

## 🔧 Configuration Options

### Database Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `SOURCE_DB_URI` | PostgreSQL URI for source database | Required |
| `TARGET_DB_URI` | PostgreSQL URI for target database | Required |
| `DB_POOL_SIZE` | Connection pool size | 10 |
| `DB_MAX_OVERFLOW` | Max overflow connections | 20 |
| `QUERY_TIMEOUT` | Query timeout in seconds | 300 |
| `BATCH_SIZE` | Batch size for large queries | 10000 |

### LLM Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `LLM_PROVIDER` | Provider: openai, azure_openai, anthropic, ollama | openai |
| `OPENAI_API_KEY` | OpenAI API key | Required |
| `OPENAI_MODEL` | Model to use | gpt-4o |
| `LLM_TEMPERATURE` | Temperature for generation | 0.0 |
| `LLM_MAX_TOKENS` | Max tokens in response | 4096 |

### Application Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_ENV` | Environment: development, staging, production | development |
| `DEBUG` | Enable debug mode | false |
| `LOG_LEVEL` | Logging level | INFO |
| `API_PORT` | API server port | 8000 |
| `CORS_ORIGINS` | Allowed CORS origins | localhost |

## 🧪 Validation Types

ValAgent supports various validation types:

| Type | Description |
|------|-------------|
| **count** | Compare row counts between source and target |
| **data** | Compare actual data values row by row |
| **aggregation** | Compare sums, averages, and other aggregations |
| **referential** | Check foreign key relationships |
| **schema** | Compare table structures |
| **custom** | Custom SQL-based validations |

## 📊 Example Business Rules

```
# Row Count Validations
All customer records from source should exist in target
Total number of orders should match between databases

# Data Accuracy
Customer email addresses should be lowercase in target
Order totals should equal sum of line items

# Referential Integrity
All order customer_ids should reference valid customers
No orphan records in the orders table

# Data Quality
No null values in customer email field
No duplicate product SKUs in target

# Transformation Accuracy
Full name in target should be concatenation of first and last name from source
Order status should be mapped correctly (1=pending, 2=completed, 3=cancelled)
```

## 🛠️ Development

```bash
# Install dev dependencies
uv sync --group dev

# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/valagent

# Lint code
uv run ruff check src/

# Type check
uv run mypy src/

# Format code
uv run ruff format src/
```

## 📄 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health check |
| GET | `/api/v1/connections` | Test database connections |
| GET | `/api/v1/schema/{db}` | Get database schema |
| POST | `/api/v1/validations` | Create validation run |
| POST | `/api/v1/validations/quick` | Create and execute validation |
| POST | `/api/v1/validations/{id}/execute` | Execute validation |
| GET | `/api/v1/validations` | List validation runs |
| GET | `/api/v1/validations/{id}` | Get validation details |
| GET | `/api/v1/validations/stats` | Get statistics |
| POST | `/api/v1/query/natural` | Natural language query |
| POST | `/api/v1/tests/generate` | Auto-generate tests |

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [FastAPI](https://fastapi.tiangolo.com/)
- Powered by [OpenAI](https://openai.com/) / [Anthropic](https://anthropic.com/)
- Database connectivity via [SQLAlchemy](https://sqlalchemy.org/)

---

<p align="center">
  Made with ❤️ by IQVIA
</p>
