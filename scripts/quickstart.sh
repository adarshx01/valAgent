#!/bin/bash
# ===========================================
# ETL Validation Agent - Quick Start Script
# ===========================================

set -e

echo "🚀 ETL Validation Agent - Quick Start"
echo "======================================"

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed. Please install it first:"
    echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
uv sync

# Check .env file
if [ ! -f .env ]; then
    echo "⚠️  No .env file found. Please create one with your database credentials."
    exit 1
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "Available commands:"
echo "-------------------"
echo ""
echo "🌐 Start API Server:"
echo "   uv run python main.py serve"
echo "   Then visit: http://localhost:8000/docs"
echo ""
echo "💻 CLI Validation:"
echo "   uv run etl-validator validate --text 'Your business rules here'"
echo ""
echo "🔍 Execute Query:"
echo "   uv run etl-validator query 'SELECT COUNT(*) FROM table_name' --db target"
echo ""
echo "📊 View Schema:"
echo "   uv run etl-validator schema source"
echo "   uv run etl-validator schema target"
echo ""
echo "🧪 Run Tests:"
echo "   uv run pytest tests/ -v"
echo ""
