"""
Example: Using the REST API with Python requests.

This example shows how to interact with the ETL Validation Agent API.
"""

import requests
import json

# Base URL - change if running on different host/port
BASE_URL = "http://localhost:8000"


def check_health():
    """Check if the API is running."""
    response = requests.get(f"{BASE_URL}/health")
    print("Health Check:", response.json())
    return response.ok


def get_database_info():
    """Get information about connected databases."""
    response = requests.get(f"{BASE_URL}/api/v1/databases/info")
    if response.ok:
        data = response.json()
        print("\nDatabase Information:")
        print(json.dumps(data, indent=2))
    return response.json()


def get_source_schema():
    """Get source database schema."""
    response = requests.get(f"{BASE_URL}/api/v1/schema/source")
    if response.ok:
        data = response.json()
        print(f"\nSource Schema: {data['schema']['tables']} tables")
    return response.json()


def run_validation(business_rules: str, name: str = "API Validation"):
    """Run ETL validation with business rules."""
    response = requests.post(
        f"{BASE_URL}/api/v1/validate",
        json={
            "business_rules": business_rules,
            "validation_name": name,
        },
    )
    
    if response.ok:
        data = response.json()
        report = data["report"]
        
        print("\n" + "=" * 50)
        print("VALIDATION RESULTS")
        print("=" * 50)
        print(f"Status: {report['overall_status']}")
        print(f"Passed: {report['summary']['passed']}/{report['summary']['total_tests']}")
        print(f"Pass Rate: {report['summary']['pass_rate']:.1f}%")
        print(f"Duration: {report['summary']['duration_ms']:.0f}ms")
        print("=" * 50)
        
        return data
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None


def execute_query(sql: str, database: str = "target"):
    """Execute an ad-hoc SQL query."""
    response = requests.post(
        f"{BASE_URL}/api/v1/query/execute",
        json={
            "sql": sql,
            "database": database,
        },
    )
    
    if response.ok:
        data = response.json()
        print(f"\nQuery Results ({data['row_count']} rows):")
        for row in data.get("data", [])[:5]:
            print(f"  {row}")
        return data
    else:
        print(f"Error: {response.text}")
        return None


def generate_sql(description: str, database: str = "target"):
    """Generate SQL from natural language description."""
    response = requests.post(
        f"{BASE_URL}/api/v1/query/generate",
        json={
            "description": description,
            "database": database,
        },
    )
    
    if response.ok:
        data = response.json()
        print(f"\nGenerated SQL:")
        print(data["sql"])
        return data["sql"]
    else:
        print(f"Error: {response.text}")
        return None


def main():
    """Main example flow."""
    print("ETL Validation Agent - API Example")
    print("=" * 50)
    
    # Check health
    if not check_health():
        print("API is not running. Start it with: python main.py serve")
        return
    
    # Get database info
    get_database_info()
    
    # Get source schema
    get_source_schema()
    
    # Generate SQL
    print("\n--- Generating SQL ---")
    generate_sql("Count all records in the customers table")
    
    # Execute query
    print("\n--- Executing Query ---")
    execute_query("SELECT 1 as test")
    
    # Run validation
    print("\n--- Running Validation ---")
    business_rules = """
    1. All customer records should exist in target
    2. Email addresses should be lowercase
    3. Row counts should match between source and target
    """
    run_validation(business_rules, "API Example Validation")


if __name__ == "__main__":
    main()
