"""
Example usage of ETL Validation Agent.

This script demonstrates how to use the validation agent programmatically.
"""

import asyncio
from src.etl_validator.agents.validation_agent import validation_agent


async def main():
    """Run example validation."""
    print("=" * 60)
    print("ETL Validation Agent - Example Usage")
    print("=" * 60)
    print()

    # Example business rules
    business_rules = """
    Data Validation Rules for Customer ETL Pipeline:
    
    1. DATA COMPLETENESS:
       - All customer records from the source database should exist in the target database
       - No NULL values should exist in the email and name columns
    
    2. DATA TRANSFORMATION:
       - Email addresses must be stored in lowercase format in the target
       - Phone numbers should be formatted consistently
       - All timestamps should be converted to UTC timezone
    
    3. DATA CONSISTENCY:
       - The total count of customers should match between source and target
       - Sum of order amounts should be equal in both databases
    
    4. REFERENTIAL INTEGRITY:
       - All order.customer_id values should exist in customers.id
       - No orphan records should exist in child tables
    
    5. DATA QUALITY:
       - No duplicate email addresses in the target database
       - All date fields should be valid dates (not in the future for birth_date)
    """

    print("📋 Business Rules:")
    print("-" * 40)
    print(business_rules)
    print()

    try:
        # Initialize the agent
        print("🔄 Initializing agent...")
        await validation_agent.initialize()
        print("✅ Agent initialized")
        print()

        # Get database information
        print("📊 Database Information:")
        print("-" * 40)
        db_info = await validation_agent.get_database_info()
        print(f"Source: {db_info['source']['tables']} tables")
        print(f"Target: {db_info['target']['tables']} tables")
        print()

        # Run validation
        print("🔍 Running validation...")
        print("-" * 40)
        
        report = await validation_agent.validate(
            business_rules=business_rules,
            validation_name="Customer ETL Validation",
        )

        # Display results
        print()
        print("=" * 60)
        print("VALIDATION RESULTS")
        print("=" * 60)
        print()
        print(f"Overall Status: {report.overall_status.value.upper()}")
        print(f"Total Tests: {report.execution_summary.total_tests}")
        print(f"Passed: {report.execution_summary.passed}")
        print(f"Failed: {report.execution_summary.failed}")
        print(f"Pass Rate: {report.execution_summary.pass_rate:.1f}%")
        print(f"Duration: {report.execution_summary.total_duration_ms:.0f}ms")
        print()

        # Display individual test results
        print("Test Results:")
        print("-" * 40)
        for result in report.test_results:
            status_icon = "✅" if result.status.value == "passed" else "❌"
            print(f"{status_icon} {result.test_case_name}: {result.message}")
        print()

        # Display AI analysis
        if report.ai_analysis:
            print("AI Analysis:")
            print("-" * 40)
            print(report.ai_analysis)
            print()

        # Display recommendations
        if report.ai_recommendations:
            print("Recommendations:")
            print("-" * 40)
            for rec in report.ai_recommendations:
                print(f"  • {rec}")
            print()

        # Save report to file
        report_path = "validation_report.md"
        with open(report_path, "w") as f:
            f.write(report.to_markdown())
        print(f"📄 Full report saved to: {report_path}")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        # Cleanup
        await validation_agent.close()
        print()
        print("✅ Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())
