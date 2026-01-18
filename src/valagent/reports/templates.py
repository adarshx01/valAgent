"""
Report templates for generating validation reports.
"""


class ReportTemplates:
    """HTML and Markdown templates for reports."""

    HTML_REPORT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Validation Report - {{ run.name }}</title>
    <style>
        :root {
            --primary: #2563eb;
            --success: #16a34a;
            --warning: #ca8a04;
            --error: #dc2626;
            --gray-50: #f9fafb;
            --gray-100: #f3f4f6;
            --gray-200: #e5e7eb;
            --gray-300: #d1d5db;
            --gray-600: #4b5563;
            --gray-800: #1f2937;
            --gray-900: #111827;
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: var(--gray-800);
            background: var(--gray-50);
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
        }

        header {
            background: white;
            border-bottom: 1px solid var(--gray-200);
            padding: 1.5rem 0;
            margin-bottom: 2rem;
        }

        header .container {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .logo {
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--primary);
        }

        .report-meta {
            text-align: right;
            color: var(--gray-600);
            font-size: 0.875rem;
        }

        h1 {
            font-size: 1.875rem;
            margin-bottom: 0.5rem;
            color: var(--gray-900);
        }

        h2 {
            font-size: 1.25rem;
            margin-bottom: 1rem;
            color: var(--gray-800);
            border-bottom: 2px solid var(--primary);
            padding-bottom: 0.5rem;
        }

        h3 {
            font-size: 1rem;
            margin-bottom: 0.75rem;
            color: var(--gray-700);
        }

        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }

        .summary-card {
            background: white;
            border-radius: 0.5rem;
            padding: 1.5rem;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .summary-card.passed { border-left: 4px solid var(--success); }
        .summary-card.failed { border-left: 4px solid var(--error); }
        .summary-card.error { border-left: 4px solid var(--warning); }
        .summary-card.total { border-left: 4px solid var(--primary); }

        .summary-card .value {
            font-size: 2rem;
            font-weight: 700;
        }

        .summary-card.passed .value { color: var(--success); }
        .summary-card.failed .value { color: var(--error); }
        .summary-card.error .value { color: var(--warning); }
        .summary-card.total .value { color: var(--primary); }

        .summary-card .label {
            color: var(--gray-600);
            font-size: 0.875rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .section {
            background: white;
            border-radius: 0.5rem;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .status-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }

        .status-passed { background: #dcfce7; color: var(--success); }
        .status-failed { background: #fee2e2; color: var(--error); }
        .status-error { background: #fef3c7; color: var(--warning); }
        .status-pending { background: var(--gray-100); color: var(--gray-600); }

        .test-case {
            border: 1px solid var(--gray-200);
            border-radius: 0.5rem;
            margin-bottom: 1rem;
            overflow: hidden;
        }

        .test-case-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem;
            background: var(--gray-50);
            cursor: pointer;
        }

        .test-case-header:hover {
            background: var(--gray-100);
        }

        .test-case-title {
            font-weight: 600;
            color: var(--gray-800);
        }

        .test-case-body {
            padding: 1rem;
            border-top: 1px solid var(--gray-200);
        }

        .test-case-body.collapsed {
            display: none;
        }

        .query-block {
            background: var(--gray-900);
            color: #e5e7eb;
            padding: 1rem;
            border-radius: 0.375rem;
            font-family: 'Fira Code', 'Monaco', monospace;
            font-size: 0.875rem;
            overflow-x: auto;
            margin: 0.5rem 0;
        }

        .evidence-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.875rem;
            margin-top: 0.5rem;
        }

        .evidence-table th,
        .evidence-table td {
            padding: 0.5rem;
            text-align: left;
            border-bottom: 1px solid var(--gray-200);
        }

        .evidence-table th {
            background: var(--gray-50);
            font-weight: 600;
            color: var(--gray-700);
        }

        .evidence-table tr:hover {
            background: var(--gray-50);
        }

        .business-rules {
            list-style: none;
        }

        .business-rules li {
            padding: 0.75rem 1rem;
            background: var(--gray-50);
            border-radius: 0.375rem;
            margin-bottom: 0.5rem;
            border-left: 3px solid var(--primary);
        }

        .execution-time {
            color: var(--gray-500);
            font-size: 0.75rem;
        }

        .progress-bar {
            height: 0.5rem;
            background: var(--gray-200);
            border-radius: 9999px;
            overflow: hidden;
            margin: 1rem 0;
        }

        .progress-bar-fill {
            height: 100%;
            background: var(--success);
            transition: width 0.3s ease;
        }

        footer {
            text-align: center;
            color: var(--gray-500);
            font-size: 0.875rem;
            padding: 2rem;
            border-top: 1px solid var(--gray-200);
            margin-top: 2rem;
        }

        @media print {
            body { background: white; }
            .container { max-width: 100%; }
            .test-case-body { display: block !important; }
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <div class="logo">🎯 ValAgent</div>
            <div class="report-meta">
                <div>Report ID: {{ run.id[:8] }}</div>
                <div>Generated: {{ generated_at }}</div>
            </div>
        </div>
    </header>

    <div class="container">
        <h1>{{ run.name }}</h1>
        {% if run.description %}
        <p style="color: var(--gray-600); margin-bottom: 1.5rem;">{{ run.description }}</p>
        {% endif %}

        <!-- Summary Cards -->
        <div class="summary-grid">
            <div class="summary-card total">
                <div class="value">{{ run.total_tests }}</div>
                <div class="label">Total Tests</div>
            </div>
            <div class="summary-card passed">
                <div class="value">{{ run.passed_tests }}</div>
                <div class="label">Passed</div>
            </div>
            <div class="summary-card failed">
                <div class="value">{{ run.failed_tests }}</div>
                <div class="label">Failed</div>
            </div>
            <div class="summary-card error">
                <div class="value">{{ run.error_tests }}</div>
                <div class="label">Errors</div>
            </div>
        </div>

        <!-- Pass Rate -->
        <div class="section">
            <h3>Pass Rate: {{ pass_rate }}%</h3>
            <div class="progress-bar">
                <div class="progress-bar-fill" style="width: {{ pass_rate }}%"></div>
            </div>
            <p class="execution-time">
                Total Execution Time: {{ "%.2f"|format(run.execution_time_ms / 1000) }}s
                {% if run.started_at and run.completed_at %}
                | Started: {{ run.started_at.strftime('%Y-%m-%d %H:%M:%S') }}
                | Completed: {{ run.completed_at.strftime('%Y-%m-%d %H:%M:%S') }}
                {% endif %}
            </p>
        </div>

        <!-- Business Rules -->
        <div class="section">
            <h2>📋 Business Rules Validated</h2>
            <ul class="business-rules">
                {% for rule in run.business_rules %}
                <li>{{ rule }}</li>
                {% endfor %}
            </ul>
        </div>

        <!-- Test Results -->
        <div class="section">
            <h2>🧪 Test Results</h2>
            
            {% for test in run.test_cases %}
            <div class="test-case">
                <div class="test-case-header" onclick="toggleTestCase(this)">
                    <div>
                        <span class="test-case-title">{{ test.name }}</span>
                        <span class="execution-time">({{ "%.0f"|format(test.execution_time_ms) }}ms)</span>
                    </div>
                    <span class="status-badge status-{{ test.status.value }}">{{ test.status.value }}</span>
                </div>
                <div class="test-case-body collapsed">
                    <p><strong>Description:</strong> {{ test.description }}</p>
                    <p><strong>Business Rule:</strong> {{ test.business_rule }}</p>
                    <p><strong>Validation Type:</strong> {{ test.validation_type }}</p>
                    
                    {% if test.source_query %}
                    <h4>Source Query:</h4>
                    <pre class="query-block">{{ test.source_query }}</pre>
                    {% endif %}
                    
                    {% if test.target_query %}
                    <h4>Target Query:</h4>
                    <pre class="query-block">{{ test.target_query }}</pre>
                    {% endif %}
                    
                    {% if test.actual_result %}
                    <h4>Result:</h4>
                    <table class="evidence-table">
                        <tr>
                            <th>Metric</th>
                            <th>Value</th>
                        </tr>
                        {% for key, value in test.actual_result.items() %}
                        <tr>
                            <td>{{ key }}</td>
                            <td>{{ value }}</td>
                        </tr>
                        {% endfor %}
                    </table>
                    {% endif %}
                    
                    {% if test.error_message %}
                    <h4>Error:</h4>
                    <pre class="query-block" style="background: #7f1d1d;">{{ test.error_message }}</pre>
                    {% endif %}
                    
                    {% if test.evidence and test.evidence.get('target_sample') %}
                    <h4>Sample Data (Target):</h4>
                    <table class="evidence-table">
                        <tr>
                            {% for col in test.evidence['target_sample'][0].keys() %}
                            <th>{{ col }}</th>
                            {% endfor %}
                        </tr>
                        {% for row in test.evidence['target_sample'][:5] %}
                        <tr>
                            {% for val in row.values() %}
                            <td>{{ val }}</td>
                            {% endfor %}
                        </tr>
                        {% endfor %}
                    </table>
                    {% endif %}
                </div>
            </div>
            {% endfor %}
        </div>
    </div>

    <footer>
        <p>Generated by ValAgent - Data Validation Agent</p>
        <p>© {{ current_year }} IQVIA. All rights reserved.</p>
    </footer>

    <script>
        function toggleTestCase(header) {
            const body = header.nextElementSibling;
            body.classList.toggle('collapsed');
        }
        
        // Expand failed tests by default
        document.querySelectorAll('.status-failed, .status-error').forEach(badge => {
            const body = badge.closest('.test-case').querySelector('.test-case-body');
            body.classList.remove('collapsed');
        });
    </script>
</body>
</html>
"""

    MARKDOWN_REPORT = """
# Validation Report: {{ run.name }}

**Report ID:** {{ run.id }}  
**Generated:** {{ generated_at }}  
**Status:** {{ run.status.value | upper }}

---

## 📊 Summary

| Metric | Value |
|--------|-------|
| Total Tests | {{ run.total_tests }} |
| Passed | {{ run.passed_tests }} ✅ |
| Failed | {{ run.failed_tests }} ❌ |
| Errors | {{ run.error_tests }} ⚠️ |
| Pass Rate | {{ pass_rate }}% |
| Execution Time | {{ "%.2f"|format(run.execution_time_ms / 1000) }}s |

---

## 📋 Business Rules Validated

{% for rule in run.business_rules %}
{{ loop.index }}. {{ rule }}
{% endfor %}

---

## 🧪 Test Results

{% for test in run.test_cases %}
### {{ loop.index }}. {{ test.name }}

**Status:** {{ test.status.value | upper }} {% if test.status.value == 'passed' %}✅{% elif test.status.value == 'failed' %}❌{% else %}⚠️{% endif %}  
**Type:** {{ test.validation_type }}  
**Execution Time:** {{ "%.0f"|format(test.execution_time_ms) }}ms

**Description:** {{ test.description }}

**Business Rule:** {{ test.business_rule }}

{% if test.target_query %}
**Target Query:**
```sql
{{ test.target_query }}
```
{% endif %}

{% if test.source_query %}
**Source Query:**
```sql
{{ test.source_query }}
```
{% endif %}

{% if test.actual_result %}
**Result:**
| Metric | Value |
|--------|-------|
{% for key, value in test.actual_result.items() %}
| {{ key }} | {{ value }} |
{% endfor %}
{% endif %}

{% if test.error_message %}
**Error:** `{{ test.error_message }}`
{% endif %}

---

{% endfor %}

## 📈 Execution Details

- **Started:** {{ run.started_at.strftime('%Y-%m-%d %H:%M:%S') if run.started_at else 'N/A' }}
- **Completed:** {{ run.completed_at.strftime('%Y-%m-%d %H:%M:%S') if run.completed_at else 'N/A' }}

---

*Generated by ValAgent - Data Validation Agent*
"""

    JSON_REPORT_SCHEMA = {
        "type": "object",
        "properties": {
            "report_id": {"type": "string"},
            "validation_run": {"type": "object"},
            "summary": {"type": "object"},
            "test_results": {"type": "array"},
            "generated_at": {"type": "string"},
            "version": {"type": "string"},
        },
    }
