import { useEffect } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import {
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  DocumentArrowDownIcon,
  ArrowLeftIcon,
  PlayIcon,
  CodeBracketIcon,
} from '@heroicons/react/24/outline';
import { useValidationStore } from '../store/validationStore';

export default function Results() {
  const { currentReport } = useValidationStore();
  const navigate = useNavigate();

  useEffect(() => {
    if (!currentReport) {
      navigate('/validate');
    }
  }, [currentReport, navigate]);

  if (!currentReport) {
    return null;
  }

  // Handle both possible API response structures
  const report_name = currentReport.report_name || 'Validation Report';
  const generated_at = currentReport.generated_at || new Date().toISOString();
  const overall_status = currentReport.overall_status || 'unknown';
  const summary = currentReport.summary || {};
  const test_results = currentReport.test_results || [];
  const markdown_report = currentReport.markdown_report;

  return (
    <div className="space-y-6">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex items-center justify-between"
      >
        <div className="flex items-center gap-4">
          <Link
            to="/validate"
            className="p-2 rounded-lg hover:bg-gray-100 transition-colors"
          >
            <ArrowLeftIcon className="h-5 w-5 text-gray-500" />
          </Link>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">{report_name}</h1>
            <p className="text-sm text-gray-500">
              Generated: {new Date(generated_at).toLocaleString()}
            </p>
          </div>
        </div>
        
        <div className="flex items-center gap-3">
          <button className="inline-flex items-center gap-2 rounded-xl border border-gray-200 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 transition-colors">
            <DocumentArrowDownIcon className="h-4 w-4" />
            Export
          </button>
          <Link
            to="/validate"
            className="inline-flex items-center gap-2 rounded-xl bg-primary-600 px-4 py-2 text-sm font-medium text-white hover:bg-primary-700 transition-colors"
          >
            <PlayIcon className="h-4 w-4" />
            New Validation
          </Link>
        </div>
      </motion.div>

      {/* Summary Cards */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4"
      >
        <SummaryCard
          title="Overall Status"
          value={(overall_status || 'unknown').toUpperCase()}
          icon={overall_status === 'passed' ? CheckCircleIcon : XCircleIcon}
          color={overall_status === 'passed' ? 'green' : 'red'}
        />
        <SummaryCard
          title="Pass Rate"
          value={`${(summary.pass_rate || 0).toFixed(1)}%`}
          icon={CheckCircleIcon}
          color={(summary.pass_rate || 0) >= 80 ? 'green' : (summary.pass_rate || 0) >= 50 ? 'yellow' : 'red'}
        />
        <SummaryCard
          title="Total Tests"
          value={(summary.total_tests || 0).toString()}
          subvalue={`${summary.passed || 0} passed, ${summary.failed || 0} failed`}
          icon={PlayIcon}
          color="blue"
        />
        <SummaryCard
          title="Execution Time"
          value={`${(((summary as any).duration_ms || (summary as any).total_execution_time || 0) / 1000).toFixed(2)}s`}
          icon={ClockIcon}
          color="purple"
        />
      </motion.div>

      {/* Test Results */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
        className="rounded-xl bg-white shadow-sm border border-gray-100"
      >
        <div className="border-b border-gray-100 px-6 py-4 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-900">Test Results</h2>
          <span className="text-sm text-gray-500">{test_results.length} tests</span>
        </div>
        <div className="divide-y divide-gray-100">
          {test_results.length === 0 ? (
            <div className="p-6 text-center text-gray-500">
              No test results available
            </div>
          ) : (
            test_results.map((result: any, index: number) => (
              <TestResultCard key={result.test_id || result.test_case_id || index} result={result} index={index} />
            ))
          )}
        </div>
      </motion.div>

      {/* Markdown Report */}
      {markdown_report && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="rounded-xl bg-white shadow-sm border border-gray-100"
        >
          <div className="border-b border-gray-100 px-6 py-4">
            <h2 className="text-lg font-semibold text-gray-900">Detailed Report</h2>
          </div>
          <div className="p-6 prose prose-sm max-w-none">
            <pre className="whitespace-pre-wrap text-sm text-gray-700 font-mono bg-gray-50 p-4 rounded-lg">
              {markdown_report}
            </pre>
          </div>
        </motion.div>
      )}
    </div>
  );
}

function SummaryCard({ title, value, subvalue, icon: Icon, color }: {
  title: string;
  value: string;
  subvalue?: string;
  icon: any;
  color: 'green' | 'red' | 'yellow' | 'blue' | 'purple';
}) {
  const colorClasses = {
    green: 'bg-green-100 text-green-600',
    red: 'bg-red-100 text-red-600',
    yellow: 'bg-yellow-100 text-yellow-600',
    blue: 'bg-blue-100 text-blue-600',
    purple: 'bg-purple-100 text-purple-600',
  };

  return (
    <div className="rounded-xl bg-white p-6 shadow-sm border border-gray-100">
      <div className="flex items-center gap-4">
        <div className={`flex h-12 w-12 items-center justify-center rounded-xl ${colorClasses[color]}`}>
          <Icon className="h-6 w-6" />
        </div>
        <div>
          <p className="text-sm text-gray-500">{title}</p>
          <p className="text-xl font-bold text-gray-900">{value}</p>
          {subvalue && <p className="text-xs text-gray-400">{subvalue}</p>}
        </div>
      </div>
    </div>
  );
}

function TestResultCard({ result, index }: { result: any; index: number }) {
  const statusConfig = {
    passed: { icon: CheckCircleIcon, bg: 'bg-green-50', text: 'text-green-700', border: 'border-green-200' },
    failed: { icon: XCircleIcon, bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200' },
    error: { icon: ExclamationTriangleIcon, bg: 'bg-orange-50', text: 'text-orange-700', border: 'border-orange-200' },
  };

  const status = result.status || 'unknown';
  const config = statusConfig[status as keyof typeof statusConfig] || statusConfig.error;
  const StatusIcon = config.icon;
  
  // Handle both naming conventions from API
  const testName = result.test_name || result.test_case_name || 'Unnamed Test';
  const description = result.description || result.message || '';
  const errorMessage = result.error_message || (status === 'error' ? result.message : null);
  const executionTime = result.execution_time || result.duration_ms;
  const sourceRows = result.source_rows;
  const targetRows = result.target_rows;

  return (
    <motion.div
      initial={{ opacity: 0, x: -20 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.02 }}
      className="p-6"
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-start gap-4 flex-1">
          <div className={`flex h-10 w-10 items-center justify-center rounded-lg ${config.bg} flex-shrink-0`}>
            <StatusIcon className={`h-5 w-5 ${config.text}`} />
          </div>
          <div className="flex-1 min-w-0">
            {/* Header */}
            <div className="flex items-center gap-3 flex-wrap">
              <h3 className="font-medium text-gray-900">{testName}</h3>
              <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${config.bg} ${config.text}`}>
                {status.toUpperCase()}
              </span>
              {executionTime && (
                <span className="text-xs text-gray-400">{executionTime.toFixed(0)}ms</span>
              )}
            </div>
            <p className="text-sm text-gray-500 mt-1">{description}</p>
            
            {/* Row counts */}
            {(sourceRows !== undefined || targetRows !== undefined) && (
              <div className="mt-2 flex items-center gap-4 text-xs text-gray-500">
                {sourceRows !== undefined && sourceRows !== null && (
                  <span>Source rows: <strong>{sourceRows.toLocaleString()}</strong></span>
                )}
                {targetRows !== undefined && targetRows !== null && (
                  <span>Target rows: <strong>{targetRows.toLocaleString()}</strong></span>
                )}
              </div>
            )}
            
            {errorMessage && (
              <div className="mt-3 rounded-lg bg-red-50 border border-red-200 p-3">
                <p className="text-sm text-red-700">{errorMessage}</p>
              </div>
            )}

            {/* SQL Queries and Results Section */}
            {(result.source_query || result.target_query) && (
              <div className="mt-4 border border-gray-200 rounded-lg overflow-hidden">
                <div className="bg-gray-50 px-4 py-2 border-b border-gray-200">
                  <h4 className="text-sm font-medium text-gray-700 flex items-center gap-2">
                    <CodeBracketIcon className="h-4 w-4" />
                    SQL Queries & Results
                  </h4>
                </div>
                <div className="divide-y divide-gray-100 max-h-[500px] overflow-y-auto">
                  {/* Source Query */}
                  {result.source_query && (
                    <SQLResultPanel
                      label="Source Database"
                      query={result.source_query}
                      result={result.source_result}
                      bgColor="bg-blue-50"
                      labelColor="text-blue-700"
                    />
                  )}
                  {/* Target Query */}
                  {result.target_query && (
                    <SQLResultPanel
                      label="Target Database"
                      query={result.target_query}
                      result={result.target_result}
                      bgColor="bg-purple-50"
                      labelColor="text-purple-700"
                    />
                  )}
                </div>
              </div>
            )}

            {/* Legacy Execution Proof (fallback) */}
            {result.execution_proof && !result.source_query && !result.target_query && (
              <div className="mt-4">
                <p className="text-xs font-medium text-gray-500 mb-2">Execution Proof</p>
                <div className="grid grid-cols-2 gap-4">
                  <ProofCard
                    label="Source"
                    rows={result.execution_proof.source_row_count}
                    time={result.execution_proof.source_execution_time}
                    sample={result.execution_proof.source_sample}
                  />
                  <ProofCard
                    label="Target"
                    rows={result.execution_proof.target_row_count}
                    time={result.execution_proof.target_execution_time}
                    sample={result.execution_proof.target_sample}
                  />
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </motion.div>
  );
}

function SQLResultPanel({ 
  label, 
  query, 
  result, 
  bgColor, 
  labelColor 
}: {
  label: string;
  query: string;
  result?: {
    row_count: number;
    execution_time_ms: number;
    sample_data: any[];
    columns: string[];
    success: boolean;
    error?: string | null;
  } | null;
  bgColor: string;
  labelColor: string;
}) {
  return (
    <div className="p-4 space-y-3">
      {/* Label */}
      <div className="flex items-center justify-between">
        <span className={`text-xs font-semibold px-2 py-1 rounded ${bgColor} ${labelColor}`}>
          {label}
        </span>
        {result && (
          <div className="flex items-center gap-3 text-xs text-gray-500">
            <span>{result.row_count.toLocaleString()} rows</span>
            <span>{result.execution_time_ms.toFixed(0)}ms</span>
            {result.success ? (
              <CheckCircleIcon className="h-4 w-4 text-green-500" />
            ) : (
              <XCircleIcon className="h-4 w-4 text-red-500" />
            )}
          </div>
        )}
      </div>
      
      {/* SQL Query */}
      <div>
        <p className="text-xs font-medium text-gray-500 mb-1">SQL Query:</p>
        <pre className="text-xs text-gray-700 bg-gray-50 rounded-lg p-3 overflow-x-auto max-h-32 overflow-y-auto border border-gray-200">
          {query}
        </pre>
      </div>
      
      {/* Query Result */}
      {result && (
        <div>
          <p className="text-xs font-medium text-gray-500 mb-1">
            Result ({result.row_count} rows{result.columns?.length > 0 && `, ${result.columns.length} columns`}):
          </p>
          {result.error ? (
            <div className="text-xs text-red-600 bg-red-50 rounded-lg p-3 border border-red-200">
              Error: {result.error}
            </div>
          ) : result.sample_data && result.sample_data.length > 0 ? (
            <div className="bg-gray-50 rounded-lg border border-gray-200 overflow-hidden">
              {/* Column headers */}
              {result.columns && result.columns.length > 0 && (
                <div className="bg-gray-100 px-3 py-2 text-xs font-medium text-gray-600 flex gap-4 overflow-x-auto">
                  {result.columns.map((col, i) => (
                    <span key={i} className="whitespace-nowrap">{col}</span>
                  ))}
                </div>
              )}
              {/* Data rows */}
              <div className="max-h-48 overflow-y-auto divide-y divide-gray-100">
                {result.sample_data.slice(0, 5).map((row, i) => (
                  <div key={i} className="px-3 py-2 text-xs text-gray-700 flex gap-4 overflow-x-auto hover:bg-gray-100">
                    {Object.entries(row).map(([key, value], j) => (
                      <span key={j} className="whitespace-nowrap">
                        <span className="text-gray-400">{key}:</span> {String(value ?? 'null')}
                      </span>
                    ))}
                  </div>
                ))}
              </div>
              {result.sample_data.length > 5 && (
                <div className="px-3 py-2 text-xs text-gray-400 bg-gray-50 border-t border-gray-200">
                  ... and {result.sample_data.length - 5} more rows (showing first 5)
                </div>
              )}
            </div>
          ) : (
            <div className="text-xs text-gray-500 bg-gray-50 rounded-lg p-3 border border-gray-200">
              No data returned
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ProofCard({ label, rows, time, sample }: {
  label: string;
  rows?: number;
  time?: number;
  sample?: any[];
}) {
  return (
    <div className="rounded-lg bg-gray-50 border border-gray-100 p-3">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-medium text-gray-500">{label}</span>
        <span className="text-xs text-gray-400">{time?.toFixed(0)}ms</span>
      </div>
      <p className="text-sm font-medium text-gray-900">{rows?.toLocaleString()} rows</p>
      {sample && sample.length > 0 && (
        <details className="mt-2">
          <summary className="text-xs text-primary-600 cursor-pointer hover:text-primary-800">
            View sample ({sample.length} rows)
          </summary>
          <pre className="mt-2 text-xs text-gray-600 overflow-x-auto max-h-32 overflow-y-auto">
            {JSON.stringify(sample, null, 2)}
          </pre>
        </details>
      )}
    </div>
  );
}
