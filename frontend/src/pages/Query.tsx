import { useState } from 'react';
import { motion } from 'framer-motion';
import {
  PlayIcon,
  SparklesIcon,
  ArrowPathIcon,
  ClipboardIcon,
  CheckIcon,
  ExclamationTriangleIcon,
} from '@heroicons/react/24/outline';
import { queryApi, QueryResult } from '../api';

export default function Query() {
  const [query, setQuery] = useState('');
  const [database, setDatabase] = useState<'source' | 'target'>('source');
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [naturalLanguage, setNaturalLanguage] = useState('');
  const [copied, setCopied] = useState(false);

  const executeQuery = async () => {
    if (!query.trim()) {
      setError('Please enter a SQL query');
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await queryApi.execute({
        query,
        database,
      });
      setResult(response);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || 'Query execution failed');
    } finally {
      setLoading(false);
    }
  };

  const generateSQL = async () => {
    if (!naturalLanguage.trim()) {
      setError('Please enter a natural language description');
      return;
    }

    setGenerating(true);
    setError(null);

    try {
      const response = await queryApi.generate({
        natural_language: naturalLanguage,
        database,
      });
      setQuery(response.sql);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || 'SQL generation failed');
    } finally {
      setGenerating(false);
    }
  };

  const copyQuery = async () => {
    await navigator.clipboard.writeText(query);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
      >
        <h1 className="text-2xl font-bold text-gray-900">Query Editor</h1>
        <p className="mt-1 text-gray-600">
          Execute SQL queries or generate them from natural language
        </p>
      </motion.div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Query Editor Section */}
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.1 }}
          className="space-y-4"
        >
          {/* Database Selector */}
          <div className="flex items-center gap-2 p-1 bg-gray-100 rounded-lg w-fit">
            <button
              onClick={() => setDatabase('source')}
              className={`px-4 py-2 text-sm font-medium rounded-md transition-all ${
                database === 'source'
                  ? 'bg-white text-gray-900 shadow-sm'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Source DB
            </button>
            <button
              onClick={() => setDatabase('target')}
              className={`px-4 py-2 text-sm font-medium rounded-md transition-all ${
                database === 'target'
                  ? 'bg-white text-gray-900 shadow-sm'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Target DB
            </button>
          </div>

          {/* AI SQL Generator */}
          <div className="rounded-xl bg-gradient-to-br from-primary-50 to-indigo-50 border border-primary-100 p-4">
            <div className="flex items-center gap-2 mb-3">
              <SparklesIcon className="h-5 w-5 text-primary-600" />
              <h3 className="font-medium text-primary-900">AI SQL Generator</h3>
            </div>
            <textarea
              value={naturalLanguage}
              onChange={(e) => setNaturalLanguage(e.target.value)}
              placeholder="Describe what you want to query, e.g., 'Show me all customers who made purchases last month'"
              rows={3}
              className="w-full rounded-lg border border-primary-200 bg-white/50 px-3 py-2 text-sm placeholder:text-gray-400 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20"
              disabled={generating}
            />
            <button
              onClick={generateSQL}
              disabled={generating || !naturalLanguage.trim()}
              className="mt-3 inline-flex items-center gap-2 rounded-lg bg-primary-600 px-4 py-2 text-sm font-medium text-white hover:bg-primary-700 disabled:opacity-50 transition-colors"
            >
              {generating ? (
                <>
                  <ArrowPathIcon className="h-4 w-4 animate-spin" />
                  Generating...
                </>
              ) : (
                <>
                  <SparklesIcon className="h-4 w-4" />
                  Generate SQL
                </>
              )}
            </button>
          </div>

          {/* SQL Editor */}
          <div className="rounded-xl bg-white shadow-sm border border-gray-100 overflow-hidden">
            <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100 bg-gray-50">
              <span className="text-sm font-medium text-gray-700">SQL Query</span>
              <button
                onClick={copyQuery}
                disabled={!query}
                className="p-1.5 rounded hover:bg-gray-200 transition-colors disabled:opacity-50"
                title="Copy query"
              >
                {copied ? (
                  <CheckIcon className="h-4 w-4 text-green-600" />
                ) : (
                  <ClipboardIcon className="h-4 w-4 text-gray-500" />
                )}
              </button>
            </div>
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="SELECT * FROM table_name LIMIT 10;"
              rows={10}
              className="w-full px-4 py-3 font-mono text-sm text-gray-900 placeholder:text-gray-400 focus:outline-none resize-none"
              disabled={loading}
            />
          </div>

          {/* Execute Button */}
          <div className="flex items-center gap-3">
            <button
              onClick={executeQuery}
              disabled={loading || !query.trim()}
              className="inline-flex items-center gap-2 rounded-xl bg-green-600 px-6 py-3 text-sm font-semibold text-white shadow-lg hover:bg-green-700 disabled:opacity-50 transition-all"
            >
              {loading ? (
                <>
                  <ArrowPathIcon className="h-5 w-5 animate-spin" />
                  Executing...
                </>
              ) : (
                <>
                  <PlayIcon className="h-5 w-5" />
                  Execute Query
                </>
              )}
            </button>
            <button
              onClick={() => {
                setQuery('');
                setResult(null);
                setError(null);
              }}
              className="px-4 py-3 text-sm font-medium text-gray-600 hover:text-gray-900"
            >
              Clear
            </button>
          </div>

          {/* Error */}
          {error && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              className="rounded-xl bg-red-50 border border-red-200 p-4"
            >
              <div className="flex items-start gap-3">
                <ExclamationTriangleIcon className="h-5 w-5 text-red-500 mt-0.5" />
                <p className="text-sm text-red-700">{error}</p>
              </div>
            </motion.div>
          )}
        </motion.div>

        {/* Results Section */}
        <motion.div
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.2 }}
          className="rounded-xl bg-white shadow-sm border border-gray-100 overflow-hidden"
        >
          <div className="px-4 py-3 border-b border-gray-100 bg-gray-50">
            <h3 className="font-medium text-gray-900">Results</h3>
          </div>
          
          {result ? (
            <div>
              {/* Stats */}
              <div className="px-4 py-2 border-b border-gray-100 bg-gray-50/50 flex items-center gap-4 text-sm">
                <span className="text-gray-600">
                  <strong>{result.row_count}</strong> rows
                </span>
                <span className="text-gray-400">•</span>
                <span className="text-gray-600">
                  <strong>{result.execution_time.toFixed(2)}</strong>ms
                </span>
              </div>

              {/* Table */}
              <div className="overflow-auto max-h-[500px]">
                {result.rows && result.rows.length > 0 ? (
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-gray-50">
                      <tr>
                        {Object.keys(result.rows[0]).map((col) => (
                          <th
                            key={col}
                            className="px-4 py-2 text-left font-medium text-gray-700 border-b border-gray-200"
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {result.rows.map((row, i) => (
                        <tr key={i} className="hover:bg-gray-50">
                          {Object.values(row).map((val, j) => (
                            <td key={j} className="px-4 py-2 text-gray-700 font-mono text-xs">
                              {val === null ? (
                                <span className="text-gray-400 italic">null</span>
                              ) : typeof val === 'object' ? (
                                JSON.stringify(val)
                              ) : (
                                String(val)
                              )}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <div className="p-8 text-center text-gray-500">
                    Query executed successfully. No rows returned.
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center p-12 text-center">
              <div className="h-16 w-16 rounded-2xl bg-gray-100 flex items-center justify-center mb-4">
                <PlayIcon className="h-8 w-8 text-gray-400" />
              </div>
              <h3 className="text-lg font-medium text-gray-900">No Results Yet</h3>
              <p className="mt-1 text-sm text-gray-500">
                Write a query and click Execute to see results
              </p>
            </div>
          )}
        </motion.div>
      </div>

      {/* Quick Queries */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.3 }}
        className="rounded-xl bg-gray-50 border border-gray-100 p-6"
      >
        <h3 className="text-sm font-semibold text-gray-900 mb-4">Quick Queries</h3>
        <div className="flex flex-wrap gap-2">
          {[
            { label: 'List Tables', sql: "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'" },
            { label: 'Table Columns', sql: "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'your_table'" },
            { label: 'Row Counts', sql: "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC" },
            { label: 'Recent Records', sql: "SELECT * FROM your_table ORDER BY created_at DESC LIMIT 10" },
          ].map((q) => (
            <button
              key={q.label}
              onClick={() => setQuery(q.sql)}
              className="px-3 py-1.5 text-xs font-medium text-gray-600 bg-white border border-gray-200 rounded-lg hover:border-primary-300 hover:text-primary-700 transition-colors"
            >
              {q.label}
            </button>
          ))}
        </div>
      </motion.div>
    </div>
  );
}
