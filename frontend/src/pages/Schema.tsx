import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
  TableCellsIcon,
  ArrowPathIcon,
  MagnifyingGlassIcon,
  ChevronRightIcon,
  ServerStackIcon,
} from '@heroicons/react/24/outline';
import { schemaApi, SchemaInfo } from '../api';

export default function Schema() {
  const [sourceSchema, setSourceSchema] = useState<SchemaInfo | null>(null);
  const [targetSchema, setTargetSchema] = useState<SchemaInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [activeTab, setActiveTab] = useState<'source' | 'target' | 'compare'>('source');

  const fetchSchemas = async () => {
    setLoading(true);
    setError(null);
    try {
      const [source, target] = await Promise.all([
        schemaApi.getSource(),
        schemaApi.getTarget(),
      ]);
      setSourceSchema(source);
      setTargetSchema(target);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch schemas');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSchemas();
  }, []);

  return (
    <div className="space-y-6">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex items-center justify-between"
      >
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Database Schema</h1>
          <p className="mt-1 text-gray-600">
            Explore and compare source and target database structures
          </p>
        </div>
        <button
          onClick={fetchSchemas}
          disabled={loading}
          className="inline-flex items-center gap-2 rounded-xl border border-gray-200 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50 transition-colors"
        >
          <ArrowPathIcon className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </motion.div>

      {/* Tabs */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="flex items-center gap-1 bg-gray-100 rounded-xl p-1"
      >
        {[
          { id: 'source', label: 'Source Database' },
          { id: 'target', label: 'Target Database' },
          { id: 'compare', label: 'Compare' },
        ].map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id as any)}
            className={`flex-1 px-4 py-2.5 text-sm font-medium rounded-lg transition-all ${
              activeTab === tab.id
                ? 'bg-white text-gray-900 shadow-sm'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </motion.div>

      {/* Search */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
        className="relative"
      >
        <MagnifyingGlassIcon className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
        <input
          type="text"
          placeholder="Search tables or columns..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="w-full pl-12 pr-4 py-3 rounded-xl border border-gray-200 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 transition-all"
        />
      </motion.div>

      {/* Error */}
      {error && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="rounded-xl bg-red-50 border border-red-200 p-4"
        >
          <p className="text-sm text-red-700">{error}</p>
        </motion.div>
      )}

      {/* Loading */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-2 border-primary-500 border-t-transparent" />
        </div>
      )}

      {/* Schema Content */}
      {!loading && !error && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          {activeTab === 'source' && sourceSchema && (
            <SchemaView schema={sourceSchema} searchTerm={searchTerm} type="source" />
          )}
          {activeTab === 'target' && targetSchema && (
            <SchemaView schema={targetSchema} searchTerm={searchTerm} type="target" />
          )}
          {activeTab === 'compare' && sourceSchema && targetSchema && (
            <SchemaComparison source={sourceSchema} target={targetSchema} searchTerm={searchTerm} />
          )}
        </motion.div>
      )}
    </div>
  );
}

function SchemaView({ schema, searchTerm, type }: {
  schema: SchemaInfo;
  searchTerm: string;
  type: 'source' | 'target';
}) {
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  const filteredTables = schema.tables?.filter(t =>
    t.table_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    t.columns.some((c: any) => c.column_name.toLowerCase().includes(searchTerm.toLowerCase()))
  ) || [];

  const toggleTable = (tableName: string) => {
    const newExpanded = new Set(expandedTables);
    if (newExpanded.has(tableName)) {
      newExpanded.delete(tableName);
    } else {
      newExpanded.add(tableName);
    }
    setExpandedTables(newExpanded);
  };

  return (
    <div className="space-y-4">
      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        <StatCard
          icon={ServerStackIcon}
          label="Database"
          value={type.charAt(0).toUpperCase() + type.slice(1)}
        />
        <StatCard
          icon={TableCellsIcon}
          label="Tables"
          value={schema.tables?.length.toString() || '0'}
        />
        <StatCard
          icon={TableCellsIcon}
          label="Total Columns"
          value={schema.tables?.reduce((acc, t) => acc + t.columns.length, 0).toString() || '0'}
        />
      </div>

      {/* Tables List */}
      <div className="rounded-xl bg-white shadow-sm border border-gray-100 divide-y divide-gray-100">
        {filteredTables.length === 0 ? (
          <div className="p-8 text-center text-gray-500">
            No tables found matching "{searchTerm}"
          </div>
        ) : (
          filteredTables.map((table: any) => (
            <div key={table.table_name}>
              <button
                onClick={() => toggleTable(table.table_name)}
                className="w-full flex items-center justify-between p-4 hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center gap-3">
                  <TableCellsIcon className="h-5 w-5 text-gray-400" />
                  <div className="text-left">
                    <p className="font-medium text-gray-900">{table.table_name}</p>
                    <p className="text-sm text-gray-500">
                      {table.columns.length} columns
                      {table.row_count !== undefined && ` • ${table.row_count.toLocaleString()} rows`}
                      {table.primary_keys?.length > 0 && ` • PK: ${table.primary_keys.join(', ')}`}
                    </p>
                  </div>
                </div>
                <ChevronRightIcon
                  className={`h-5 w-5 text-gray-400 transition-transform ${
                    expandedTables.has(table.table_name) ? 'rotate-90' : ''
                  }`}
                />
              </button>
              
              {expandedTables.has(table.table_name) && (
                <div className="px-4 pb-4">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-gray-500 border-b border-gray-100">
                        <th className="pb-2 font-medium">Column</th>
                        <th className="pb-2 font-medium">Type</th>
                        <th className="pb-2 font-medium">Nullable</th>
                        <th className="pb-2 font-medium">Default</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-50">
                      {table.columns.map((col: any) => (
                        <tr key={col.column_name} className="text-gray-700">
                          <td className="py-2 font-mono text-xs">{col.column_name}</td>
                          <td className="py-2">
                            <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-700">
                              {col.data_type}
                            </span>
                          </td>
                          <td className="py-2">
                            <span className={`text-xs ${col.is_nullable ? 'text-gray-400' : 'text-gray-900 font-medium'}`}>
                              {col.is_nullable ? 'Yes' : 'No'}
                            </span>
                          </td>
                          <td className="py-2 font-mono text-xs text-gray-500">
                            {col.column_default || '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function SchemaComparison({ source, target, searchTerm }: {
  source: SchemaInfo;
  target: SchemaInfo;
  searchTerm: string;
}) {
  const sourceTableNames = new Set(source.tables?.map(t => t.table_name) || []);
  const targetTableNames = new Set(target.tables?.map(t => t.table_name) || []);
  
  const allTableNames = [...new Set([...sourceTableNames, ...targetTableNames])];
  
  const filteredTables = allTableNames.filter(name =>
    name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="rounded-xl bg-white shadow-sm border border-gray-100 overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-50 text-left text-gray-700">
            <th className="p-4 font-semibold">Table Name</th>
            <th className="p-4 font-semibold text-center">Source</th>
            <th className="p-4 font-semibold text-center">Target</th>
            <th className="p-4 font-semibold">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {filteredTables.map((tableName) => {
            const inSource = sourceTableNames.has(tableName);
            const inTarget = targetTableNames.has(tableName);
            
            let status: 'match' | 'source_only' | 'target_only' = 'match';
            if (inSource && !inTarget) status = 'source_only';
            if (!inSource && inTarget) status = 'target_only';

            return (
              <tr key={tableName} className="hover:bg-gray-50">
                <td className="p-4 font-medium">{tableName}</td>
                <td className="p-4 text-center">
                  {inSource ? (
                    <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-green-100">
                      <CheckIcon className="h-4 w-4 text-green-600" />
                    </span>
                  ) : (
                    <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-gray-100">
                      <XIcon className="h-4 w-4 text-gray-400" />
                    </span>
                  )}
                </td>
                <td className="p-4 text-center">
                  {inTarget ? (
                    <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-green-100">
                      <CheckIcon className="h-4 w-4 text-green-600" />
                    </span>
                  ) : (
                    <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-gray-100">
                      <XIcon className="h-4 w-4 text-gray-400" />
                    </span>
                  )}
                </td>
                <td className="p-4">
                  <StatusBadge status={status} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function StatCard({ icon: Icon, label, value }: { icon: any; label: string; value: string }) {
  return (
    <div className="rounded-xl bg-white p-4 shadow-sm border border-gray-100">
      <div className="flex items-center gap-3">
        <Icon className="h-5 w-5 text-gray-400" />
        <div>
          <p className="text-xs text-gray-500">{label}</p>
          <p className="text-lg font-semibold text-gray-900">{value}</p>
        </div>
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status: 'match' | 'source_only' | 'target_only' }) {
  const config = {
    match: { label: 'Match', bg: 'bg-green-100', text: 'text-green-700' },
    source_only: { label: 'Source Only', bg: 'bg-yellow-100', text: 'text-yellow-700' },
    target_only: { label: 'Target Only', bg: 'bg-blue-100', text: 'text-blue-700' },
  };

  const { label, bg, text } = config[status];

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${bg} ${text}`}>
      {label}
    </span>
  );
}

function CheckIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
    </svg>
  );
}

function XIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
    </svg>
  );
}
