import { useState } from 'react';
import { Link } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import {
  ClockIcon,
  CheckCircleIcon,
  XCircleIcon,
  TrashIcon,
  EyeIcon,
  FunnelIcon,
  MagnifyingGlassIcon,
} from '@heroicons/react/24/outline';
import { useValidationStore } from '../store/validationStore';

export default function History() {
  const { validationHistory, clearHistory, setCurrentReport } = useValidationStore();
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | 'passed' | 'failed'>('all');
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);

  const filteredHistory = validationHistory.filter((item) => {
    const matchesSearch = item.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      item.rules.toLowerCase().includes(searchTerm.toLowerCase());
    const overallStatus = item.report?.overall_status || 'unknown';
    const matchesStatus = statusFilter === 'all' || 
      overallStatus === statusFilter || 
      (statusFilter === 'failed' && (overallStatus === 'failed' || overallStatus === 'error'));
    return matchesSearch && matchesStatus;
  });

  return (
    <div className="space-y-6">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex items-center justify-between"
      >
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Validation History</h1>
          <p className="mt-1 text-gray-600">
            View and manage your past validation runs
          </p>
        </div>
        {validationHistory.length > 0 && (
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className="inline-flex items-center gap-2 rounded-xl border border-red-200 bg-red-50 px-4 py-2 text-sm font-medium text-red-700 hover:bg-red-100 transition-colors"
          >
            <TrashIcon className="h-4 w-4" />
            Clear History
          </button>
        )}
      </motion.div>

      {/* Delete Confirmation Modal */}
      <AnimatePresence>
        {showDeleteConfirm && (
          <>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="fixed inset-0 z-50 bg-gray-900/50"
              onClick={() => setShowDeleteConfirm(false)}
            />
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="fixed left-1/2 top-1/2 z-50 -translate-x-1/2 -translate-y-1/2 w-full max-w-md rounded-2xl bg-white p-6 shadow-xl"
            >
              <h3 className="text-lg font-semibold text-gray-900">Clear History</h3>
              <p className="mt-2 text-gray-600">
                Are you sure you want to delete all validation history? This action cannot be undone.
              </p>
              <div className="mt-6 flex justify-end gap-3">
                <button
                  onClick={() => setShowDeleteConfirm(false)}
                  className="px-4 py-2 text-sm font-medium text-gray-700 hover:text-gray-900"
                >
                  Cancel
                </button>
                <button
                  onClick={() => {
                    clearHistory();
                    setShowDeleteConfirm(false);
                  }}
                  className="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-lg hover:bg-red-700"
                >
                  Delete All
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Filters */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="flex flex-col sm:flex-row gap-4"
      >
        {/* Search */}
        <div className="relative flex-1">
          <MagnifyingGlassIcon className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
          <input
            type="text"
            placeholder="Search validations..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-12 pr-4 py-3 rounded-xl border border-gray-200 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 transition-all"
          />
        </div>

        {/* Status Filter */}
        <div className="flex items-center gap-2">
          <FunnelIcon className="h-5 w-5 text-gray-400" />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value as any)}
            className="rounded-xl border border-gray-200 px-4 py-3 text-sm focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20"
          >
            <option value="all">All Status</option>
            <option value="passed">Passed</option>
            <option value="failed">Failed</option>
          </select>
        </div>
      </motion.div>

      {/* Empty State */}
      {validationHistory.length === 0 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="rounded-xl bg-white shadow-sm border border-gray-100 p-12 text-center"
        >
          <div className="mx-auto h-16 w-16 rounded-2xl bg-gray-100 flex items-center justify-center mb-4">
            <ClockIcon className="h-8 w-8 text-gray-400" />
          </div>
          <h3 className="text-lg font-medium text-gray-900">No Validation History</h3>
          <p className="mt-2 text-gray-500">
            Run your first validation to see results here
          </p>
          <Link
            to="/validate"
            className="mt-6 inline-flex items-center gap-2 rounded-xl bg-primary-600 px-6 py-3 text-sm font-semibold text-white hover:bg-primary-700 transition-colors"
          >
            Start Validation
          </Link>
        </motion.div>
      )}

      {/* History List */}
      {filteredHistory.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="rounded-xl bg-white shadow-sm border border-gray-100 divide-y divide-gray-100"
        >
          {filteredHistory.map((item, index) => (
            <motion.div
              key={item.timestamp}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: index * 0.05 }}
              className="p-6 hover:bg-gray-50 transition-colors"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex items-start gap-4 flex-1">
                  <div className={`flex h-12 w-12 items-center justify-center rounded-xl ${
                    item.report?.overall_status === 'passed' ? 'bg-green-100' : 'bg-red-100'
                  }`}>
                    {item.report?.overall_status === 'passed' ? (
                      <CheckCircleIcon className="h-6 w-6 text-green-600" />
                    ) : (
                      <XCircleIcon className="h-6 w-6 text-red-600" />
                    )}
                  </div>
                  
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-3">
                      <h3 className="font-medium text-gray-900 truncate">{item.name}</h3>
                      <StatusBadge status={item.report?.overall_status || 'unknown'} />
                    </div>
                    
                    <p className="mt-1 text-sm text-gray-500 line-clamp-2">
                      {item.rules}
                    </p>
                    
                    <div className="mt-3 flex items-center gap-4 text-sm text-gray-500">
                      <span className="flex items-center gap-1">
                        <ClockIcon className="h-4 w-4" />
                        {new Date(item.timestamp).toLocaleString()}
                      </span>
                      <span>•</span>
                      <span>
                        {item.report?.summary?.passed || 0}/{item.report?.summary?.total_tests || 0} tests passed
                      </span>
                      <span>•</span>
                      <span>{(item.report?.summary?.pass_rate || 0).toFixed(1)}% pass rate</span>
                    </div>
                  </div>
                </div>

                <Link
                  to="/results"
                  onClick={() => setCurrentReport(item.report)}
                  className="inline-flex items-center gap-2 rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 transition-colors"
                >
                  <EyeIcon className="h-4 w-4" />
                  View
                </Link>
              </div>
            </motion.div>
          ))}
        </motion.div>
      )}

      {/* No Results */}
      {validationHistory.length > 0 && filteredHistory.length === 0 && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="rounded-xl bg-gray-50 border border-gray-100 p-8 text-center"
        >
          <p className="text-gray-500">No validations match your filters</p>
        </motion.div>
      )}

      {/* Stats */}
      {validationHistory.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="grid grid-cols-1 sm:grid-cols-3 gap-4"
        >
          <StatCard
            label="Total Validations"
            value={validationHistory.length.toString()}
          />
          <StatCard
            label="Average Pass Rate"
            value={`${(validationHistory.reduce((acc, v) => acc + (v.report?.summary?.pass_rate || 0), 0) / validationHistory.length).toFixed(1)}%`}
          />
          <StatCard
            label="Total Tests Run"
            value={validationHistory.reduce((acc, v) => acc + (v.report?.summary?.total_tests || 0), 0).toString()}
          />
        </motion.div>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const styles = {
    passed: 'bg-green-100 text-green-700',
    failed: 'bg-red-100 text-red-700',
    partial: 'bg-yellow-100 text-yellow-700',
    error: 'bg-orange-100 text-orange-700',
    unknown: 'bg-gray-100 text-gray-700',
  };

  return (
    <span className={`text-xs font-medium px-2.5 py-0.5 rounded-full ${styles[status as keyof typeof styles] || 'bg-gray-100 text-gray-700'}`}>
      {(status || 'unknown').toUpperCase()}
    </span>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl bg-white p-4 shadow-sm border border-gray-100">
      <p className="text-sm text-gray-500">{label}</p>
      <p className="text-2xl font-bold text-gray-900">{value}</p>
    </div>
  );
}
