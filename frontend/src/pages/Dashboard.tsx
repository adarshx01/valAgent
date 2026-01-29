import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import {
  PlayIcon,
  TableCellsIcon,
  CommandLineIcon,
  CheckCircleIcon,
  XCircleIcon,
  ClockIcon,
  ArrowTrendingUpIcon,
  ServerStackIcon,
} from '@heroicons/react/24/outline';
import { healthApi, schemaApi } from '../api';
import { useValidationStore } from '../store/validationStore';

export default function Dashboard() {
  const [dbInfo, setDbInfo] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const { validationHistory } = useValidationStore();

  useEffect(() => {
    const fetchData = async () => {
      try {
        const info = await schemaApi.getDatabaseInfo();
        setDbInfo(info.databases);
      } catch (error) {
        console.error('Failed to fetch database info:', error);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  const stats = [
    {
      name: 'Total Validations',
      value: validationHistory.length.toString(),
      icon: PlayIcon,
      color: 'bg-blue-500',
    },
    {
      name: 'Passed',
      value: validationHistory.filter(v => v.report.overall_status === 'passed').length.toString(),
      icon: CheckCircleIcon,
      color: 'bg-green-500',
    },
    {
      name: 'Failed',
      value: validationHistory.filter(v => v.report.overall_status === 'failed').length.toString(),
      icon: XCircleIcon,
      color: 'bg-red-500',
    },
    {
      name: 'Avg Pass Rate',
      value: validationHistory.length > 0 
        ? `${Math.round(validationHistory.reduce((acc, v) => acc + v.report.summary.pass_rate, 0) / validationHistory.length)}%`
        : '0%',
      icon: ArrowTrendingUpIcon,
      color: 'bg-purple-500',
    },
  ];

  return (
    <div className="space-y-8">
      {/* Hero Section */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-primary-600 to-indigo-600 px-8 py-12 shadow-xl"
      >
        <div className="absolute inset-0 bg-grid-white/10" />
        <div className="relative">
          <h1 className="text-3xl font-bold text-white">ETL Validation Agent</h1>
          <p className="mt-2 max-w-2xl text-lg text-primary-100">
            AI-powered data pipeline validation. Define business rules in natural language,
            and let the agent automatically generate and execute comprehensive test cases.
          </p>
          <div className="mt-6 flex gap-4">
            <Link
              to="/validate"
              className="inline-flex items-center gap-2 rounded-xl bg-white px-6 py-3 text-sm font-semibold text-primary-600 shadow-lg hover:bg-primary-50 transition-all"
            >
              <PlayIcon className="h-5 w-5" />
              Start Validation
            </Link>
            <Link
              to="/schema"
              className="inline-flex items-center gap-2 rounded-xl bg-white/10 px-6 py-3 text-sm font-semibold text-white backdrop-blur hover:bg-white/20 transition-all"
            >
              <TableCellsIcon className="h-5 w-5" />
              View Schema
            </Link>
          </div>
        </div>
      </motion.div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {stats.map((stat, index) => (
          <motion.div
            key={stat.name}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: index * 0.1 }}
            className="relative overflow-hidden rounded-xl bg-white p-6 shadow-sm border border-gray-100"
          >
            <div className={`absolute -right-4 -top-4 h-24 w-24 rounded-full ${stat.color} opacity-10`} />
            <div className="flex items-center gap-4">
              <div className={`flex h-12 w-12 items-center justify-center rounded-xl ${stat.color}`}>
                <stat.icon className="h-6 w-6 text-white" />
              </div>
              <div>
                <p className="text-sm text-gray-500">{stat.name}</p>
                <p className="text-2xl font-bold text-gray-900">{stat.value}</p>
              </div>
            </div>
          </motion.div>
        ))}
      </div>

      {/* Database Status & Quick Actions */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        {/* Database Status */}
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.2 }}
          className="rounded-xl bg-white p-6 shadow-sm border border-gray-100"
        >
          <div className="flex items-center gap-3 mb-6">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-indigo-100">
              <ServerStackIcon className="h-5 w-5 text-indigo-600" />
            </div>
            <h2 className="text-lg font-semibold text-gray-900">Database Status</h2>
          </div>

          {loading ? (
            <div className="animate-pulse space-y-4">
              <div className="h-16 bg-gray-100 rounded-lg" />
              <div className="h-16 bg-gray-100 rounded-lg" />
            </div>
          ) : dbInfo ? (
            <div className="space-y-4">
              <DatabaseCard
                name="Source Database"
                status={dbInfo.source ? 'connected' : 'disconnected'}
                tables={dbInfo.source?.tables || 0}
              />
              <DatabaseCard
                name="Target Database"
                status={dbInfo.target ? 'connected' : 'disconnected'}
                tables={dbInfo.target?.tables || 0}
              />
            </div>
          ) : (
            <p className="text-gray-500">Failed to load database info</p>
          )}
        </motion.div>

        {/* Quick Actions */}
        <motion.div
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.3 }}
          className="rounded-xl bg-white p-6 shadow-sm border border-gray-100"
        >
          <h2 className="text-lg font-semibold text-gray-900 mb-6">Quick Actions</h2>
          <div className="grid grid-cols-2 gap-4">
            <QuickActionCard
              title="Run Validation"
              description="Execute business rules"
              icon={PlayIcon}
              href="/validate"
              color="bg-green-500"
            />
            <QuickActionCard
              title="Compare Schemas"
              description="Source vs Target"
              icon={TableCellsIcon}
              href="/schema"
              color="bg-blue-500"
            />
            <QuickActionCard
              title="Execute Query"
              description="Run ad-hoc SQL"
              icon={CommandLineIcon}
              href="/query"
              color="bg-purple-500"
            />
            <QuickActionCard
              title="View History"
              description="Past validations"
              icon={ClockIcon}
              href="/history"
              color="bg-orange-500"
            />
          </div>
        </motion.div>
      </div>

      {/* Recent Validations */}
      {validationHistory.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className="rounded-xl bg-white p-6 shadow-sm border border-gray-100"
        >
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-lg font-semibold text-gray-900">Recent Validations</h2>
            <Link to="/history" className="text-sm text-primary-600 hover:text-primary-800">
              View all →
            </Link>
          </div>
          <div className="space-y-3">
            {validationHistory.slice(0, 5).map((validation, index) => (
              <div
                key={index}
                className="flex items-center justify-between rounded-lg border border-gray-100 p-4 hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center gap-4">
                  <StatusBadge status={validation.report.overall_status} />
                  <div>
                    <p className="font-medium text-gray-900">{validation.report.report_name}</p>
                    <p className="text-sm text-gray-500">
                      {new Date(validation.report.generated_at).toLocaleString()}
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-sm font-medium text-gray-900">
                    {validation.report.summary.passed}/{validation.report.summary.total_tests} passed
                  </p>
                  <p className="text-xs text-gray-500">
                    {validation.report.summary.pass_rate.toFixed(1)}% pass rate
                  </p>
                </div>
              </div>
            ))}
          </div>
        </motion.div>
      )}
    </div>
  );
}

function DatabaseCard({ name, status, tables }: { name: string; status: string; tables: number }) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-gray-100 p-4">
      <div className="flex items-center gap-3">
        <div className={`h-3 w-3 rounded-full ${status === 'connected' ? 'bg-green-500' : 'bg-red-500'}`} />
        <div>
          <p className="font-medium text-gray-900">{name}</p>
          <p className="text-sm text-gray-500">{tables} tables</p>
        </div>
      </div>
      <span className={`text-xs font-medium px-2 py-1 rounded-full ${
        status === 'connected' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
      }`}>
        {status}
      </span>
    </div>
  );
}

function QuickActionCard({ title, description, icon: Icon, href, color }: {
  title: string;
  description: string;
  icon: any;
  href: string;
  color: string;
}) {
  return (
    <Link
      to={href}
      className="group rounded-xl border border-gray-100 p-4 hover:border-primary-200 hover:bg-primary-50/50 transition-all"
    >
      <div className={`h-10 w-10 rounded-lg ${color} flex items-center justify-center mb-3`}>
        <Icon className="h-5 w-5 text-white" />
      </div>
      <h3 className="font-medium text-gray-900 group-hover:text-primary-700">{title}</h3>
      <p className="text-sm text-gray-500">{description}</p>
    </Link>
  );
}

function StatusBadge({ status }: { status: string }) {
  const styles = {
    passed: 'bg-green-100 text-green-700',
    failed: 'bg-red-100 text-red-700',
    partial: 'bg-yellow-100 text-yellow-700',
    error: 'bg-orange-100 text-orange-700',
  };

  return (
    <span className={`text-xs font-medium px-2.5 py-1 rounded-full ${styles[status as keyof typeof styles] || 'bg-gray-100 text-gray-700'}`}>
      {status.toUpperCase()}
    </span>
  );
}
