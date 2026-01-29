import { Outlet, NavLink, useLocation } from 'react-router-dom';
import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  HomeIcon,
  PlayIcon,
  TableCellsIcon,
  CommandLineIcon,
  ClockIcon,
  Bars3Icon,
  XMarkIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
} from '@heroicons/react/24/outline';
import { healthApi } from '../api';

const navigation = [
  { name: 'Dashboard', href: '/', icon: HomeIcon },
  { name: 'Validate', href: '/validate', icon: PlayIcon },
  { name: 'Schema', href: '/schema', icon: TableCellsIcon },
  { name: 'Query', href: '/query', icon: CommandLineIcon },
  { name: 'History', href: '/history', icon: ClockIcon },
];

export default function Layout() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [serverStatus, setServerStatus] = useState<'connected' | 'disconnected' | 'checking'>('checking');
  const location = useLocation();

  useEffect(() => {
    const checkHealth = async () => {
      try {
        await healthApi.check();
        setServerStatus('connected');
      } catch {
        setServerStatus('disconnected');
      }
    };

    checkHealth();
    const interval = setInterval(checkHealth, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
      {/* Mobile sidebar */}
      <AnimatePresence>
        {sidebarOpen && (
          <>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="fixed inset-0 z-40 bg-gray-900/50 lg:hidden"
              onClick={() => setSidebarOpen(false)}
            />
            <motion.div
              initial={{ x: -280 }}
              animate={{ x: 0 }}
              exit={{ x: -280 }}
              transition={{ type: 'spring', damping: 25 }}
              className="fixed inset-y-0 left-0 z-50 w-72 bg-white shadow-xl lg:hidden"
            >
              <Sidebar onClose={() => setSidebarOpen(false)} serverStatus={serverStatus} />
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Desktop sidebar */}
      <div className="hidden lg:fixed lg:inset-y-0 lg:left-0 lg:z-50 lg:block lg:w-72 lg:bg-white lg:shadow-lg">
        <Sidebar serverStatus={serverStatus} />
      </div>

      {/* Main content */}
      <div className="lg:pl-72">
        {/* Top bar */}
        <div className="sticky top-0 z-40 flex h-16 items-center gap-x-4 border-b border-gray-200 bg-white/80 backdrop-blur-lg px-4 shadow-sm sm:px-6 lg:px-8">
          <button
            type="button"
            className="lg:hidden -m-2.5 p-2.5 text-gray-700"
            onClick={() => setSidebarOpen(true)}
          >
            <Bars3Icon className="h-6 w-6" />
          </button>

          <div className="flex flex-1 items-center justify-between">
            <h1 className="text-lg font-semibold text-gray-900">
              {navigation.find(n => n.href === location.pathname)?.name || 'ETL Validator'}
            </h1>
            
            <div className="flex items-center gap-x-4">
              <div className="flex items-center gap-2">
                {serverStatus === 'connected' ? (
                  <>
                    <CheckCircleIcon className="h-5 w-5 text-green-500" />
                    <span className="text-sm text-green-600">Connected</span>
                  </>
                ) : serverStatus === 'disconnected' ? (
                  <>
                    <ExclamationCircleIcon className="h-5 w-5 text-red-500" />
                    <span className="text-sm text-red-600">Disconnected</span>
                  </>
                ) : (
                  <>
                    <div className="h-5 w-5 rounded-full border-2 border-gray-300 border-t-primary-500 animate-spin" />
                    <span className="text-sm text-gray-500">Checking...</span>
                  </>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Page content */}
        <main className="p-4 sm:p-6 lg:p-8">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

function Sidebar({ onClose, serverStatus }: { onClose?: () => void; serverStatus: string }) {
  const location = useLocation();

  return (
    <div className="flex h-full flex-col">
      {/* Logo */}
      <div className="flex h-16 items-center justify-between px-6 border-b border-gray-100">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-xl bg-gradient-to-br from-primary-500 to-primary-700 flex items-center justify-center">
            <svg className="h-5 w-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <div>
            <h1 className="text-lg font-bold text-gray-900">ETL Validator</h1>
            <p className="text-xs text-gray-500">AI-Powered Testing</p>
          </div>
        </div>
        {onClose && (
          <button onClick={onClose} className="lg:hidden p-2 text-gray-400 hover:text-gray-600">
            <XMarkIcon className="h-5 w-5" />
          </button>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-4 py-6 space-y-1">
        {navigation.map((item) => {
          const isActive = location.pathname === item.href;
          return (
            <NavLink
              key={item.name}
              to={item.href}
              onClick={onClose}
              className={`
                group flex items-center gap-3 rounded-xl px-4 py-3 text-sm font-medium transition-all duration-200
                ${isActive 
                  ? 'bg-primary-50 text-primary-700 shadow-sm' 
                  : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                }
              `}
            >
              <item.icon className={`h-5 w-5 ${isActive ? 'text-primary-600' : 'text-gray-400 group-hover:text-gray-600'}`} />
              {item.name}
              {isActive && (
                <motion.div
                  layoutId="activeNav"
                  className="ml-auto h-2 w-2 rounded-full bg-primary-500"
                />
              )}
            </NavLink>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="border-t border-gray-100 p-4">
        <div className="rounded-xl bg-gradient-to-br from-primary-50 to-indigo-50 p-4">
          <h3 className="text-sm font-semibold text-primary-900">Need Help?</h3>
          <p className="mt-1 text-xs text-primary-700">
            Check the documentation for guides and examples.
          </p>
          <a
            href="/docs"
            target="_blank"
            className="mt-3 inline-flex items-center text-xs font-medium text-primary-600 hover:text-primary-800"
          >
            View API Docs →
          </a>
        </div>
      </div>
    </div>
  );
}
