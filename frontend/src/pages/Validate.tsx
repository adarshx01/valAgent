import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import {
  PlayIcon,
  SparklesIcon,
  ExclamationTriangleIcon,
  DocumentTextIcon,
  ArrowPathIcon,
} from '@heroicons/react/24/outline';
import { validationApi } from '../api';
import { useValidationStore } from '../store/validationStore';

const exampleRules = `1. All customer IDs in the target should exist in the source
2. Total order amounts should match between source and target after currency conversion
3. Customer names should not be null in the target
4. Date fields should be in ISO format
5. Product SKUs should follow the pattern XXX-####-XX
6. Email addresses should be valid and lowercase`;

export default function Validate() {
  const [validationName, setValidationName] = useState('');
  const [businessRules, setBusinessRules] = useState('');
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();
  
  const { 
    isValidating, 
    progress, 
    currentStep,
    startValidation, 
    updateProgress, 
    setCurrentStep,
    completeValidation,
    addToHistory,
  } = useValidationStore();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!businessRules.trim()) {
      setError('Please enter business rules to validate');
      return;
    }

    setError(null);
    startValidation();

    const steps = [
      'Analyzing business rules...',
      'Extracting database schemas...',
      'Generating test cases with AI...',
      'Executing queries in parallel...',
      'Collecting execution proofs...',
      'Generating validation report...',
    ];

    try {
      // Simulate progress updates for streaming
      let stepIndex = 0;
      const progressInterval = setInterval(() => {
        if (stepIndex < steps.length) {
          setCurrentStep(steps[stepIndex]);
          updateProgress((stepIndex + 1) * (100 / steps.length));
          stepIndex++;
        }
      }, 2000);

      const response = await validationApi.validate({
        business_rules: businessRules,
        validation_name: validationName || 'Validation ' + new Date().toISOString(),
      });

      clearInterval(progressInterval);
      
      // Merge report data - test_results and markdown_report may be at root level
      const reportData = {
        ...response.report,
        test_results: response.test_results || response.report?.test_results || [],
        markdown_report: response.markdown_report || response.report?.markdown_report,
      };
      
      completeValidation(reportData);
      addToHistory({
        name: validationName || 'Validation',
        rules: businessRules,
        report: reportData,
        timestamp: new Date().toISOString(),
      });

      navigate('/results');
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Validation failed. Please try again.');
      completeValidation(null);
    }
  };

  const loadExample = () => {
    setBusinessRules(exampleRules);
    setValidationName('Sample Validation');
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
      >
        <h1 className="text-2xl font-bold text-gray-900">Run Validation</h1>
        <p className="mt-2 text-gray-600">
          Enter your business rules in natural language. The AI agent will automatically
          generate SQL queries and execute comprehensive tests.
        </p>
      </motion.div>

      {/* Form */}
      <motion.form
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        onSubmit={handleSubmit}
        className="space-y-6"
      >
        {/* Validation Name */}
        <div>
          <label htmlFor="name" className="block text-sm font-medium text-gray-700 mb-2">
            Validation Name (Optional)
          </label>
          <input
            type="text"
            id="name"
            value={validationName}
            onChange={(e) => setValidationName(e.target.value)}
            placeholder="e.g., Customer Data Migration - Phase 1"
            className="w-full rounded-xl border border-gray-200 px-4 py-3 text-gray-900 placeholder:text-gray-400 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 transition-all"
            disabled={isValidating}
          />
        </div>

        {/* Business Rules */}
        <div>
          <div className="flex items-center justify-between mb-2">
            <label htmlFor="rules" className="block text-sm font-medium text-gray-700">
              Business Rules
            </label>
            <button
              type="button"
              onClick={loadExample}
              className="inline-flex items-center gap-1 text-sm text-primary-600 hover:text-primary-800"
              disabled={isValidating}
            >
              <DocumentTextIcon className="h-4 w-4" />
              Load Example
            </button>
          </div>
          <textarea
            id="rules"
            value={businessRules}
            onChange={(e) => setBusinessRules(e.target.value)}
            placeholder={`Enter your business rules in natural language, for example:\n\n1. All customer emails should be unique\n2. Order dates cannot be in the future\n3. Product prices must be positive\n4. Foreign key relationships must be maintained`}
            rows={12}
            className="w-full rounded-xl border border-gray-200 px-4 py-3 text-gray-900 placeholder:text-gray-400 focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 transition-all font-mono text-sm"
            disabled={isValidating}
          />
        </div>

        {/* Error Message */}
        <AnimatePresence>
          {error && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="rounded-xl bg-red-50 border border-red-200 p-4"
            >
              <div className="flex items-center gap-3">
                <ExclamationTriangleIcon className="h-5 w-5 text-red-500" />
                <p className="text-sm text-red-700">{error}</p>
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Progress Section */}
        <AnimatePresence>
          {isValidating && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="rounded-xl bg-primary-50 border border-primary-200 p-6"
            >
              <div className="flex items-center gap-4 mb-4">
                <div className="relative">
                  <SparklesIcon className="h-8 w-8 text-primary-500" />
                  <div className="absolute inset-0 animate-ping">
                    <SparklesIcon className="h-8 w-8 text-primary-400 opacity-50" />
                  </div>
                </div>
                <div>
                  <p className="font-semibold text-primary-900">AI Agent Processing</p>
                  <p className="text-sm text-primary-700">{currentStep}</p>
                </div>
              </div>
              
              {/* Progress Bar */}
              <div className="relative h-3 bg-primary-100 rounded-full overflow-hidden">
                <motion.div
                  className="absolute inset-y-0 left-0 bg-gradient-to-r from-primary-500 to-indigo-500 rounded-full"
                  initial={{ width: 0 }}
                  animate={{ width: `${progress}%` }}
                  transition={{ duration: 0.5 }}
                />
                <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent animate-shimmer" />
              </div>
              
              <p className="text-right text-sm text-primary-600 mt-2">{Math.round(progress)}% complete</p>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Submit Button */}
        <div className="flex justify-end gap-4">
          <button
            type="button"
            onClick={() => {
              setBusinessRules('');
              setValidationName('');
              setError(null);
            }}
            className="px-6 py-3 text-sm font-medium text-gray-700 hover:text-gray-900 transition-colors"
            disabled={isValidating}
          >
            Clear
          </button>
          <button
            type="submit"
            disabled={isValidating}
            className="inline-flex items-center gap-2 rounded-xl bg-gradient-to-r from-primary-600 to-indigo-600 px-8 py-3 text-sm font-semibold text-white shadow-lg hover:from-primary-700 hover:to-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
          >
            {isValidating ? (
              <>
                <ArrowPathIcon className="h-5 w-5 animate-spin" />
                Validating...
              </>
            ) : (
              <>
                <PlayIcon className="h-5 w-5" />
                Start Validation
              </>
            )}
          </button>
        </div>
      </motion.form>

      {/* Help Section */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.3 }}
        className="rounded-xl bg-gray-50 border border-gray-100 p-6"
      >
        <h3 className="text-sm font-semibold text-gray-900 mb-3">Tips for Writing Business Rules</h3>
        <ul className="space-y-2 text-sm text-gray-600">
          <li className="flex items-start gap-2">
            <span className="text-primary-500 mt-1">•</span>
            <span>Be specific about which tables and columns you're referring to</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-primary-500 mt-1">•</span>
            <span>Include data type expectations (dates, numbers, strings)</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-primary-500 mt-1">•</span>
            <span>Mention any transformations that should have occurred</span>
          </li>
          <li className="flex items-start gap-2">
            <span className="text-primary-500 mt-1">•</span>
            <span>Specify relationships between source and target data</span>
          </li>
        </ul>
      </motion.div>
    </div>
  );
}
