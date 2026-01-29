import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export interface QueryResult {
  row_count: number;
  execution_time_ms: number;
  sample_data: Record<string, any>[];
  columns: string[];
  success: boolean;
  error?: string | null;
}

export interface TestResult {
  test_id?: string;
  test_case_id?: string;
  test_name?: string;
  test_case_name?: string;
  description?: string;
  message?: string;
  status: 'passed' | 'failed' | 'error' | 'skipped';
  source_query?: string;
  target_query?: string;
  source_result?: QueryResult | null;
  target_result?: QueryResult | null;
  execution_time?: number;
  duration_ms?: number;
  error_message?: string;
  source_rows?: number | null;
  target_rows?: number | null;
  execution_proof?: {
    source_row_count?: number;
    target_row_count?: number;
    source_execution_time?: number;
    target_execution_time?: number;
    source_sample?: any[];
    target_sample?: any[];
  };
}

export interface ValidationSummary {
  total_tests: number;
  passed: number;
  failed: number;
  errors?: number;
  skipped?: number;
  pass_rate: number;
  total_execution_time?: number;
  duration_ms?: number;
}

export interface ValidationReport {
  validation_id?: string;
  report_id?: string;
  report_name: string;
  generated_at: string;
  overall_status: 'passed' | 'failed' | 'partial' | 'error';
  summary: ValidationSummary;
  test_results: TestResult[];
  markdown_report?: string;
  scenarios_covered?: number;
  total_scenarios?: number;
  has_critical_failures?: boolean;
}

export interface ValidationHistoryItem {
  name: string;
  rules: string;
  report: ValidationReport;
  timestamp: string;
}

export interface SchemaInfo {
  database: string;
  tables: {
    table_name: string;
    columns: {
      column_name: string;
      data_type: string;
      is_nullable: boolean;
      column_default?: string;
    }[];
  }[];
}

interface ValidationState {
  // Current validation
  isValidating: boolean;
  progress: number;
  currentStep: string;
  
  // Results
  currentReport: ValidationReport | null;
  validationHistory: ValidationHistoryItem[];
  
  // Schema
  sourceSchema: SchemaInfo | null;
  targetSchema: SchemaInfo | null;
  
  // Actions
  startValidation: () => void;
  updateProgress: (progress: number) => void;
  setCurrentStep: (step: string) => void;
  completeValidation: (report: ValidationReport | null) => void;
  setCurrentReport: (report: ValidationReport | null) => void;
  addToHistory: (item: ValidationHistoryItem) => void;
  clearHistory: () => void;
  setSourceSchema: (schema: SchemaInfo | null) => void;
  setTargetSchema: (schema: SchemaInfo | null) => void;
  reset: () => void;
}

export const useValidationStore = create<ValidationState>()(
  persist(
    (set) => ({
      // Initial state
      isValidating: false,
      progress: 0,
      currentStep: '',
      currentReport: null,
      validationHistory: [],
      sourceSchema: null,
      targetSchema: null,

      // Actions
      startValidation: () => set({
        isValidating: true,
        progress: 0,
        currentStep: 'Initializing...',
        currentReport: null,
      }),

      updateProgress: (progress) => set({ progress }),

      setCurrentStep: (step) => set({ currentStep: step }),

      completeValidation: (report) => set({
        isValidating: false,
        progress: 100,
        currentStep: 'Complete',
        currentReport: report,
      }),

      setCurrentReport: (report) => set({ currentReport: report }),

      addToHistory: (item) => set((state) => ({
        validationHistory: [item, ...state.validationHistory].slice(0, 100),
      })),

      clearHistory: () => set({ validationHistory: [] }),

      setSourceSchema: (schema) => set({ sourceSchema: schema }),

      setTargetSchema: (schema) => set({ targetSchema: schema }),

      reset: () => set({
        isValidating: false,
        progress: 0,
        currentStep: '',
        currentReport: null,
      }),
    }),
    {
      name: 'etl-validator-storage',
      partialize: (state) => ({
        validationHistory: state.validationHistory,
      }),
    }
  )
);
