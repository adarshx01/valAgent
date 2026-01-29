import { create } from 'zustand';
import { ValidationResponse, TestResult, SchemaInfo } from '../api';

interface ValidationState {
  // Current validation
  isValidating: boolean;
  progress: number;
  progressMessage: string;
  
  // Results
  currentReport: ValidationResponse | null;
  validationHistory: ValidationResponse[];
  
  // Schema
  sourceSchema: SchemaInfo | null;
  targetSchema: SchemaInfo | null;
  
  // Actions
  setValidating: (isValidating: boolean) => void;
  setProgress: (progress: number, message: string) => void;
  setCurrentReport: (report: ValidationResponse | null) => void;
  addToHistory: (report: ValidationResponse) => void;
  setSourceSchema: (schema: SchemaInfo | null) => void;
  setTargetSchema: (schema: SchemaInfo | null) => void;
  reset: () => void;
}

export const useValidationStore = create<ValidationState>((set) => ({
  // Initial state
  isValidating: false,
  progress: 0,
  progressMessage: '',
  currentReport: null,
  validationHistory: [],
  sourceSchema: null,
  targetSchema: null,

  // Actions
  setValidating: (isValidating) => set({ isValidating }),
  
  setProgress: (progress, message) => set({ 
    progress, 
    progressMessage: message 
  }),
  
  setCurrentReport: (report) => set({ currentReport: report }),
  
  addToHistory: (report) => set((state) => ({
    validationHistory: [report, ...state.validationHistory].slice(0, 50), // Keep last 50
  })),
  
  setSourceSchema: (schema) => set({ sourceSchema: schema }),
  
  setTargetSchema: (schema) => set({ targetSchema: schema }),
  
  reset: () => set({
    isValidating: false,
    progress: 0,
    progressMessage: '',
    currentReport: null,
  }),
}));
