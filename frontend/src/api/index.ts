import axios from 'axios';

const API_BASE = '/api/v1';

const api = axios.create({
  baseURL: API_BASE,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Types
export interface ValidationRequest {
  business_rules: string;
  validation_name?: string;
}

export interface QueryRequest {
  query: string;
  database: 'source' | 'target';
}

export interface SQLGenerationRequest {
  natural_language: string;
  database: 'source' | 'target';
}

export interface TestResult {
  test_id: string;
  test_name: string;
  description: string;
  status: 'passed' | 'failed' | 'error';
  source_query?: string;
  target_query?: string;
  execution_time?: number;
  error_message?: string;
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
  errors: number;
  overall_status: 'passed' | 'failed' | 'partial';
  pass_rate: number;
  total_execution_time: number;
}

export interface ValidationReport {
  validation_id: string;
  report_name: string;
  generated_at: string;
  overall_status: 'passed' | 'failed' | 'partial';
  summary: ValidationSummary;
  test_results: TestResult[];
  markdown_report?: string;
}

export interface ValidationResponse {
  success: boolean;
  report: ValidationReport;
  // These may be at root level in some API versions
  test_results?: TestResult[];
  markdown_report?: string;
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

export interface QueryResult {
  success: boolean;
  rows: Record<string, any>[];
  row_count: number;
  execution_time: number;
  error?: string;
}

// API Functions
export const validationApi = {
  // Run full validation
  validate: async (request: ValidationRequest): Promise<ValidationResponse> => {
    const response = await api.post('/validate', request);
    return response.data;
  },

  // Run quick validation
  quickValidate: async (rule: string): Promise<any> => {
    const response = await api.post('/validate/quick', { rule });
    return response.data;
  },

  // Stream validation (SSE)
  validateStream: (request: ValidationRequest, onMessage: (data: any) => void): EventSource => {
    const eventSource = new EventSource(
      `${API_BASE}/validate/stream?business_rules=${encodeURIComponent(request.business_rules)}&validation_name=${encodeURIComponent(request.validation_name || '')}`
    );
    
    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      onMessage(data);
    };

    return eventSource;
  },
};

export const schemaApi = {
  // Get source schema
  getSource: async (): Promise<SchemaInfo> => {
    const response = await api.get('/schema/source');
    return response.data.schema || response.data;
  },

  // Get target schema
  getTarget: async (): Promise<SchemaInfo> => {
    const response = await api.get('/schema/target');
    return response.data.schema || response.data;
  },

  // Compare schemas
  compare: async (): Promise<any> => {
    const response = await api.get('/schema/compare');
    return response.data;
  },

  // Get database info
  getDatabaseInfo: async (): Promise<any> => {
    const response = await api.get('/databases/info');
    return response.data;
  },
};

export const queryApi = {
  // Execute query
  execute: async (request: QueryRequest): Promise<QueryResult> => {
    const response = await api.post('/query/execute', request);
    return response.data;
  },

  // Generate SQL
  generate: async (request: SQLGenerationRequest): Promise<{ sql: string }> => {
    const response = await api.post('/query/generate', request);
    return response.data;
  },
};

export const healthApi = {
  check: async (): Promise<{ status: string; service: string; version: string }> => {
    const response = await axios.get('/health');
    return response.data;
  },

  getStatus: async (): Promise<any> => {
    const response = await api.get('/status');
    return response.data;
  },
};

export default api;
