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
  sql: string;
  database: 'source' | 'target';
}

export interface SQLGenerationRequest {
  description: string;
  database: 'source' | 'target';
}

export interface TestResult {
  test_case_id: string;
  test_case_name: string;
  status: 'passed' | 'failed' | 'error' | 'skipped';
  duration_ms: number;
  message: string;
  source_rows?: number;
  target_rows?: number;
}

export interface ValidationSummary {
  total_tests: number;
  passed: number;
  failed: number;
  errors: number;
  skipped: number;
  pass_rate: number;
  duration_ms: number;
}

export interface ValidationReport {
  report_id: string;
  report_name: string;
  generated_at: string;
  overall_status: 'passed' | 'failed' | 'partial' | 'error';
  summary: ValidationSummary;
  scenarios_covered: number;
  total_scenarios: number;
  has_critical_failures: boolean;
}

export interface ValidationResponse {
  success: boolean;
  report: ValidationReport;
  markdown_report: string;
  test_results: TestResult[];
}

export interface SchemaInfo {
  database: string;
  tables: number;
  schema: Record<string, {
    columns: number;
    primary_keys: string[];
    row_count: number;
  }>;
}

export interface ExecutionProof {
  query_id: string;
  database: string;
  sql: string;
  execution_time_ms: number;
  row_count: number;
  sample_data: Record<string, any>[];
  column_names: string[];
  executed_at: string;
  success: boolean;
  error_message?: string;
}

export interface QueryResult {
  success: boolean;
  data: Record<string, any>[];
  row_count: number;
  proof: ExecutionProof;
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
  getSourceSchema: async (): Promise<{ success: boolean; schema: SchemaInfo }> => {
    const response = await api.get('/schema/source');
    return response.data;
  },

  // Get target schema
  getTargetSchema: async (): Promise<{ success: boolean; schema: SchemaInfo }> => {
    const response = await api.get('/schema/target');
    return response.data;
  },

  // Compare schemas
  compareSchemas: async (): Promise<any> => {
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
  generateSQL: async (request: SQLGenerationRequest): Promise<{ success: boolean; sql: string }> => {
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
