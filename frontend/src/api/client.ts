import axios, { AxiosError } from 'axios';
import type { AxiosInstance } from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080';

// Create axios instance with default config
const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: 120000, // 2 minutes for LLM queries
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for adding request ID
apiClient.interceptors.request.use((config) => {
  config.headers['X-Request-ID'] = `ui-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  return config;
});

// Response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    // Check for CORS errors
    const isCorsError = !error.response && (
      error.message?.includes('CORS') ||
      error.message?.includes('Access-Control') ||
      error.message?.includes('Network Error') ||
      error.code === 'ERR_NETWORK' ||
      error.code === 'ERR_FAILED'
    );
    
    const errorResponse: any = {
      success: false,
      error: isCorsError 
        ? 'CORS error: Backend is not allowing requests from this origin. Make sure the backend CORS configuration includes this frontend origin.'
        : error.message || 'Unknown error',
      status: error.response?.status,
      request_id: error.config?.headers?.['X-Request-ID'],
      code: error.code,
      isCorsError,
    };
    
    if (error.response?.data && typeof error.response.data === 'object') {
      Object.assign(errorResponse, error.response.data);
    }
    
    console.error('API Error:', errorResponse);
    return Promise.reject(errorResponse);
  }
);

// Health check API
export const healthAPI = {
  check: async (): Promise<{ status: string; version?: string; timestamp?: string }> => {
    const response = await apiClient.get('/api/v1/health');
    return response.data;
  },
  detailed: async (): Promise<any> => {
    const response = await apiClient.get('/api/v1/health/detailed');
    return response.data;
  },
  ready: async (): Promise<{ status: string }> => {
    const response = await apiClient.get('/api/v1/health/ready');
    return response.data;
  },
};

// Metrics API
export const metricsAPI = {
  get: async (): Promise<any> => {
    const response = await apiClient.get('/api/v1/metrics');
    return response.data;
  },
};

export interface AgentResponse {
  status: string;
  message?: string;
  choices?: Array<{ id: string; label: string; description?: string }>;
  error?: string;
  data?: any;
  trace?: Array<{
    event_type: string;
    payload?: any;
  }>;
  clarification?: ClarificationRequest;
  final_answer?: string;
}

export interface ClarificationRequest {
  query: string;
  answer?: string;
  question?: string;
  confidence?: number;
  partial_understanding?: {
    task_type?: string;
    metrics?: string[];
    systems?: string[];
  };
  missing_pieces?: Array<{
    field: string;
    importance: string;
    description: string;
    suggestions?: string[];
  }>;
  response_hints?: string[];
  choices?: Array<{ id: string; label: string; score?: number }>;
}

export const agentAPI = {
  run: async (sessionId: string, query: string, uiContext?: any): Promise<AgentResponse> => {
    const response = await apiClient.post('/api/agent/run', {
      session_id: sessionId,
      user_query: query,
      ui_context: uiContext || {},
    });
    return response.data;
  },
  continue: async (sessionId: string, choiceId: string, uiContext?: any): Promise<AgentResponse> => {
    const response = await apiClient.post('/api/agent/continue', {
      session_id: sessionId,
      choice_id: choiceId,
      ui_context: uiContext || {},
    });
    return response.data;
  },
};

export const reasoningAPI = {
  assess: async (query: string): Promise<any> => {
    const response = await apiClient.post('/api/reasoning/assess', {
      query,
    });
    return response.data;
  },
  clarify: async (query: string, answer: string): Promise<any> => {
    const response = await apiClient.post('/api/reasoning/clarify', {
      query,
      answer,
    });
    return response.data;
  },
  query: async (query: string): Promise<any> => {
    const response = await apiClient.post('/api/reasoning/query', {
      query,
    });
    return response.data;
  },
};

export const assistantAPI = {
  ask: async (question: string): Promise<any> => {
    const response = await apiClient.post('/api/assistant/ask', {
      question,
    });
    return response.data;
  },
};

export interface QueryGenerationResult {
  success: boolean;
  sql?: string;
  metric?: {
    name: string;
    description: string;
  } | null;
  dimensions?: Array<{
    name: string;
    description: string;
  }>;
  joins?: Array<{
    from_table: string;
    to_table: string;
    on: string;
  }>;
  filters?: string[] | Array<Record<string, any>>;
  error?: string;
  suggestion?: string;
  business_rules_applied?: string[];
  reasoning_steps?: string[];
  method?: string;
  intent?: Record<string, any>;
}

export interface PrerequisitesResult {
  success: boolean;
  metadata?: {
    semantic_registry: any;
    tables: any;
  };
  loaded?: {
    metrics: number;
    dimensions: number;
    tables: number;
  };
  error?: string;
}

export const queryAPI = {
  loadPrerequisites: async (): Promise<PrerequisitesResult> => {
    const response = await apiClient.get('/api/query/load-prerequisites');
    return response.data;
  },
  generateSQL: async (query: string, useLLM: boolean = true): Promise<QueryGenerationResult> => {
    const response = await apiClient.post('/api/query/generate-sql', {
      query,
      use_llm: useLLM,
    });
    return response.data;
  },
};

// Pipelines API
export const pipelinesAPI = {
  list: async (): Promise<any[]> => {
    const response = await apiClient.get('/api/pipelines');
    return response.data;
  },
  create: async (data: any): Promise<any> => {
    const response = await apiClient.post('/api/pipelines', data);
    return response.data;
  },
  get: async (id: string): Promise<any> => {
    const response = await apiClient.get(`/api/pipelines/${id}`);
    return response.data;
  },
  update: async (id: string, data: any): Promise<any> => {
    const response = await apiClient.put(`/api/pipelines/${id}`, data);
    return response.data;
  },
  delete: async (id: string): Promise<any> => {
    const response = await apiClient.delete(`/api/pipelines/${id}`);
    return response.data;
  },
  run: async (id: string): Promise<any> => {
    const response = await apiClient.post(`/api/pipelines/${id}/run`);
    return response.data;
  },
  status: async (id: string): Promise<any> => {
    const response = await apiClient.get(`/api/pipelines/${id}/status`);
    return response.data;
  },
};

// Rules API
export const rulesAPI = {
  list: async (): Promise<{ rules: any[] }> => {
    const response = await apiClient.get('/api/rules');
    return response.data;
  },
  create: async (data: any): Promise<any> => {
    const response = await apiClient.post('/api/rules', data);
    return response.data;
  },
  get: async (id: string): Promise<any> => {
    const response = await apiClient.get(`/api/rules/${id}`);
    return response.data;
  },
  update: async (id: string, data: any): Promise<any> => {
    const response = await apiClient.put(`/api/rules/${id}`, data);
    return response.data;
  },
  delete: async (id: string): Promise<any> => {
    const response = await apiClient.delete(`/api/rules/${id}`);
    return response.data;
  },
};

// Metadata Ingestion API
export const metadataAPI = {
  ingestTable: async (tableDescription: string, system?: string): Promise<any> => {
    const response = await apiClient.post('/api/metadata/ingest/table', {
      table_description: tableDescription,
      system,
    });
    return response.data;
  },
  ingestJoin: async (joinCondition: string): Promise<any> => {
    const response = await apiClient.post('/api/metadata/ingest/join', {
      join_condition: joinCondition,
    });
    return response.data;
  },
  ingestRules: async (rulesText: string): Promise<any> => {
    const response = await apiClient.post('/api/metadata/ingest/rules', {
      rules_text: rulesText,
    });
    return response.data;
  },
  ingestComplete: async (metadataText: string, system?: string): Promise<any> => {
    const response = await apiClient.post('/api/metadata/ingest/complete', {
      metadata_text: metadataText,
      system,
    });
    return response.data;
  },
};

// Notebook API
export interface NotebookCell {
  id: string;
  sql: string;
  status?: 'idle' | 'running' | 'success' | 'error';
  result?: {
    schema: Array<{ name: string; type: string }>;
    rows: any[][];
    row_count: number;
    execution_time_ms: number;
  };
  error?: string;
}

export interface Notebook {
  id: string;
  engine: string;
  cells: NotebookCell[];
  metadata?: Record<string, any>;
  created_at?: string;
  updated_at?: string;
}

export const notebookAPI = {
  create: async (notebook: Partial<Notebook>): Promise<{ success: boolean; notebook: Notebook; error?: string }> => {
    try {
      const response = await apiClient.post('/api/v1/notebooks', notebook);
      return response.data;
    } catch (err: any) {
      console.error('Notebook API create error:', err);
      throw err;
    }
  },
  get: async (notebookId: string): Promise<{ success: boolean; notebook: Notebook; error?: string }> => {
    try {
      const response = await apiClient.get(`/api/v1/notebooks/${notebookId}`);
      return response.data;
    } catch (err: any) {
      // 404 is expected if notebook doesn't exist
      if (err.response?.status === 404) {
        return { success: false, notebook: {} as Notebook, error: 'Notebook not found' };
      }
      console.error('Notebook API get error:', err);
      throw err;
    }
  },
  list: async (): Promise<{ success: boolean; notebooks: Notebook[] }> => {
    const response = await apiClient.get('/api/v1/notebooks');
    return response.data;
  },
  update: async (notebookId: string, notebook: Partial<Notebook>): Promise<{ success: boolean; notebook: Notebook; error?: string }> => {
    const response = await apiClient.put(`/api/v1/notebooks/${notebookId}`, notebook);
    return response.data;
  },
  compile: async (notebookId: string, cellId?: string): Promise<{ success: boolean; sql: string; error?: string }> => {
    const response = await apiClient.post(`/api/v1/notebooks/${notebookId}/compile`, { cell_id: cellId });
    return response.data;
  },
  execute: async (notebookId: string, data: { cell_id: string }): Promise<{ success: boolean; cell_id: string; result?: any; compiled_sql?: string; error?: string }> => {
    const response = await apiClient.post(`/api/v1/notebooks/${notebookId}/execute`, data);
    return response.data;
  },
  generateSQL: async (notebookId: string, cellId: string, data: { query: string }): Promise<{ success: boolean; sql?: string; cell_id?: string; error?: string }> => {
    const response = await apiClient.post(`/api/v1/notebooks/${notebookId}/cells/${cellId}/generate-sql`, data);
    return response.data;
  },
};

// Export apiClient for advanced usage
export { apiClient, API_BASE_URL };

