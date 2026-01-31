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
    const errorResponse = {
      success: false,
      error: error.message || 'Unknown error',
      status: error.response?.status,
      request_id: error.config?.headers?.['X-Request-ID'],
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

// Export apiClient for advanced usage
export { apiClient, API_BASE_URL };

