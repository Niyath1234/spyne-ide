import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || '';

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Pipeline API
export const pipelineAPI = {
  list: () => apiClient.get('/api/pipelines'),
  create: (pipeline: any) => apiClient.post('/api/pipelines', pipeline),
  update: (id: string, pipeline: any) => apiClient.put(`/api/pipelines/${id}`, pipeline),
  delete: (id: string) => apiClient.delete(`/api/pipelines/${id}`),
  run: (id: string) => apiClient.post(`/api/pipelines/${id}/run`),
  status: (id: string) => apiClient.get(`/api/pipelines/${id}/status`),
};

// Reasoning API
export const reasoningAPI = {
  // Original query endpoint (direct execution)
  query: (query: string, context?: any) =>
    apiClient.post('/api/reasoning/query', { query, context }),
  
  // NEW: Assess query confidence (fail-fast check)
  // Returns clarification request if confidence is low
  assess: (query: string) =>
    apiClient.post('/api/reasoning/assess', { query }),
  
  // NEW: Submit clarification answer
  // Combines original query with user's answer
  clarify: (query: string, answer: string) =>
    apiClient.post('/api/reasoning/clarify', { query, answer }),
  
  // Streaming responses
  stream: (query: string) => {
    // For streaming responses
    return fetch(`${API_BASE_URL}/api/reasoning/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });
  },
};

// Types for clarification flow
export interface ClarificationRequest {
  status: 'needs_clarification';
  needs_clarification: true;
  question: string;
  missing_pieces: Array<{
    field: string;
    description: string;
    importance: 'Required' | 'Helpful';
    suggestions: string[];
  }>;
  confidence: number;
  partial_understanding: {
    task_type: string | null;
    systems: string[];
    metrics: string[];
    entities: string[];
    grain: string[];
    keywords: string[];
  };
  response_hints: string[];
}

export interface AssessmentSuccess {
  status: 'success';
  needs_clarification: false;
  intent: any;
  message: string;
}

export interface AssessmentFailed {
  status: 'failed';
  needs_clarification: false;
  error: string;
}

export type AssessmentResponse = ClarificationRequest | AssessmentSuccess | AssessmentFailed;

// Ingestion API
export const ingestionAPI = {
  ingest: (config: any) => apiClient.post('/api/ingestion/ingest', config),
  validate: (config: any) => apiClient.post('/api/ingestion/validate', config),
  preview: (config: any) => apiClient.post('/api/ingestion/preview', config),
  uploadCsv: (formData: FormData) => {
    return apiClient.post('/api/upload/csv', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
  },
};

// Rules API
export const rulesAPI = {
  list: () => apiClient.get('/api/rules'),
  get: (id: string) => apiClient.get(`/api/rules/${id}`),
  create: (rule: any) => apiClient.post('/api/rules', rule),
  update: (id: string, rule: any) => apiClient.put(`/api/rules/${id}`, rule),
  delete: (id: string) => apiClient.delete(`/api/rules/${id}`),
};

