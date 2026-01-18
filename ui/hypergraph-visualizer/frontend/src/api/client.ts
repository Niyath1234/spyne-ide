import axios from 'axios';

const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

const api = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
});

export interface GraphNode {
  id: string;
  label: string;
  type: 'table';
  row_count?: number;
  columns?: string[];
  title?: string;
}

export interface GraphEdge {
  id: string;
  from: string;
  to: string;
  label: string;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
  stats: {
    total_nodes: number;
    total_edges: number;
    table_count: number;
    column_count: number;
  };
}

export async function getGraphData(): Promise<GraphData> {
  try {
    const response = await api.get<GraphData>('/graph');
    return response.data;
  } catch (error: any) {
    console.error('Failed to fetch graph data:', error);
    throw new Error(error.response?.data?.error || error.message || 'Failed to load graph data');
  }
}

