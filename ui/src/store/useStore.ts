import { create } from 'zustand';

export interface Pipeline {
  id: string;
  name: string;
  type: 'csv' | 'json' | 'parquet' | 'database';
  source: string;
  destination?: string; // Optional - system will handle automatically
  status: 'active' | 'inactive' | 'error';
  lastRun?: string;
  config: Record<string, any>;
}

export interface ReasoningStep {
  id: string;
  type: 'thought' | 'action' | 'result' | 'error';
  content: string;
  timestamp: string;
  metadata?: Record<string, any>;
}

interface AppState {
  // Sidebar state
  sidebarOpen: boolean;
  sidebarWidth: number;
  setSidebarOpen: (open: boolean) => void;
  setSidebarWidth: (width: number) => void;

  // Pipelines
  pipelines: Pipeline[];
  activePipelineId: string | null;
  addPipeline: (pipeline: Pipeline) => void;
  updatePipeline: (id: string, updates: Partial<Pipeline>) => void;
  deletePipeline: (id: string) => void;
  setActivePipeline: (id: string | null) => void;
  setPipelines: (pipelines: Pipeline[]) => void;

  // Reasoning/Chain of Thought
  reasoningSteps: ReasoningStep[];
  addReasoningStep: (step: ReasoningStep) => void;
  clearReasoning: () => void;

  // View mode
  viewMode: 'pipelines' | 'reasoning' | 'rules' | 'monitoring' | 'visualizer';
  setViewMode: (mode: 'pipelines' | 'reasoning' | 'rules' | 'monitoring' | 'visualizer') => void;
}

export const useStore = create<AppState>((set) => ({
  // Sidebar
  sidebarOpen: true,
  sidebarWidth: 250,
  setSidebarOpen: (open) => set({ sidebarOpen: open }),
  setSidebarWidth: (width) => set({ sidebarWidth: Math.max(200, Math.min(600, width)) }),

  // Pipelines
  pipelines: [],
  activePipelineId: null,
  addPipeline: (pipeline) => set((state) => ({ pipelines: [...state.pipelines, pipeline] })),
  updatePipeline: (id, updates) =>
    set((state) => ({
      pipelines: state.pipelines.map((p) => (p.id === id ? { ...p, ...updates } : p)),
    })),
  deletePipeline: (id) =>
    set((state) => ({
      pipelines: state.pipelines.filter((p) => p.id !== id),
      activePipelineId: state.activePipelineId === id ? null : state.activePipelineId,
    })),
  setActivePipeline: (id) => set({ activePipelineId: id }),
  setPipelines: (pipelines) => set({ pipelines }),

  // Reasoning
  reasoningSteps: [],
  addReasoningStep: (step) =>
    set((state) => ({ reasoningSteps: [...state.reasoningSteps, step] })),
  clearReasoning: () => set({ reasoningSteps: [] }),

  // View mode
  viewMode: 'pipelines',
  setViewMode: (mode) => set({ viewMode: mode }),
}));

