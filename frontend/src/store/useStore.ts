import { create } from 'zustand';

interface ReasoningStep {
  id: string;
  type: 'thought' | 'action' | 'result' | 'error';
  content: string;
  timestamp: string;
  metadata?: any;
}

type ViewMode = 'pipelines' | 'reasoning' | 'rules' | 'visualizer' | 'monitoring' | 'query-regeneration' | 'knowledge-register' | 'metadata-register';

interface StoreState {
  reasoningSteps: ReasoningStep[];
  addReasoningStep: (step: ReasoningStep) => void;
  clearReasoning: () => void;
  sidebarOpen: boolean;
  sidebarWidth: number;
  viewMode: ViewMode;
  setViewMode: (mode: ViewMode) => void;
  setSidebarOpen: (open: boolean) => void;
  setSidebarWidth: (width: number) => void;
}

export const useStore = create<StoreState>((set) => ({
  reasoningSteps: [],
  addReasoningStep: (step: ReasoningStep) =>
    set((state) => ({
      reasoningSteps: [...state.reasoningSteps, step],
    })),
  clearReasoning: () => set({ reasoningSteps: [] }),
  sidebarOpen: true,
  sidebarWidth: 250,
  viewMode: 'query-regeneration',
  setViewMode: (mode: ViewMode) => set({ viewMode: mode }),
  setSidebarOpen: (open: boolean) => set({ sidebarOpen: open }),
  setSidebarWidth: (width: number) => set({ sidebarWidth: width }),
}));

