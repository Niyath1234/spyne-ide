import { ThemeProvider, CssBaseline, Box } from '@mui/material';
import { theme } from './theme';
import { TopBar } from './components/TopBar';
import { Sidebar } from './components/Sidebar';
import { PipelineManager } from './components/PipelineManager';
import { ReasoningChat } from './components/ReasoningChat';
import { Monitoring } from './components/Monitoring';
import { RulesView } from './components/RulesView';
import { HypergraphVisualizer } from './components/HypergraphVisualizer';
import { CursorLikeQueryBuilder } from './components/CursorLikeQueryBuilder';
import { KnowledgeRegister } from './components/KnowledgeRegister';
import { MetadataRegister } from './components/MetadataRegister';
import { useStore } from './store/useStore';

function App() {
  const { sidebarOpen, sidebarWidth, viewMode } = useStore();

  const renderContent = () => {
    switch (viewMode) {
      case 'pipelines':
        return <PipelineManager />;
      case 'reasoning':
        return <ReasoningChat />;
      case 'rules':
        return <RulesView />;
      case 'visualizer':
        return <HypergraphVisualizer />;
      case 'monitoring':
        return <Monitoring />;
      case 'query-regeneration':
        return <CursorLikeQueryBuilder />;
      case 'knowledge-register':
        return <KnowledgeRegister />;
      case 'metadata-register':
        return <MetadataRegister />;
      default:
        return <CursorLikeQueryBuilder />;
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden' }}>
        <TopBar />
        <Box sx={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
          {sidebarOpen && <Sidebar width={sidebarWidth} />}
          <Box
            sx={{
              flex: 1,
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden',
              backgroundColor: '#0D1117',
            }}
          >
            {renderContent()}
          </Box>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

export default App;
