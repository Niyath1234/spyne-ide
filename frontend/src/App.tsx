import { ThemeProvider, CssBaseline, Box } from '@mui/material';
import { useIsAuthenticated, useMsal } from '@azure/msal-react';
import { useEffect } from 'react';
import { theme } from './theme';
import { Sidebar } from './components/Sidebar';
import { PipelineManager } from './components/PipelineManager';
import { ReasoningChat } from './components/ReasoningChat';
import { Monitoring } from './components/Monitoring';
import { RulesView } from './components/RulesView';
import { HypergraphVisualizer } from './components/HypergraphVisualizer';
import { KnowledgeRegister } from './components/KnowledgeRegister';
import { MetadataRegister } from './components/MetadataRegister';
import { TrinoNotebook } from './components/TrinoNotebook';
import { SettingsButton } from './components/SettingsButton';
import { Login } from './components/Login';
import { useStore } from './store/useStore';

function App() {
  const isAuthenticated = useIsAuthenticated();
  const { instance } = useMsal();
  const { sidebarOpen, sidebarWidth, viewMode } = useStore();

  // Handle redirect response after login
  useEffect(() => {
    instance.handleRedirectPromise().then((response) => {
      if (response) {
        console.log('Logged in user:', response.account);
        instance.setActiveAccount(response.account);
      }
    }).catch((error) => {
      console.error('Error handling redirect:', error);
    });
  }, [instance]);

  // Show login screen if not authenticated
  if (!isAuthenticated) {
    return <Login />;
  }

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
      case 'knowledge-register':
        return <KnowledgeRegister />;
      case 'metadata-register':
        return <MetadataRegister />;
      case 'notebook':
        return <TrinoNotebook />;
      default:
        return <TrinoNotebook />;
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden' }}>
        <Box sx={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
          {sidebarOpen && <Sidebar width={sidebarWidth} />}
          <Box
            sx={{
              flex: 1,
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden',
              backgroundColor: '#0F1117',
              p: 3,
              gap: 2,
              position: 'relative',
            }}
          >
            {renderContent()}
            <SettingsButton />
          </Box>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

export default App;
