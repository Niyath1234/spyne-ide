import { ThemeProvider, CssBaseline, Box } from '@mui/material';
import { useIsAuthenticated, useMsal } from '@azure/msal-react';
import { useEffect, useState } from 'react';
import { InteractionStatus } from '@azure/msal-browser';
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
  const { instance, inProgress } = useMsal();
  const { sidebarOpen, sidebarWidth, viewMode } = useStore();
  const [isInitialized, setIsInitialized] = useState(false);

  // Initialize MSAL and handle redirect response
  useEffect(() => {
    let isMounted = true;

    const initializeMsal = async () => {
      try {
        // Wait for MSAL to be ready
        await instance.initialize();
        
        if (!isMounted) return;

        // Handle redirect promise (if user is returning from login)
        const response = await instance.handleRedirectPromise();
        
        if (response && response.account) {
          console.log('Logged in user:', response.account);
          instance.setActiveAccount(response.account);
        } else {
          // Check for existing accounts
          const allAccounts = instance.getAllAccounts();
          if (allAccounts.length > 0) {
            instance.setActiveAccount(allAccounts[0]);
          }
        }
        
        setIsInitialized(true);
      } catch (error) {
        console.error('Error initializing MSAL:', error);
        if (isMounted) {
          setIsInitialized(true); // Set to true anyway to render the app
        }
      }
    };

    initializeMsal();

    return () => {
      isMounted = false;
    };
  }, [instance]);

  // Show loading while MSAL initializes
  if (!isInitialized || inProgress !== InteractionStatus.None) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            height: '100vh',
            backgroundColor: '#0F1117',
          }}
        >
          <Box sx={{ textAlign: 'center', color: '#E6EDF3' }}>
            <Box
              sx={{
                width: 50,
                height: 50,
                border: '3px solid #232833',
                borderTop: '3px solid #0078d4',
                borderRadius: '50%',
                animation: 'spin 1s linear infinite',
                margin: '0 auto 16px',
                '@keyframes spin': {
                  '0%': { transform: 'rotate(0deg)' },
                  '100%': { transform: 'rotate(360deg)' },
                },
              }}
            />
            <Box sx={{ color: '#A7B0C0' }}>Initializing...</Box>
          </Box>
        </Box>
      </ThemeProvider>
    );
  }

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
