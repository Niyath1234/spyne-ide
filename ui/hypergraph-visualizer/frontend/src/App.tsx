import React from 'react';
import { Box } from '@mui/material';
import HypergraphVisualizer from './components/HypergraphVisualizer';

function App() {
  return (
    <Box sx={{ height: '100vh', width: '100vw', overflow: 'hidden' }}>
      <HypergraphVisualizer />
    </Box>
  );
}

export default App;

