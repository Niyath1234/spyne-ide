import React from 'react';
import { Box, Typography } from '@mui/material';

export const Monitoring: React.FC = () => {
  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" sx={{ color: '#E6EDF3' }}>
        Monitoring
      </Typography>
      <Typography variant="body1" sx={{ color: '#8B949E', mt: 2 }}>
        Monitoring dashboard coming soon...
      </Typography>
    </Box>
  );
};
