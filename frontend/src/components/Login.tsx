import { useMsal } from '@azure/msal-react';
import { InteractionStatus } from '@azure/msal-browser';
import { Box, Button, Typography, Container, Paper, CircularProgress, Alert } from '@mui/material';
import { loginRequest } from '../auth/authConfig';
import { useState } from 'react';
import logoImage from '../assets/logo.png';

export const Login: React.FC = () => {
  const { instance, inProgress } = useMsal();
  const [error, setError] = useState<string | null>(null);

  const handleLogin = () => {
    setError(null);
    try {
      // Use loginRedirect instead of loginPopup to avoid nested popup errors
      instance.loginRedirect(loginRequest).catch((err) => {
        console.error('Login redirect failed:', err);
        setError('Failed to start sign in. Please try again.');
      });
    } catch (err: any) {
      console.error('Login failed:', err);
      setError('Failed to start sign in. Please try again.');
    }
  };

  if (inProgress !== InteractionStatus.None) {
    return (
      <Container
        maxWidth={false}
        sx={{
          height: '100vh',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          backgroundColor: '#0F1117',
        }}
      >
        <Paper
          elevation={0}
          sx={{
            p: 6,
            borderRadius: 3,
            backgroundColor: '#161B22',
            border: '1px solid #232833',
            textAlign: 'center',
            minWidth: 400,
          }}
        >
          <CircularProgress sx={{ mb: 3, color: '#A7B0C0' }} />
          <Typography variant="h6" sx={{ color: '#E6EDF3', mb: 1 }}>
            Signing in...
          </Typography>
          <Typography variant="body2" sx={{ color: '#A7B0C0' }}>
            Redirecting to Microsoft sign in...
          </Typography>
        </Paper>
      </Container>
    );
  }

  return (
    <Container
      maxWidth={false}
      sx={{
        height: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        backgroundColor: '#0F1117',
      }}
    >
      <Paper
        elevation={0}
        sx={{
          p: 6,
          borderRadius: 3,
          backgroundColor: '#161B22',
          border: '1px solid #232833',
          textAlign: 'center',
          minWidth: 400,
          boxShadow: '0 8px 24px rgba(0, 0, 0, 0.25)',
        }}
      >
        <Box
          component="img"
          src={logoImage}
          alt="Logo"
          sx={{
            height: 120,
            width: 'auto',
            maxWidth: '100%',
            objectFit: 'contain',
            mb: 4,
          }}
        />
        <Typography variant="h4" sx={{ color: '#E6EDF3', mb: 2, fontWeight: 600 }}>
          Welcome
        </Typography>
        <Typography variant="body1" sx={{ color: '#A7B0C0', mb: 4 }}>
          Please sign in with your Microsoft account to continue
        </Typography>
        {error && (
          <Alert 
            severity="error" 
            sx={{ 
              mb: 3, 
              backgroundColor: '#2d1b1e',
              color: '#f44336',
              border: '1px solid #f44336',
              '& .MuiAlert-icon': {
                color: '#f44336',
              },
            }}
          >
            {error}
          </Alert>
        )}
        <Button
          variant="contained"
          onClick={handleLogin}
          fullWidth
          sx={{
            py: 1.5,
            backgroundColor: '#0078d4',
            color: '#ffffff',
            fontWeight: 600,
            textTransform: 'none',
            fontSize: '1rem',
            '&:hover': {
              backgroundColor: '#106ebe',
            },
          }}
        >
          Sign in with Microsoft
        </Button>
      </Paper>
    </Container>
  );
};
