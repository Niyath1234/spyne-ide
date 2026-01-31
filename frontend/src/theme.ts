import { createTheme } from '@mui/material/styles';

// Dark theme matching GitHub's dark mode colors
export const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#1F6FEB',
      light: '#58A6FF',
      dark: '#1F6FEB',
    },
    secondary: {
      main: '#238636',
      light: '#2EA043',
      dark: '#238636',
    },
    background: {
      default: '#0D1117',
      paper: '#161B22',
    },
    text: {
      primary: '#E6EDF3',
      secondary: '#8B949E',
    },
    divider: '#30363D',
    error: {
      main: '#F85149',
    },
    warning: {
      main: '#D29922',
    },
    info: {
      main: '#58A6FF',
    },
    success: {
      main: '#238636',
    },
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          borderRadius: 6,
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundColor: '#161B22',
          border: '1px solid #30363D',
        },
      },
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          '& .MuiOutlinedInput-root': {
            backgroundColor: '#0D1117',
            color: '#E6EDF3',
            '& fieldset': {
              borderColor: '#30363D',
            },
            '&:hover fieldset': {
              borderColor: '#484F58',
            },
            '&.Mui-focused fieldset': {
              borderColor: '#1F6FEB',
            },
          },
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          backgroundColor: '#21262D',
          color: '#E6EDF3',
          border: '1px solid #30363D',
        },
      },
    },
    MuiAccordion: {
      styleOverrides: {
        root: {
          backgroundColor: '#0D1117',
          '&:before': {
            display: 'none',
          },
        },
      },
    },
  },
});
