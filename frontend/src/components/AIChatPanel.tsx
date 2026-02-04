import React, { useState } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Typography,
  CircularProgress,
} from '@mui/material';
import {
  Send,
  Close,
} from '@mui/icons-material';

interface AIChatPanelProps {
  cellId: string;
  onClose: () => void;
  onGenerateSQL: (query: string) => Promise<void>;
}

export const AIChatPanel: React.FC<AIChatPanelProps> = ({
  cellId,
  onClose,
  onGenerateSQL,
}) => {
  const [input, setInput] = useState('');
  const [isGenerating, setIsGenerating] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isGenerating) return;

    setIsGenerating(true);
    try {
      await onGenerateSQL(input.trim());
      setInput('');
    } catch (error) {
      console.error('Failed to generate SQL:', error);
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <Box
      sx={{
        background: '#161B22',
        border: '1px solid #232833',
        borderRadius: '12px',
        color: '#E6EDF3',
        display: 'flex',
        flexDirection: 'column',
        maxHeight: '400px',
        overflow: 'hidden',
        boxShadow: '0 8px 24px rgba(0, 0, 0, 0.2)',
      }}
    >
      {/* Header */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '12px 16px',
          borderBottom: '1px solid #232833',
        }}
      >
        <Typography
          sx={{
            fontSize: '0.875rem',
            color: '#ff5fa8',
            fontWeight: 600,
          }}
        >
          AI for Cell {cellId.substring(0, 8)}
        </Typography>
        <IconButton
          size="small"
          onClick={onClose}
          sx={{
            color: '#A7B0C0',
            padding: '4px',
            borderRadius: '10px',
            transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
            '&:hover': {
              backgroundColor: 'rgba(255, 95, 168, 0.12)',
              color: '#ff5fa8',
              boxShadow: `0 0 10px rgba(255, 95, 168, 0.2)`,
            },
            '&:active': {
              transform: 'scale(0.98)',
            },
          }}
        >
          <Close sx={{ fontSize: '18px' }} />
        </IconButton>
      </Box>

      {/* Input Area */}
      <Box
        component="form"
        onSubmit={handleSubmit}
        sx={{
          padding: '16px',
          borderTop: '1px solid #232833',
          display: 'flex',
          gap: '8px',
          alignItems: 'flex-end',
        }}
      >
        <TextField
          fullWidth
          multiline
          rows={3}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Describe what you want to query. The AI will generate SQL for this cell only."
          disabled={isGenerating}
          sx={{
            '& .MuiOutlinedInput-root': {
              backgroundColor: '#12161D',
              color: '#E6EDF3',
              fontSize: '0.875rem',
              borderRadius: '10px',
              '& fieldset': {
                borderColor: '#232833',
              },
              '&:hover fieldset': {
                borderColor: '#ff5fa8',
              },
              '&.Mui-focused fieldset': {
                borderColor: '#ff5fa8',
                boxShadow: `0 0 8px rgba(255, 95, 168, 0.35)`,
              },
            },
            '& .MuiInputBase-input': {
              color: '#E6EDF3',
              '&::placeholder': {
                color: '#A7B0C0',
                opacity: 1,
              },
            },
          }}
        />
        <IconButton
          type="submit"
          disabled={!input.trim() || isGenerating}
          sx={{
            color: isGenerating ? '#4B5262' : '#ff5fa8',
            padding: '8px',
            borderRadius: '10px',
            transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
            '&:hover': {
              backgroundColor: 'rgba(255, 95, 168, 0.12)',
              color: '#ff5fa8',
              boxShadow: `0 0 10px rgba(255, 95, 168, 0.2)`,
            },
            '&:disabled': {
              color: '#4B5262',
            },
            '&:active': {
              transform: 'scale(0.98)',
            },
          }}
        >
          {isGenerating ? (
            <CircularProgress size={20} sx={{ color: '#ff5fa8' }} />
          ) : (
            <Send sx={{ fontSize: '20px' }} />
          )}
        </IconButton>
      </Box>
    </Box>
  );
};
