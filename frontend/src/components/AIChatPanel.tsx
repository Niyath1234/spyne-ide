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
        background: '#202124',
        border: '1px solid #2a2a2a',
        borderRadius: '8px',
        color: '#e8eaed',
        display: 'flex',
        flexDirection: 'column',
        maxHeight: '400px',
        overflow: 'hidden',
      }}
    >
      {/* Header */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '12px 16px',
          borderBottom: '1px solid #2a2a2a',
        }}
      >
        <Typography
          sx={{
            fontSize: '0.875rem',
            color: '#e8eaed',
            fontWeight: 500,
          }}
        >
          AI for Cell {cellId.substring(0, 8)}
        </Typography>
        <IconButton
          size="small"
          onClick={onClose}
          sx={{
            color: '#9aa0a6',
            padding: '4px',
            '&:hover': {
              backgroundColor: 'rgba(154, 160, 166, 0.1)',
              color: '#e8eaed',
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
          borderTop: '1px solid #2a2a2a',
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
              backgroundColor: '#1f1f1f',
              color: '#e8eaed',
              fontSize: '0.875rem',
              '& fieldset': {
                borderColor: '#2a2a2a',
              },
              '&:hover fieldset': {
                borderColor: '#3a3a3a',
              },
              '&.Mui-focused fieldset': {
                borderColor: '#8ab4f8',
              },
            },
            '& .MuiInputBase-input': {
              color: '#e8eaed',
              '&::placeholder': {
                color: '#7a7a7a',
                opacity: 1,
              },
            },
          }}
        />
        <IconButton
          type="submit"
          disabled={!input.trim() || isGenerating}
          sx={{
            color: isGenerating ? '#7a7a7a' : '#8ab4f8',
            padding: '8px',
            '&:hover': {
              backgroundColor: 'rgba(138, 180, 248, 0.1)',
              color: '#8ab4f8',
            },
            '&:disabled': {
              color: '#7a7a7a',
            },
          }}
        >
          {isGenerating ? (
            <CircularProgress size={20} sx={{ color: '#8ab4f8' }} />
          ) : (
            <Send sx={{ fontSize: '20px' }} />
          )}
        </IconButton>
      </Box>
    </Box>
  );
};
