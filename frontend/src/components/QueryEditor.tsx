import React, { useState } from 'react';
import { Box, IconButton, Tabs, Tab, Select, MenuItem, FormControl, Typography } from '@mui/material';
import { PlayArrow, Save, History } from '@mui/icons-material';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { EditorView } from '@codemirror/view';
import type { Extension } from '@codemirror/state';

interface QueryEditorProps {
  onExecute: (query: string, mode?: string) => void;
  isExecuting: boolean;
}

// Custom dark theme for CodeMirror with modern, muted accents
const vscodeDarkTheme: Extension = EditorView.theme({
  '&': {
    backgroundColor: '#12161D !important',
    color: '#E6EDF3',
    height: '100%',
  },
  '.cm-editor': {
    height: '100%',
    backgroundColor: '#12161D !important',
  },
  '.cm-scroller': {
    fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, Consolas, 'Courier New', monospace",
    backgroundColor: '#12161D !important',
  },
  '.cm-content': {
    padding: '8px 16px',
    minHeight: '100%',
    fontSize: '0.875rem',
    fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, Consolas, 'Courier New', monospace",
    color: '#E6EDF3 !important',
    backgroundColor: '#12161D !important',
  },
  '.cm-focused': {
    outline: 'none',
  },
  '.cm-gutters': {
    backgroundColor: '#1F242E !important',
    border: 'none',
    color: '#A7B0C0 !important',
  },
  '.cm-lineNumbers': {
    minWidth: '40px',
  },
  '.cm-lineNumbers .cm-gutterElement': {
    padding: '0 8px',
    fontSize: '0.8125rem',
    color: '#A7B0C0 !important',
  },
  '.cm-activeLineGutter': {
    backgroundColor: '#1F242E !important',
    color: '#E6EDF3 !important',
  },
  '.cm-activeLine': {
    backgroundColor: '#1F242E !important',
  },
  '.cm-selectionBackground': {
    backgroundColor: '#2A3342 !important',
  },
  '.cm-cursor': {
    borderLeftColor: '#E6EDF3 !important',
    borderLeftWidth: '2px',
  },
  // SQL syntax highlighting colors - muted modern palette
  '.cm-keyword': { color: '#ff5fa8 !important', fontWeight: 'bold' },
  '.cm-string': { color: '#C4B5FD !important', fontWeight: 'normal' },
  '.cm-number': { color: '#93C5FD !important', fontWeight: 'normal' },
  '.cm-operator': { color: '#E6EDF3 !important', fontWeight: 'bold' },
  '.cm-variable': { color: '#E6EDF3 !important', fontWeight: 'normal' },
  '.cm-builtin': { color: '#8BD5CA !important', fontWeight: 'normal' },
  '.cm-comment': { color: '#A7B0C0 !important', fontStyle: 'italic' },
  '.cm-meta': { color: '#ff5fa8 !important', fontWeight: 'normal' },
  '.cm-typeName': { color: '#A7B0C0 !important', fontWeight: 'normal' },
  '.cm-propertyName': { color: '#A7B0C0 !important', fontWeight: 'normal' },
  // Additional SQL-specific highlighting
  '.cm-atom': { color: '#ff5fa8 !important' },
  '.cm-def': { color: '#F9E2AF !important' },
  '.cm-qualifier': { color: '#8BD5CA !important' },
  '.cm-line': {
    color: '#E6EDF3 !important',
  },
  // Default text color
  '.cm-text': {
    color: '#E6EDF3 !important',
  },
  // Ensure all text is visible
  '.cm-matchingBracket': {
    backgroundColor: '#2A3342 !important',
    color: '#E6EDF3 !important',
    fontWeight: 'bold',
  },
});

type QueryMode = 'sql' | 'knowledge' | 'metadata';

export const QueryEditor: React.FC<QueryEditorProps> = ({ onExecute, isExecuting }) => {
  const [query, setQuery] = useState("SELECT * FROM table_complete_profile WHERE table_name = 'disbursements';");
  const [activeTab, setActiveTab] = useState(0);
  const [queryMode, setQueryMode] = useState<QueryMode>('sql');

  const handleExecute = () => {
    console.log('Play button clicked, query:', query, 'mode:', queryMode);
    if (query.trim()) {
      console.log('Calling onExecute with:', query, queryMode);
      onExecute(query, queryMode);
    } else {
      console.warn('Query is empty, not executing');
    }
  };

  const getDefaultQuery = (mode: QueryMode) => {
    switch (mode) {
      case 'knowledge':
        return "SELECT * FROM knowledge_register WHERE node_type = 'table';";
      case 'metadata':
        return "SELECT * FROM metadata_register WHERE node_type = 'table';";
      default:
        return "SELECT * FROM table_complete_profile WHERE table_name = 'disbursements';";
    }
  };

  const handleModeChange = (mode: QueryMode) => {
    setQueryMode(mode);
    setQuery(getDefaultQuery(mode));
  };

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        flex: 1,
        overflow: 'hidden',
        backgroundColor: '#12161D',
        border: '1px solid #232833',
        borderRadius: '12px',
        boxShadow: '0 8px 24px rgba(0, 0, 0, 0.25)',
      }}
    >
      {/* Toolbar */}
      <Box
        sx={{
          height: 44,
          backgroundColor: '#12161D',
          borderBottom: '1px solid #232833',
          display: 'flex',
          alignItems: 'center',
          px: 2,
          gap: 1,
        }}
      >
        <Typography sx={{ color: '#A7B0C0', fontSize: '0.875rem', mr: 2 }}>
          rca_engine/niyathnair@RCA Engine
        </Typography>
        <FormControl size="small" sx={{ minWidth: 140 }}>
          <Select
            value={queryMode}
            onChange={(e) => handleModeChange(e.target.value as QueryMode)}
            sx={{
              color: '#E6EDF3',
              fontSize: '0.875rem',
              height: 30,
              bgcolor: '#161B22',
              '& .MuiOutlinedInput-notchedOutline': {
                borderColor: '#232833',
              },
              '&:hover .MuiOutlinedInput-notchedOutline': {
                borderColor: '#ff5fa8',
              },
            }}
          >
            <MenuItem value="sql">SQL Tables</MenuItem>
            <MenuItem value="knowledge">Knowledge Register</MenuItem>
            <MenuItem value="metadata">Metadata Register</MenuItem>
          </Select>
        </FormControl>
        <FormControl size="small" sx={{ minWidth: 100, ml: 1 }}>
          <Select
            value="nolimit"
            sx={{
              color: '#E6EDF3',
              fontSize: '0.875rem',
              height: 30,
              bgcolor: '#161B22',
              '& .MuiOutlinedInput-notchedOutline': {
                borderColor: '#232833',
              },
              '&:hover .MuiOutlinedInput-notchedOutline': {
                borderColor: '#ff5fa8',
              },
            }}
          >
            <MenuItem value="nolimit">No limit</MenuItem>
            <MenuItem value="100">100 rows</MenuItem>
            <MenuItem value="1000">1000 rows</MenuItem>
          </Select>
        </FormControl>
        <Box sx={{ flex: 1 }} />
        <IconButton
          size="small"
          sx={{
            color: '#A7B0C0',
            borderRadius: '10px',
            transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
            '&:hover': {
              backgroundColor: 'rgba(255, 95, 168, 0.12)',
              color: '#ff5fa8',
              boxShadow: '0 0 10px rgba(255, 95, 168, 0.2)',
            },
            '&:active': {
              transform: 'scale(0.98)',
            },
          }}
        >
          <Save fontSize="small" />
        </IconButton>
        <IconButton
          size="small"
          onClick={handleExecute}
          disabled={isExecuting || !query.trim()}
          sx={{
            color: isExecuting ? '#4B5262' : '#A7B0C0',
            borderRadius: '10px',
            transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
            '&:hover': {
              backgroundColor: 'rgba(255, 95, 168, 0.12)',
              color: '#ff5fa8',
              boxShadow: '0 0 10px rgba(255, 95, 168, 0.2)',
            },
            '&:disabled': { color: '#4B5262' },
            '&:active': {
              transform: 'scale(0.98)',
            },
          }}
        >
          <PlayArrow fontSize="small" />
        </IconButton>
      </Box>

      {/* Tabs */}
      <Box sx={{ borderBottom: '1px solid #232833' }}>
        <Tabs
          value={activeTab}
          onChange={(_, value) => setActiveTab(value)}
          sx={{
            minHeight: 36,
            '& .MuiTab-root': {
              minHeight: 36,
              padding: '0 16px',
              textTransform: 'none',
              color: '#A7B0C0',
              fontSize: '0.875rem',
              '&.Mui-selected': {
                color: '#E6EDF3',
              },
            },
            '& .MuiTabs-indicator': {
              backgroundColor: '#ff5fa8',
            },
          }}
        >
          <Tab label="Query" />
          <Tab label="Query History" icon={<History fontSize="small" />} iconPosition="end" />
        </Tabs>
      </Box>

      {/* Editor */}
      <Box 
        sx={{ 
          flex: 1, 
          overflow: 'hidden', 
          position: 'relative', 
          display: 'flex',
          backgroundColor: '#12161D',
          '& .cm-editor': {
            backgroundColor: '#12161D !important',
            width: '100%',
            height: '100%',
          },
          '& .cm-scroller': {
            backgroundColor: '#12161D !important',
          },
        }}
      >
        <CodeMirror
          value={query}
          onChange={(value) => setQuery(value)}
          height="100%"
          extensions={[sql(), vscodeDarkTheme]}
          basicSetup={{
            lineNumbers: true,
            highlightActiveLine: true,
            highlightSelectionMatches: true,
            foldGutter: true,
            dropCursor: false,
            allowMultipleSelections: false,
            indentOnInput: true,
            bracketMatching: true,
            closeBrackets: true,
            autocompletion: true,
            highlightActiveLineGutter: true,
          }}
        />
      </Box>
    </Box>
  );
};

