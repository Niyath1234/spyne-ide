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

// Custom VS Code Dark+ theme for CodeMirror with BRIGHT, HIGH-CONTRAST colors
const vscodeDarkTheme: Extension = EditorView.theme({
  '&': {
    backgroundColor: '#1E1E1E !important',
    color: '#FFFFFF',
    height: '100%',
  },
  '.cm-editor': {
    height: '100%',
    backgroundColor: '#1E1E1E !important',
  },
  '.cm-scroller': {
    fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
    backgroundColor: '#1E1E1E !important',
  },
  '.cm-content': {
    padding: '8px 16px',
    minHeight: '100%',
    fontSize: '0.875rem',
    fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
    color: '#FFFFFF !important',
    backgroundColor: '#1E1E1E !important',
  },
  '.cm-focused': {
    outline: 'none',
  },
  '.cm-gutters': {
    backgroundColor: '#252526 !important',
    border: 'none',
    color: '#B0B0B0 !important',
  },
  '.cm-lineNumbers': {
    minWidth: '40px',
  },
  '.cm-lineNumbers .cm-gutterElement': {
    padding: '0 8px',
    fontSize: '0.8125rem',
    color: '#B0B0B0 !important',
  },
  '.cm-activeLineGutter': {
    backgroundColor: '#2A2D2E !important',
    color: '#FFFFFF !important',
  },
  '.cm-activeLine': {
    backgroundColor: '#2A2D2E !important',
  },
  '.cm-selectionBackground': {
    backgroundColor: '#264F78 !important',
  },
  '.cm-cursor': {
    borderLeftColor: '#FFFFFF !important',
    borderLeftWidth: '2px',
  },
  // SQL syntax highlighting colors - BRIGHT like pgAdmin
  '.cm-keyword': { color: '#FF5C8D !important', fontWeight: 'bold' }, // Bright pink/red for keywords (SELECT, FROM, WHERE, etc.) - like pgAdmin
  '.cm-string': { color: '#FFA057 !important', fontWeight: 'normal' }, // Bright orange for strings - like pgAdmin
  '.cm-number': { color: '#B5CEA8 !important', fontWeight: 'normal' }, // Bright green for numbers
  '.cm-operator': { color: '#FFFFFF !important', fontWeight: 'bold' }, // Bright white for operators (=, *, etc.)
  '.cm-variable': { color: '#5DBBF5 !important', fontWeight: 'normal' }, // Bright cyan/blue for identifiers - like pgAdmin
  '.cm-builtin': { color: '#5DBBF5 !important', fontWeight: 'normal' }, // Bright cyan for built-ins - like pgAdmin
  '.cm-comment': { color: '#6A9955 !important', fontStyle: 'italic' }, // Bright green for comments
  '.cm-meta': { color: '#FF5C8D !important', fontWeight: 'normal' }, // Bright pink for meta
  '.cm-typeName': { color: '#5DBBF5 !important', fontWeight: 'normal' }, // Bright cyan for type names
  '.cm-propertyName': { color: '#5DBBF5 !important', fontWeight: 'normal' }, // Bright cyan for property names
  // Additional SQL-specific highlighting
  '.cm-atom': { color: '#FF5C8D !important' }, // For boolean literals - bright pink
  '.cm-def': { color: '#DCDCAA !important' }, // For definitions - yellow
  '.cm-qualifier': { color: '#5DBBF5 !important' }, // For qualifiers - bright cyan
  '.cm-line': {
    color: '#FFFFFF !important',
  },
  // Default text color - bright white for maximum visibility
  '.cm-text': {
    color: '#FFFFFF !important',
  },
  // Ensure all text is visible
  '.cm-matchingBracket': {
    backgroundColor: '#264F78 !important',
    color: '#FFFFFF !important',
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
    <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, overflow: 'hidden', backgroundColor: '#1E1E1E' }}>
      {/* Toolbar */}
      <Box
        sx={{
          height: 40,
          backgroundColor: '#252526',
          borderBottom: '1px solid #3E3E42',
          display: 'flex',
          alignItems: 'center',
          px: 2,
          gap: 1,
        }}
      >
        <Typography sx={{ color: '#CCCCCC', fontSize: '0.875rem', mr: 2 }}>
          rca_engine/niyathnair@RCA Engine
        </Typography>
        <FormControl size="small" sx={{ minWidth: 140 }}>
          <Select
            value={queryMode}
            onChange={(e) => handleModeChange(e.target.value as QueryMode)}
            sx={{
              color: '#CCCCCC',
              fontSize: '0.875rem',
              height: 28,
              '& .MuiOutlinedInput-notchedOutline': {
                borderColor: '#3E3E42',
              },
              '&:hover .MuiOutlinedInput-notchedOutline': {
                borderColor: '#464647',
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
              color: '#CCCCCC',
              fontSize: '0.875rem',
              height: 28,
              '& .MuiOutlinedInput-notchedOutline': {
                borderColor: '#3E3E42',
              },
              '&:hover .MuiOutlinedInput-notchedOutline': {
                borderColor: '#464647',
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
            color: '#CCCCCC',
            '&:hover': { backgroundColor: '#3E3E42' },
          }}
        >
          <Save fontSize="small" />
        </IconButton>
        <IconButton
          size="small"
          onClick={handleExecute}
          disabled={isExecuting || !query.trim()}
          sx={{
            color: isExecuting ? '#808080' : '#CCCCCC',
            '&:hover': { backgroundColor: '#3E3E42' },
            '&:disabled': { color: '#606060' },
          }}
        >
          <PlayArrow fontSize="small" />
        </IconButton>
      </Box>

      {/* Tabs */}
      <Box sx={{ borderBottom: '1px solid #3E3E42' }}>
        <Tabs
          value={activeTab}
          onChange={(_, value) => setActiveTab(value)}
          sx={{
            minHeight: 36,
            '& .MuiTab-root': {
              minHeight: 36,
              padding: '0 16px',
              textTransform: 'none',
              color: '#CCCCCC',
              fontSize: '0.875rem',
              '&.Mui-selected': {
                color: '#FFFFFF',
              },
            },
            '& .MuiTabs-indicator': {
              backgroundColor: '#007ACC',
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
          backgroundColor: '#1E1E1E',
          '& .cm-editor': {
            backgroundColor: '#1E1E1E !important',
            width: '100%',
            height: '100%',
          },
          '& .cm-scroller': {
            backgroundColor: '#1E1E1E !important',
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

