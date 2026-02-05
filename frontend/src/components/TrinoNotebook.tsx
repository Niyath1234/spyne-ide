import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Button,
  CircularProgress,
  Alert,
  Select,
  FormControl,
  MenuItem,
} from '@mui/material';
import {
  Add,
  InsertDriveFileOutlined,
} from '@mui/icons-material';
import { notebookAPI } from '../api/client';
import type { Notebook, NotebookCell } from '../api/client';
import { NotebookCell as NotebookCellComponent } from './NotebookCell';
import { AIChatPanel } from './AIChatPanel';

const SUPPORTED_ENGINES = [
  { value: 'trino', label: 'Trino (Presto)' },
  { value: 'presto', label: 'Presto' },
  { value: 'hive', label: 'Hive' },
  { value: 'druid', label: 'Druid' },
  { value: 'snowflake', label: 'Snowflake' },
  { value: 'bigquery', label: 'BigQuery' },
  { value: 'mysql', label: 'MySQL' },
  { value: 'sqlite', label: 'SQLite' },
  { value: 'postgresql', label: 'PostgreSQL' },
  { value: 'mssql', label: 'SQL Server' },
  { value: 'oracle', label: 'Oracle' },
];

export const TrinoNotebook: React.FC = () => {
  const [notebook, setNotebook] = useState<Notebook | null>(null);
  const [cells, setCells] = useState<NotebookCell[]>([]);
  const [isExecuting, setIsExecuting] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [aiOpenCellId, setAiOpenCellId] = useState<string | null>(null);
  const [engine, setEngine] = useState<string>('trino');

  // Initialize notebook
  useEffect(() => {
    const initNotebook = async () => {
      setIsLoading(true);
      setError(null);
      try {
        // Try to load existing notebook first
        try {
          const loadResponse = await notebookAPI.get('default');
          if (loadResponse.success && loadResponse.notebook) {
          const notebookData = loadResponse.notebook;
          const typedCells: NotebookCell[] = (notebookData.cells || []).map(cell => ({
            ...cell,
            status: (cell.status as NotebookCell['status']) || 'idle',
          }));
          setNotebook({ ...notebookData, cells: typedCells });
          setCells(typedCells.length > 0 ? typedCells : [{ id: `cell${Date.now()}`, sql: '', status: 'idle' }]);
          // Set engine from notebook if available
          if (notebookData.engine) {
            setEngine(notebookData.engine);
          }
          setIsLoading(false);
          return;
          }
        } catch (loadErr: any) {
          // Notebook doesn't exist, create new one
          console.log('Notebook not found, creating new one:', loadErr.message);
        }

        // Create new notebook with one empty cell
        const emptyCell: NotebookCell = {
          id: `cell${Date.now()}`,
          sql: '',
          status: 'idle',
        };

        const response = await notebookAPI.create({
          id: 'default',
          engine: engine,
          cells: [emptyCell],
        });

        if (response.success) {
          const notebookData = response.notebook;
          const typedCells: NotebookCell[] = (notebookData.cells || []).map(cell => ({
            ...cell,
            status: (cell.status as NotebookCell['status']) || 'idle',
          }));
          setNotebook({ ...notebookData, cells: typedCells });
          setCells(typedCells.length > 0 ? typedCells : [emptyCell]);
        } else {
          setError(response.error || 'Failed to create notebook');
        }
      } catch (err: any) {
        console.error('Notebook initialization error:', err);
        
        const errorMessage = err.message || '';
        const isCorsError = errorMessage.includes('CORS') || errorMessage.includes('Access-Control');
        const isNetworkError = err.code === 'ECONNREFUSED' || 
                              err.code === 'ERR_NETWORK' || 
                              err.code === 'ERR_FAILED' ||
                              errorMessage.includes('Network Error') || 
                              errorMessage.includes('fetch') ||
                              errorMessage.includes('Failed to fetch');
        
        // Check if it's a CORS or network error
        if (isCorsError || isNetworkError) {
          const corsMessage = isCorsError 
            ? 'CORS error: Backend is not allowing requests from this origin. ' +
              'Make sure RCA_CORS_ORIGINS includes http://localhost:5173 or set it to "*" for development. '
            : '';
          
          setError(
            corsMessage +
            'Cannot connect to backend server. ' +
            'Make sure the backend is running on http://localhost:8080. ' +
            'Starting with local-only mode (notebooks will not be saved).'
          );
          
          // Create local notebook as fallback with one empty cell
          const emptyCell: NotebookCell = {
            id: `cell${Date.now()}`,
            sql: '',
            status: 'idle',
          };
          const localNotebook: Notebook = {
            id: 'local-default',
            engine: 'trino',
            cells: [emptyCell],
          };
          setNotebook(localNotebook);
          setCells([emptyCell]);
        } else {
          // Extract meaningful error message
          const apiError = err.response?.data?.error;
          const errorMsg = err.message;
          
          // Avoid duplication
          let finalError = 'Failed to initialize notebook';
          if (apiError && !apiError.includes('Failed to initialize notebook')) {
            finalError = apiError;
          } else if (errorMsg && !errorMsg.includes('Failed to initialize notebook')) {
            finalError = errorMsg;
          }
          
          setError(finalError);
        }
      } finally {
        setIsLoading(false);
      }
    };

    initNotebook();
  }, []);

  const addCell = () => {
    const newCell: NotebookCell = {
      id: `cell${Date.now()}`,
      sql: '',
      status: 'idle',
    };
    const newCells = [...cells, newCell];
    setCells(newCells);
    updateNotebook({ cells: newCells });
  };

  const updateCell = (cellId: string, updates: Partial<NotebookCell>) => {
    const newCells = cells.map((c) => (c.id === cellId ? { ...c, ...updates } : c));
    setCells(newCells);
    updateNotebook({ cells: newCells });
  };

  const updateNotebook = async (updates: Partial<Notebook>) => {
    if (!notebook) return;

    try {
      const updated = { ...notebook, ...updates, engine: engine, updated_at: new Date().toISOString() };
      await notebookAPI.update(notebook.id, updated);
      setNotebook(updated);
    } catch (err: any) {
      console.error('Failed to update notebook:', err);
    }
  };

  // Update engine when changed
  useEffect(() => {
    if (notebook && notebook.engine !== engine) {
      updateNotebook({ engine });
    }
  }, [engine]);

  const executeCell = async (cellId: string) => {
    if (!notebook) return;

    setIsExecuting(true);
    setError(null);

    updateCell(cellId, { status: 'running', error: undefined });

    try {
      const response = await notebookAPI.execute(notebook.id, { cell_id: cellId });

      if (response.success) {
        updateCell(cellId, {
          status: 'success',
          result: response.result,
          error: undefined,
        });
      } else {
        updateCell(cellId, {
          status: 'error',
          error: response.error,
          result: undefined,
        });
      }
    } catch (err: any) {
      // Extract error message from API response
      const errorMessage = err.error || err.response?.data?.error || err.message || 'Execution failed';
      console.error('Cell execution error:', err);
      updateCell(cellId, {
        status: 'error',
        error: errorMessage,
        result: undefined,
      });
    } finally {
      setIsExecuting(false);
    }
  };

  const runAllCells = async () => {
    if (!notebook || cells.length === 0) return;

    setIsExecuting(true);
    setError(null);

    for (const cell of cells) {
      await executeCell(cell.id);
    }

    setIsExecuting(false);
  };

  const handleGenerateSQL = async (query: string) => {
    if (!aiOpenCellId || !notebook) return;

    try {
      const response = await notebookAPI.generateSQL(notebook.id, aiOpenCellId, {
        query,
      });

      if (response.success && response.sql) {
        updateCell(aiOpenCellId, { sql: response.sql });
        setAiOpenCellId(null);
      } else {
        setError(response.error || 'Failed to generate SQL');
      }
    } catch (err: any) {
      setError(err.message || 'Failed to generate SQL');
    }
  };

  const handleToggleAI = (cellId: string | null) => {
    setAiOpenCellId(cellId);
  };

  if (isLoading && !notebook) {
    return (
      <Box sx={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', height: '100%', bgcolor: '#0F1117', gap: 2 }}>
        <CircularProgress sx={{ color: '#ff5fa8' }} />
        <Typography sx={{ color: '#A7B0C0' }}>Initializing notebook...</Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', bgcolor: '#0F1117', overflow: 'hidden' }}>
      {/* Top Header Bar */}
      <Box
        sx={{
          height: 48,
          bgcolor: '#12161D',
          border: '1px solid #232833',
          borderRadius: '12px',
          boxShadow: '0 8px 24px rgba(0, 0, 0, 0.25)',
          display: 'flex',
          alignItems: 'center',
          px: 2.5,
          gap: 1,
          mb: 2,
        }}
      >
        {/* Left: Commands */}
        <Button
          size="small"
          onClick={addCell}
          sx={{
            color: '#A7B0C0',
            textTransform: 'none',
            fontSize: '0.875rem',
            minWidth: 'auto',
            px: 1.5,
            borderRadius: '10px',
            transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
            '&:hover': {
              backgroundColor: 'rgba(255, 95, 168, 0.12)',
              color: '#ff5fa8',
              boxShadow: `0 0 10px rgba(255, 95, 168, 0.25)`,
              transform: 'translateY(-1px)',
            },
            '&:active': {
              transform: 'scale(0.98)',
            },
          }}
        >
          + Code
        </Button>
        <Button
          size="small"
          onClick={runAllCells}
          disabled={isExecuting || cells.length === 0}
          sx={{
            color: '#A7B0C0',
            textTransform: 'none',
            fontSize: '0.875rem',
            minWidth: 'auto',
            px: 1.5,
            borderRadius: '10px',
            transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
            '&:hover': {
              backgroundColor: 'rgba(255, 95, 168, 0.12)',
              color: '#ff5fa8',
              boxShadow: `0 0 10px rgba(255, 95, 168, 0.25)`,
              transform: 'translateY(-1px)',
            },
            '&:disabled': {
              color: '#4B5262',
            },
            '&:active': {
              transform: 'scale(0.98)',
            },
          }}
        >
          Run all
        </Button>

        <Box sx={{ flex: 1 }} />

        {/* Engine Selector */}
        <FormControl size="small" sx={{ minWidth: 150, mr: 2 }}>
          <Select
            value={engine}
            onChange={(e) => setEngine(e.target.value)}
            sx={{
              color: '#E6EDF3',
              fontSize: '0.875rem',
              height: 32,
              bgcolor: '#12161D',
              '& .MuiOutlinedInput-notchedOutline': {
                borderColor: '#232833',
              },
              '&:hover .MuiOutlinedInput-notchedOutline': {
                borderColor: '#ff5fa8',
              },
              '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
                borderColor: '#ff5fa8',
                boxShadow: `0 0 8px rgba(255, 95, 168, 0.35)`,
              },
            }}
          >
            {SUPPORTED_ENGINES.map((eng) => (
              <MenuItem key={eng.value} value={eng.value} sx={{ bgcolor: '#12161D', color: '#E6EDF3' }}>
                {eng.label}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>

      {/* Main Content Area */}
      <Box sx={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* Notebook Cells Area */}
        <Box
          sx={{
            flex: 1,
            overflowY: 'auto',
            overflowX: 'hidden',
            bgcolor: '#12161D',
            p: 3,
            border: '1px solid #232833',
            borderRadius: '12px',
            boxShadow: '0 8px 24px rgba(0, 0, 0, 0.25)',
          }}
        >
          {/* Error Display */}
          {error && (
            <Alert
              severity="error"
              onClose={() => setError(null)}
              sx={{
                mb: 2,
                bgcolor: '#1A202A',
                color: '#E6EDF3',
                borderLeft: '4px solid #E57373',
                borderRadius: '10px',
                '& .MuiAlert-icon': { color: '#E57373' },
                '& .MuiAlert-message': { color: '#E6EDF3' },
              }}
            >
              {error}
            </Alert>
          )}

          {cells.length === 0 ? (
            <Box
              sx={{
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                height: '100%',
              }}
            >
              <Box
                sx={{
                  backgroundColor: '#161B22',
                  border: '1px solid #232833',
                  borderRadius: '12px',
                  p: 4,
                  textAlign: 'center',
                  maxWidth: 420,
                  boxShadow: '0 8px 24px rgba(0, 0, 0, 0.25)',
                }}
              >
                <InsertDriveFileOutlined sx={{ color: '#ff5fa8', fontSize: 32, mb: 1 }} />
                <Typography sx={{ fontWeight: 600, color: '#E6EDF3', mb: 0.5 }}>No cells yet</Typography>
                <Typography sx={{ color: '#A7B0C0', fontSize: '0.875rem', mb: 2 }}>
                  Create your first SQL cell to start querying.
                </Typography>
                <Button
                  size="small"
                  startIcon={<Add sx={{ fontSize: 16 }} />}
                  onClick={addCell}
                  sx={{
                    color: '#E6EDF3',
                    backgroundColor: 'rgba(255, 95, 168, 0.12)',
                    border: '1px solid rgba(255, 95, 168, 0.35)',
                    borderRadius: '10px',
                    px: 2,
                    '&:hover': {
                      backgroundColor: 'rgba(255, 95, 168, 0.18)',
                      boxShadow: '0 0 10px rgba(255, 95, 168, 0.25)',
                    },
                  }}
                >
                  Add a code cell
                </Button>
              </Box>
            </Box>
          ) : (
            <>
              {cells.map((cell) => (
                <Box
                  key={cell.id}
                  sx={{
                    mb: 2,
                  }}
                >
                  {/* Cell */}
                  <NotebookCellComponent
                    cell={cell}
                    onRun={executeCell}
                    onChange={(cellId, sql) => updateCell(cellId, { sql })}
                    isAIOpen={aiOpenCellId === cell.id}
                    onToggleAI={handleToggleAI}
                  />

                  {/* AI Panel (shown when AI is open for this cell, below the cell) */}
                  {aiOpenCellId === cell.id && (
                    <Box
                      sx={{
                        mt: 1,
                      }}
                    >
                      <AIChatPanel
                        cellId={cell.id}
                        onClose={() => setAiOpenCellId(null)}
                        onGenerateSQL={handleGenerateSQL}
                      />
                    </Box>
                  )}
                </Box>
              ))}

              {/* Empty Cell Placeholder */}
              <Box
                sx={{
                  p: 3,
                  textAlign: 'center',
                  color: '#9AA0A6',
                  cursor: 'pointer',
                  '&:hover': { color: '#ff5fa8' },
                }}
                onClick={addCell}
              >
                <Typography variant="body2">Start coding or generate with AI.</Typography>
              </Box>
            </>
          )}
        </Box>
      </Box>
    </Box>
  );
};
