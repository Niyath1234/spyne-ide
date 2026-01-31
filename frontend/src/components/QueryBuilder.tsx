import React, { useState, useEffect, useRef } from 'react';
import {
  Box,
  TextField,
  Button,
  Typography,
  Card,
  CardContent,
  Alert,
  CircularProgress,
  Chip,
  Stack,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Divider,
  Grid,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  IconButton,
  Tooltip,
} from '@mui/material';
import {
  Send as SendIcon,
  ExpandMore as ExpandMoreIcon,
  Code as CodeIcon,
  TableChart as TableIcon,
  Link as LinkIcon,
  FilterList as FilterIcon,
  CheckCircle as CheckIcon,
  Error as ErrorIcon,
  AutoAwesome as BuildIcon,
  Add as AddIcon,
  Delete as DeleteIcon,
  ContentCopy as CopyIcon,
} from '@mui/icons-material';
import { queryAPI, type QueryGenerationResult, type PrerequisitesResult } from '../api/client';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';


interface QuerySession {
  id: string;
  name: string;
  query: string;
  result: QueryGenerationResult | null;
  error: string | null;
  isLoading: boolean;
  viewType: 'sql' | 'breakdown' | null; // null means hidden
}

export const QueryBuilder: React.FC = () => {
  const [sessions, setSessions] = useState<QuerySession[]>(() => {
    // Initialize with one default session
    return [{
      id: '1',
      name: 'Query #1',
      query: '',
      result: null,
      error: null,
      isLoading: false,
      viewType: null,
    }];
  });
  const [activeSessionId, setActiveSessionId] = useState<string>('1');
  const [isLoadingPrerequisites, setIsLoadingPrerequisites] = useState(true);
  const [prerequisites, setPrerequisites] = useState<PrerequisitesResult | null>(null);
  

  // Load prerequisites on mount
  useEffect(() => {
    loadPrerequisites();
  }, []);

  // Get active session
  const activeSession = sessions.find(s => s.id === activeSessionId) || sessions[0];

  // Removed graph initialization - no longer needed

  // Add new query session
  const handleAddQuery = () => {
    const newId = String(sessions.length + 1);
    const newSession: QuerySession = {
      id: newId,
      name: `Query #${newId}`,
      query: '',
      result: null,
      error: null,
      isLoading: false,
      viewType: null,
    };
    setSessions([...sessions, newSession]);
    setActiveSessionId(newId);
  };

  // Delete query session
  const handleDeleteQuery = (sessionId: string) => {
    if (sessions.length === 1) {
      // Don't allow deleting the last session
      return;
    }
    const newSessions = sessions.filter(s => s.id !== sessionId);
    setSessions(newSessions);
    // Switch to first session if deleted session was active
    if (activeSessionId === sessionId) {
      setActiveSessionId(newSessions[0].id);
    }
  };

  // Duplicate query session
  const handleDuplicateQuery = (sessionId: string) => {
    const sessionToDuplicate = sessions.find(s => s.id === sessionId);
    if (!sessionToDuplicate) return;
    
    const newId = String(sessions.length + 1);
    const newSession: QuerySession = {
      ...sessionToDuplicate,
      id: newId,
      name: `Query #${newId}`,
      result: null,
      error: null,
      isLoading: false,
      viewType: null,
    };
    setSessions([...sessions, newSession]);
    setActiveSessionId(newId);
  };

  // Update session
  const updateSession = (sessionId: string, updates: Partial<QuerySession>) => {
    setSessions(sessions.map(s => s.id === sessionId ? { ...s, ...updates } : s));
  };

  const loadPrerequisites = async () => {
    setIsLoadingPrerequisites(true);
    try {
      const data = await queryAPI.loadPrerequisites();
      setPrerequisites(data);
    } catch (err: any) {
      console.error('Failed to load prerequisites:', err);
      setPrerequisites({
        success: false,
        error: err.message || 'Failed to load metadata and business rules',
      });
    } finally {
      setIsLoadingPrerequisites(false);
    }
  };


  const handleGenerate = async () => {
    if (!activeSession.query.trim()) {
      updateSession(activeSessionId, { error: 'Please enter a query' });
      return;
    }

    updateSession(activeSessionId, { isLoading: true, error: null, result: null });

    try {
      const data = await queryAPI.generateSQL(activeSession.query);
      updateSession(activeSessionId, { 
        result: data,
        isLoading: false,
        error: data.success ? null : (data.error || 'Failed to build query'),
        viewType: data.success ? 'sql' : null,
      });
    } catch (err: any) {
      updateSession(activeSessionId, { 
        isLoading: false,
        error: err.message || 'Failed to build query',
      });
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      handleGenerate();
    }
  };

  return (
    <Box sx={{ p: 3, height: '100%', overflow: 'auto' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <BuildIcon sx={{ color: '#1F6FEB', fontSize: 32 }} />
          <Box>
            <Typography variant="h4" sx={{ color: '#E6EDF3' }}>
              Query Builder
            </Typography>
            <Typography variant="body2" sx={{ color: '#8B949E' }}>
              Build perfect SQL queries from metadata and business rules
            </Typography>
          </Box>
        </Box>
        
        {/* Query Selector Dropdown */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <FormControl size="small" sx={{ minWidth: 150 }}>
            <InputLabel sx={{ color: '#8B949E' }}>Select Query</InputLabel>
            <Select
              value={activeSessionId}
              onChange={(e) => setActiveSessionId(e.target.value)}
              label="Select Query"
              sx={{
                color: '#E6EDF3',
                '& .MuiOutlinedInput-notchedOutline': {
                  borderColor: '#30363D',
                },
                '&:hover .MuiOutlinedInput-notchedOutline': {
                  borderColor: '#484F58',
                },
                '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
                  borderColor: '#1F6FEB',
                },
                '& .MuiSvgIcon-root': {
                  color: '#8B949E',
                },
              }}
            >
              {sessions.map((session) => (
                <MenuItem key={session.id} value={session.id}>
                  {session.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          
          <Tooltip title="Add New Query">
            <IconButton
              onClick={handleAddQuery}
              sx={{
                color: '#8B949E',
                '&:hover': { color: '#1F6FEB', bgcolor: '#21262D' },
              }}
            >
              <AddIcon />
            </IconButton>
          </Tooltip>
          
          {sessions.length > 1 && (
            <Tooltip title="Delete Query">
              <IconButton
                onClick={() => handleDeleteQuery(activeSessionId)}
                sx={{
                  color: '#8B949E',
                  '&:hover': { color: '#F85149', bgcolor: '#21262D' },
                }}
              >
                <DeleteIcon />
              </IconButton>
            </Tooltip>
          )}
          
          <Tooltip title="Duplicate Query">
            <IconButton
              onClick={() => handleDuplicateQuery(activeSessionId)}
              sx={{
                color: '#8B949E',
                '&:hover': { color: '#1F6FEB', bgcolor: '#21262D' },
              }}
            >
              <CopyIcon />
            </IconButton>
          </Tooltip>
        </Box>
      </Box>

      {/* Metadata & Business Rules Status */}
      <Card sx={{ mb: 3, bgcolor: '#161B22', border: '1px solid #30363D' }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
            <Box>
              <Typography variant="h6" sx={{ color: '#E6EDF3' }}>
                Metadata & Business Rules Registry
              </Typography>
              <Typography variant="caption" sx={{ color: '#8B949E' }}>
                All metadata, business rules, and relationships loaded from semantic registry
              </Typography>
            </Box>
            <Button
              size="small"
              variant="outlined"
              onClick={loadPrerequisites}
              disabled={isLoadingPrerequisites}
            >
              {isLoadingPrerequisites ? <CircularProgress size={16} /> : 'Reload'}
            </Button>
          </Box>

          {isLoadingPrerequisites ? (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <CircularProgress size={20} />
              <Typography variant="body2" sx={{ color: '#8B949E' }}>
                Loading metadata and business rules...
              </Typography>
            </Box>
          ) : prerequisites?.success ? (
            <Stack direction="row" spacing={2} flexWrap="wrap">
              <Chip
                icon={<CheckIcon />}
                label={`${prerequisites.loaded?.metrics || 0} Metrics`}
                color="success"
                size="small"
              />
              <Chip
                icon={<CheckIcon />}
                label={`${prerequisites.loaded?.dimensions || 0} Dimensions`}
                color="success"
                size="small"
              />
              <Chip
                icon={<CheckIcon />}
                label={`${prerequisites.loaded?.tables || 0} Tables`}
                color="success"
                size="small"
              />
              <Chip
                icon={<CheckIcon />}
                label="Business Rules Loaded"
                color="success"
                size="small"
              />
            </Stack>
          ) : (
            <Alert severity="error" sx={{ bgcolor: '#1C2128' }}>
              {prerequisites?.error || 'Failed to load metadata and business rules'}
            </Alert>
          )}
        </CardContent>
      </Card>

      {/* Query Input - Show when no results or results are hidden */}
      {(!activeSession.result || !activeSession.result.success || activeSession.viewType === null) && (
        <Card sx={{ mb: 3, bgcolor: '#161B22', border: '1px solid #30363D' }}>
          <CardContent>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="h6" sx={{ color: '#E6EDF3' }}>
                {activeSession.name}
              </Typography>
              <Typography variant="caption" sx={{ color: '#8B949E' }}>
                Ask your question in plain English. The pipeline will use metadata and business rules to build the perfect query.
              </Typography>
            </Box>
            <TextField
              fullWidth
              multiline
              rows={4}
              placeholder="Example: Show me the current principal outstanding by order type, region, and product group"
              value={activeSession.query}
              onChange={(e) => updateSession(activeSessionId, { query: e.target.value })}
              onKeyPress={handleKeyPress}
              disabled={activeSession.isLoading || !prerequisites?.success}
              sx={{
                '& .MuiOutlinedInput-root': {
                  bgcolor: '#0D1117',
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
              }}
            />
            <Box sx={{ mt: 2, display: 'flex', justifyContent: 'flex-end' }}>
              <Button
                variant="contained"
                startIcon={activeSession.isLoading ? <CircularProgress size={16} color="inherit" /> : <SendIcon />}
                onClick={handleGenerate}
                disabled={activeSession.isLoading || !prerequisites?.success || !activeSession.query.trim()}
                sx={{
                  bgcolor: '#238636',
                  '&:hover': { bgcolor: '#2EA043' },
                }}
              >
                Build Query
              </Button>
            </Box>
            <Typography variant="caption" sx={{ color: '#8B949E', mt: 1, display: 'block' }}>
              Press Ctrl+Enter (Cmd+Enter on Mac) to build query
            </Typography>
          </CardContent>
        </Card>
      )}

      {/* Error Display */}
      {activeSession.error && (
        <Alert severity="error" sx={{ mb: 3, bgcolor: '#1C2128' }}>
          {activeSession.error}
        </Alert>
      )}

      {/* Results Section with Dropdown */}
      {activeSession.result && activeSession.result.success && (
        <Card sx={{ bgcolor: '#161B22', border: '1px solid #30363D', mb: 3 }}>
          <CardContent>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="h6" sx={{ color: '#E6EDF3' }}>
                {activeSession.name} - Results
              </Typography>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <InputLabel sx={{ color: '#8B949E' }}>View</InputLabel>
                  <Select
                    value={activeSession.viewType || ''}
                    onChange={(e) => updateSession(activeSessionId, { viewType: e.target.value as 'sql' | 'breakdown' })}
                    label="View"
                    sx={{
                      color: '#E6EDF3',
                      '& .MuiOutlinedInput-notchedOutline': {
                        borderColor: '#30363D',
                      },
                      '&:hover .MuiOutlinedInput-notchedOutline': {
                        borderColor: '#484F58',
                      },
                      '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
                        borderColor: '#1F6FEB',
                      },
                      '& .MuiSvgIcon-root': {
                        color: '#8B949E',
                      },
                    }}
                  >
                    <MenuItem value="sql">
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <CodeIcon fontSize="small" />
                        <span>Generated SQL</span>
                      </Box>
                    </MenuItem>
                    <MenuItem value="breakdown">
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <TableIcon fontSize="small" />
                        <span>Query Breakdown</span>
                      </Box>
                    </MenuItem>
                  </Select>
                </FormControl>
                <Tooltip title="Hide Results">
                  <IconButton
                    onClick={() => updateSession(activeSessionId, { viewType: null })}
                    sx={{
                      color: '#8B949E',
                      '&:hover': { color: '#F85149', bgcolor: '#21262D' },
                    }}
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Box>
            </Box>

            {/* SQL View */}
            {activeSession.viewType === 'sql' && (
              <Box>
                <Typography variant="subtitle2" gutterBottom sx={{ color: '#8B949E', mb: 1 }}>
                  Built SQL Query
                </Typography>
                <CodeMirror
                  value={activeSession.result.sql || ''}
                  height="400px"
                  extensions={[sql()]}
                  theme={oneDark}
                  editable={false}
                />
              </Box>
            )}

            {/* Query Breakdown View */}
            {activeSession.viewType === 'breakdown' && (
              <Box>
                <Typography variant="subtitle2" gutterBottom sx={{ color: '#8B949E', mb: 2 }}>
                  Query Breakdown
                </Typography>

                {/* Metric & Dimensions */}
                <Accordion defaultExpanded sx={{ bgcolor: '#0D1117', mb: 2 }}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#E6EDF3' }} />}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <TableIcon sx={{ color: '#58A6FF' }} />
                      <Typography sx={{ color: '#E6EDF3' }}>Metric & Dimensions</Typography>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails>
                    {activeSession.result.metric && (
                      <Box sx={{ mb: 2 }}>
                        <Typography variant="subtitle2" sx={{ color: '#58A6FF', mb: 1 }}>
                          Metric: {activeSession.result.metric.name}
                        </Typography>
                        <Typography variant="body2" sx={{ color: '#8B949E' }}>
                          {activeSession.result.metric.description}
                        </Typography>
                      </Box>
                    )}
                    {activeSession.result.dimensions && activeSession.result.dimensions.length > 0 && (
                      <Box>
                        <Typography variant="subtitle2" sx={{ color: '#58A6FF', mb: 1 }}>
                          Dimensions:
                        </Typography>
                        <Stack spacing={1}>
                          {activeSession.result.dimensions.map((dim, idx) => (
                            <Box key={idx} sx={{ pl: 2 }}>
                              <Typography variant="body2" sx={{ color: '#E6EDF3' }}>
                                • {dim.name}
                              </Typography>
                              <Typography variant="caption" sx={{ color: '#8B949E', pl: 2 }}>
                                {dim.description}
                              </Typography>
                            </Box>
                          ))}
                        </Stack>
                      </Box>
                    )}
                  </AccordionDetails>
                </Accordion>

                {/* Joins */}
                <Accordion sx={{ bgcolor: '#0D1117', mb: 2 }}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#E6EDF3' }} />}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <LinkIcon sx={{ color: '#58A6FF' }} />
                      <Typography sx={{ color: '#E6EDF3' }}>Joins (from Metadata)</Typography>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails>
                    {activeSession.result.joins && activeSession.result.joins.length > 0 ? (
                      <Stack spacing={1}>
                        {activeSession.result.joins.map((join, idx) => (
                          <Box key={idx} sx={{ pl: 2 }}>
                            <Typography variant="body2" sx={{ color: '#E6EDF3' }}>
                              {join.from_table} → {join.to_table}
                            </Typography>
                            <Typography variant="caption" sx={{ color: '#8B949E', pl: 2 }}>
                              ON {join.on}
                            </Typography>
                          </Box>
                        ))}
                      </Stack>
                    ) : (
                      <Typography variant="body2" sx={{ color: '#8B949E' }}>
                        No joins required
                      </Typography>
                    )}
                  </AccordionDetails>
                </Accordion>

                {/* Filters */}
                <Accordion sx={{ bgcolor: '#0D1117' }}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#E6EDF3' }} />}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <FilterIcon sx={{ color: '#58A6FF' }} />
                      <Typography sx={{ color: '#E6EDF3' }}>Filters (from Business Rules)</Typography>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails>
                    {activeSession.result.filters && activeSession.result.filters.length > 0 ? (
                      <Stack spacing={1}>
                        {activeSession.result.filters.map((filter, idx) => (
                          <Chip
                            key={idx}
                            label={filter}
                            size="small"
                            sx={{
                              bgcolor: '#21262D',
                              color: '#E6EDF3',
                              border: '1px solid #30363D',
                            }}
                          />
                        ))}
                      </Stack>
                    ) : (
                      <Typography variant="body2" sx={{ color: '#8B949E' }}>
                        No filters applied
                      </Typography>
                    )}
                  </AccordionDetails>
                </Accordion>
              </Box>
            )}
          </CardContent>
        </Card>
      )}

    </Box>
  );
};

