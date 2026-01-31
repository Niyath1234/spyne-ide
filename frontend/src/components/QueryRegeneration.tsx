import React, { useState, useEffect } from 'react';
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
} from '@mui/icons-material';
import { queryAPI, type QueryGenerationResult, type PrerequisitesResult } from '../api/client';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';

export const QueryRegeneration: React.FC = () => {
  const [query, setQuery] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingPrerequisites, setIsLoadingPrerequisites] = useState(true);
  const [prerequisites, setPrerequisites] = useState<PrerequisitesResult | null>(null);
  const [result, setResult] = useState<QueryGenerationResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Load prerequisites on mount
  useEffect(() => {
    loadPrerequisites();
  }, []);

  const loadPrerequisites = async () => {
    setIsLoadingPrerequisites(true);
    setError(null);
    try {
      const data = await queryAPI.loadPrerequisites();
      setPrerequisites(data);
      if (!data.success) {
        setError(data.error || 'Failed to load prerequisites');
      }
    } catch (err: any) {
      setError(err.message || 'Failed to load prerequisites');
    } finally {
      setIsLoadingPrerequisites(false);
    }
  };

  const handleGenerate = async () => {
    if (!query.trim()) {
      setError('Please enter a query');
      return;
    }

    setIsLoading(true);
    setError(null);
    setResult(null);

    try {
      const data = await queryAPI.generateSQL(query);
      setResult(data);
      if (!data.success) {
        setError(data.error || 'Failed to generate SQL');
      }
    } catch (err: any) {
      setError(err.message || 'Failed to generate SQL');
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      handleGenerate();
    }
  };

  return (
    <Box sx={{ p: 3, height: '100%', overflow: 'auto' }}>
      <Typography variant="h4" gutterBottom sx={{ color: '#E6EDF3', mb: 3 }}>
        Query Regeneration from Natural Language
      </Typography>

      {/* Prerequisites Status */}
      <Card sx={{ mb: 3, bgcolor: '#161B22', border: '1px solid #30363D' }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
            <Typography variant="h6" sx={{ color: '#E6EDF3' }}>
              Prerequisites Status
            </Typography>
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
                Loading metadata...
              </Typography>
            </Box>
          ) : prerequisites?.success ? (
            <Stack direction="row" spacing={2}>
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
            </Stack>
          ) : (
            <Alert severity="error" sx={{ bgcolor: '#1C2128' }}>
              {prerequisites?.error || 'Failed to load prerequisites'}
            </Alert>
          )}
        </CardContent>
      </Card>

      {/* Query Input */}
      <Card sx={{ mb: 3, bgcolor: '#161B22', border: '1px solid #30363D' }}>
        <CardContent>
          <Typography variant="h6" gutterBottom sx={{ color: '#E6EDF3', mb: 2 }}>
            Natural Language Query
          </Typography>
          <TextField
            fullWidth
            multiline
            rows={3}
            placeholder="Example: Show me the current principal outstanding by order type, region, and product group"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyPress={handleKeyPress}
            disabled={isLoading || !prerequisites?.success}
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
              startIcon={isLoading ? <CircularProgress size={16} color="inherit" /> : <SendIcon />}
              onClick={handleGenerate}
              disabled={isLoading || !prerequisites?.success || !query.trim()}
              sx={{
                bgcolor: '#238636',
                '&:hover': { bgcolor: '#2EA043' },
              }}
            >
              Generate SQL
            </Button>
          </Box>
          <Typography variant="caption" sx={{ color: '#8B949E', mt: 1, display: 'block' }}>
            Press Ctrl+Enter (Cmd+Enter on Mac) to generate
          </Typography>
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <Alert severity="error" sx={{ mb: 3, bgcolor: '#1C2128' }}>
          {error}
        </Alert>
      )}

      {/* Results */}
      {result && (
        <Card sx={{ bgcolor: '#161B22', border: '1px solid #30363D' }}>
          <CardContent>
            {result.success ? (
              <>
                <Typography variant="h6" gutterBottom sx={{ color: '#E6EDF3', mb: 2 }}>
                  Generated SQL
                </Typography>

                {/* SQL Display */}
                <Box sx={{ mb: 3 }}>
                  <CodeMirror
                    value={result.sql || ''}
                    height="300px"
                    extensions={[sql()]}
                    theme={oneDark}
                    editable={false}
                  />
                </Box>

                <Divider sx={{ my: 3, borderColor: '#30363D' }} />

                {/* Metadata Display */}
                <Accordion defaultExpanded sx={{ bgcolor: '#0D1117', mb: 2 }}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#E6EDF3' }} />}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <TableIcon sx={{ color: '#58A6FF' }} />
                      <Typography sx={{ color: '#E6EDF3' }}>Metric & Dimensions</Typography>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails>
                    {result.metric && (
                      <Box sx={{ mb: 2 }}>
                        <Typography variant="subtitle2" sx={{ color: '#58A6FF', mb: 1 }}>
                          Metric: {result.metric.name}
                        </Typography>
                        <Typography variant="body2" sx={{ color: '#8B949E' }}>
                          {result.metric.description}
                        </Typography>
                      </Box>
                    )}
                    {result.dimensions && result.dimensions.length > 0 && (
                      <Box>
                        <Typography variant="subtitle2" sx={{ color: '#58A6FF', mb: 1 }}>
                          Dimensions:
                        </Typography>
                        <Stack spacing={1}>
                          {result.dimensions.map((dim, idx) => (
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

                <Accordion sx={{ bgcolor: '#0D1117', mb: 2 }}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#E6EDF3' }} />}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <LinkIcon sx={{ color: '#58A6FF' }} />
                      <Typography sx={{ color: '#E6EDF3' }}>Joins</Typography>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails>
                    {result.joins && result.joins.length > 0 ? (
                      <Stack spacing={1}>
                        {result.joins.map((join, idx) => (
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

                <Accordion sx={{ bgcolor: '#0D1117' }}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#E6EDF3' }} />}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <FilterIcon sx={{ color: '#58A6FF' }} />
                      <Typography sx={{ color: '#E6EDF3' }}>Filters</Typography>
                    </Box>
                  </AccordionSummary>
                  <AccordionDetails>
                    {result.filters && result.filters.length > 0 ? (
                      <Stack spacing={1}>
                        {result.filters.map((filter, idx) => (
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
              </>
            ) : (
              <Alert severity="error" sx={{ bgcolor: '#1C2128' }}>
                {result.error}
                {result.suggestion && (
                  <Typography variant="body2" sx={{ mt: 1 }}>
                    Suggestion: {result.suggestion}
                  </Typography>
                )}
              </Alert>
            )}
          </CardContent>
        </Card>
      )}
    </Box>
  );
};





