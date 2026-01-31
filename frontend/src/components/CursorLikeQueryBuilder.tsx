import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Typography,
  Paper,
  Chip,
  CircularProgress,
  Alert,
  Collapse,
} from '@mui/material';
import {
  Send as SendIcon,
  Code as CodeIcon,
  CheckCircle as CheckIcon,
  Error as ErrorIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
} from '@mui/icons-material';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { EditorView } from '@codemirror/view';
import { queryAPI } from '../api/client';

interface ReasoningStep {
  id: string;
  type: 'thought' | 'action' | 'result' | 'error' | 'info';
  content: string;
  timestamp: string;
}

export const CursorLikeQueryBuilder: React.FC = () => {
  const [sqlQuery, setSqlQuery] = useState('');
  const [chatInput, setChatInput] = useState('');
  const [reasoningSteps, setReasoningSteps] = useState<ReasoningStep[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const [expandedSteps, setExpandedSteps] = useState<Set<string>>(new Set());

  // Auto-scroll chat to bottom
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [reasoningSteps]);

  const getStepIcon = (type: ReasoningStep['type']) => {
    switch (type) {
      case 'result':
        return <CheckIcon sx={{ color: '#3FB950', fontSize: 16 }} />;
      case 'error':
        return <ErrorIcon sx={{ color: '#F85149', fontSize: 16 }} />;
      case 'action':
        return <CircularProgress size={16} sx={{ color: '#58A6FF' }} />;
      default:
        return <Typography sx={{ color: '#8B949E', fontSize: 12 }}>•</Typography>;
    }
  };

  const getStepColor = (type: ReasoningStep['type']) => {
    switch (type) {
      case 'result':
        return '#3FB950';
      case 'error':
        return '#F85149';
      case 'action':
        return '#58A6FF';
      case 'info':
        return '#8B949E';
      default:
        return '#E6EDF3';
    }
  };

  const handleSendQuery = async () => {
    if (!chatInput.trim() || isLoading) return;

    const userQuery = chatInput.trim();
    setChatInput('');
    setError(null);
    setIsLoading(true);
    
    // Clear previous reasoning
    setReasoningSteps([]);

    // Add user message
    const userStep: ReasoningStep = {
      id: `user-${Date.now()}`,
      type: 'info',
      content: userQuery,
      timestamp: new Date().toISOString(),
    };
    setReasoningSteps([userStep]);

    try {
      // Call query generation API with LLM
      const response = await queryAPI.generateSQL(userQuery, true);
      
      if (response.success) {
        // Add reasoning steps from API - DYNAMICALLY with delays
        const apiSteps = response.reasoning_steps || [];
        const newSteps: ReasoningStep[] = [userStep];
        setReasoningSteps(newSteps);
        
        // Add steps dynamically with delays for visual effect
        for (let i = 0; i < apiSteps.length; i++) {
          const stepContent = apiSteps[i];
          // Determine step type based on content
          const contentLower = stepContent.toLowerCase();
          let stepType: ReasoningStep['type'] = 'thought';
          
          if (contentLower.includes('error') || contentLower.includes('failed') || contentLower.includes('❌')) {
            stepType = 'error';
          } else if (contentLower.includes('✅') || contentLower.includes('success') || contentLower.includes('generated sql')) {
            stepType = 'result';
          } else if (contentLower.includes('calling') || contentLower.includes('analyzing') || contentLower.includes('🤖')) {
            stepType = 'action';
          } else if (contentLower.includes('📊') || contentLower.includes('📝') || contentLower.includes('🔍')) {
            stepType = 'info';
          }
          
          // Add delay between steps (faster for thoughts, slower for results)
          const delay = stepType === 'thought' ? 100 : stepType === 'action' ? 200 : 300;
          
          setTimeout(() => {
            setReasoningSteps(prev => [...prev, {
              id: `step-${Date.now()}-${i}-${Math.random()}`,
              type: stepType,
              content: stepContent,
              timestamp: new Date().toISOString(),
            }]);
            
            // Scroll to bottom after adding step
            setTimeout(() => {
              const chatContainer = document.querySelector('[data-chat-container]');
              if (chatContainer) {
                chatContainer.scrollTop = chatContainer.scrollHeight;
              }
            }, 50);
          }, i * delay);
        }

        // Add final SQL result step (after all reasoning steps)
        if (response.sql) {
          const totalDelay = apiSteps.length * 200;
          setTimeout(() => {
            // Write SQL to center editor
            setSqlQuery(response.sql);
            
            // Add SQL to chat window as part of chain of thought
            setReasoningSteps(prev => [...prev, {
              id: `sql-result-${Date.now()}`,
              type: 'result',
              content: `✅ **SQL Query Generated**\n\nThe following SQL query has been written to the editor:\n\n\`\`\`sql\n${response.sql}\n\`\`\``,
              timestamp: new Date().toISOString(),
            }]);
            
            // Scroll to bottom after adding SQL
            setTimeout(() => {
              const chatContainer = document.querySelector('[data-chat-container]');
              if (chatContainer) {
                chatContainer.scrollTop = chatContainer.scrollHeight;
              }
            }, 50);
          }, totalDelay + 300);
        }
        
        // Expand all steps by default
        const allStepIds = new Set(newSteps.map(s => s.id));
        setExpandedSteps(allStepIds);
      } else {
        setError(response.error || 'Failed to generate SQL');
        setReasoningSteps([
          userStep,
          {
            id: `error-${Date.now()}`,
            type: 'error',
            content: `❌ Error: ${response.error || 'Failed to generate SQL'}`,
            timestamp: new Date().toISOString(),
          },
        ]);
      }
    } catch (err: any) {
      const errorMsg = err.message || 'Failed to generate SQL';
      setError(errorMsg);
      setReasoningSteps([
        userStep,
        {
          id: `error-${Date.now()}`,
          type: 'error',
          content: `❌ Error: ${errorMsg}`,
          timestamp: new Date().toISOString(),
        },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendQuery();
    }
  };

  const toggleStepExpansion = (stepId: string) => {
    const newExpanded = new Set(expandedSteps);
    if (newExpanded.has(stepId)) {
      newExpanded.delete(stepId);
    } else {
      newExpanded.add(stepId);
    }
    setExpandedSteps(newExpanded);
  };

  const formatStepContent = (content: string) => {
    // Format markdown-like content
    const lines = content.split('\n');
    const formatted: React.ReactNode[] = [];
    let inCodeBlock = false;
    let codeLanguage = '';
    let codeLines: string[] = [];
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      
      // Code blocks
      if (line.startsWith('```')) {
        if (inCodeBlock) {
          // End of code block
          formatted.push(
            <Box
              key={`code-${i}`}
              sx={{
                bgcolor: '#0D1117',
                border: '1px solid #30363D',
                borderRadius: 1,
                p: 1.5,
                my: 1,
                fontFamily: 'monospace',
                fontSize: '0.875rem',
                overflowX: 'auto',
                overflowY: 'auto',
                position: 'relative',
                maxWidth: '100%',
                wordBreak: 'break-word',
                overflowWrap: 'break-word',
              }}
            >
              {codeLanguage && (
                <Typography
                  sx={{
                    position: 'absolute',
                    top: 4,
                    right: 8,
                    fontSize: '0.7rem',
                    color: '#8B949E',
                    textTransform: 'uppercase',
                  }}
                >
                  {codeLanguage}
                </Typography>
              )}
              <pre style={{ 
                margin: 0, 
                color: '#E6EDF3', 
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
                overflowWrap: 'break-word',
                maxWidth: '100%',
                overflowX: 'auto'
              }}>
                {codeLines.join('\n')}
              </pre>
            </Box>
          );
          codeLines = [];
          codeLanguage = '';
          inCodeBlock = false;
        } else {
          // Start of code block
          inCodeBlock = true;
          codeLanguage = line.slice(3).trim();
        }
        continue;
      }
      
      if (inCodeBlock) {
        codeLines.push(line);
        continue;
      }
      
      // Bold text (markdown **)
      if (line.includes('**')) {
        const parts = line.split('**');
        formatted.push(
          <Typography key={`line-${i}`} sx={{ color: '#E6EDF3', fontSize: '0.875rem', mb: 0.5, lineHeight: 1.6 }}>
            {parts.map((part, idx) => 
              idx % 2 === 1 ? <strong key={idx} style={{ fontWeight: 600 }}>{part}</strong> : part
            )}
          </Typography>
        );
        continue;
      }
      
      // Regular line
      if (line.trim()) {
        formatted.push(
          <Typography key={`line-${i}`} sx={{ color: '#E6EDF3', fontSize: '0.875rem', mb: 0.5, whiteSpace: 'pre-wrap', lineHeight: 1.6 }}>
            {line}
          </Typography>
        );
      } else {
        // Empty line for spacing
        formatted.push(<Box key={`spacer-${i}`} sx={{ height: '0.5rem' }} />);
      }
    }
    
    // Handle unclosed code block
    if (inCodeBlock && codeLines.length > 0) {
      formatted.push(
        <Box
          key={`code-final`}
          sx={{
            bgcolor: '#0D1117',
            border: '1px solid #30363D',
            borderRadius: 1,
            p: 1.5,
            my: 1,
            fontFamily: 'monospace',
            fontSize: '0.875rem',
            overflowX: 'auto',
            overflowY: 'auto',
            maxWidth: '100%',
            wordBreak: 'break-word',
            overflowWrap: 'break-word',
          }}
        >
          <pre style={{ 
            margin: 0, 
            color: '#E6EDF3', 
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            overflowWrap: 'break-word',
            maxWidth: '100%',
            overflowX: 'auto'
          }}>
            {codeLines.join('\n')}
          </pre>
        </Box>
      );
    }
    
    return formatted.length > 0 ? formatted : [<Typography key="empty" sx={{ color: '#8B949E' }}>{content}</Typography>];
  };

  return (
    <Box sx={{ display: 'flex', height: '100%', overflow: 'hidden', bgcolor: '#0D1117' }}>
      {/* Center: SQL Editor */}
      <Box
        sx={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          borderRight: '1px solid #30363D',
          bgcolor: '#0D1117',
        }}
      >
        {/* SQL Editor Header */}
        <Box
          sx={{
            p: 2,
            borderBottom: '1px solid #30363D',
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            bgcolor: '#161B22',
          }}
        >
          <CodeIcon sx={{ color: '#58A6FF' }} />
          <Typography variant="h6" sx={{ color: '#E6EDF3', flex: 1, fontWeight: 600 }}>
            SQL Editor
          </Typography>
          {sqlQuery && (
            <Chip
              label="SQL Ready"
              size="small"
              icon={<CheckIcon sx={{ fontSize: 16 }} />}
              sx={{ 
                bgcolor: '#238636', 
                color: '#E6EDF3',
                fontWeight: 500,
                '& .MuiChip-icon': {
                  color: '#3FB950',
                },
              }}
            />
          )}
        </Box>

        {/* CodeMirror Editor */}
        <Box sx={{ flex: 1, overflow: 'hidden' }}>
          <CodeMirror
            value={sqlQuery}
            onChange={(value) => setSqlQuery(value)}
            height="100%"
            extensions={[
              sql(),
              EditorView.lineWrapping,
            ]}
            theme={oneDark}
            basicSetup={{
              lineNumbers: true,
              foldGutter: true,
              dropCursor: false,
              allowMultipleSelections: false,
            }}
          />
        </Box>
      </Box>

      {/* Right Side: Chat Panel with Chain of Thought */}
      <Box
        sx={{
          width: '450px',
          display: 'flex',
          flexDirection: 'column',
          bgcolor: '#161B22',
          borderLeft: '1px solid #30363D',
        }}
      >
        {/* Chat Header */}
        <Box
          sx={{
            p: 2,
            borderBottom: '1px solid #30363D',
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            bgcolor: '#0D1117',
          }}
        >
          <Typography variant="h6" sx={{ color: '#E6EDF3', flex: 1, fontWeight: 600 }}>
            Query Builder
          </Typography>
          {isLoading && <CircularProgress size={20} sx={{ color: '#58A6FF' }} />}
        </Box>

        {/* Reasoning Steps (Chain of Thought) */}
        <Box
          data-chat-container
          sx={{
            flex: 1,
            overflowY: 'auto',
            p: 2,
            display: 'flex',
            flexDirection: 'column',
            gap: 1.5,
            '&::-webkit-scrollbar': {
              width: '8px',
            },
            '&::-webkit-scrollbar-track': {
              bgcolor: '#0D1117',
            },
            '&::-webkit-scrollbar-thumb': {
              bgcolor: '#30363D',
              borderRadius: '4px',
              '&:hover': {
                bgcolor: '#484F58',
              },
            },
          }}
        >
          {reasoningSteps.length === 0 && (
            <Box sx={{ textAlign: 'center', mt: 4, color: '#8B949E', px: 2 }}>
              <Typography variant="body2" sx={{ color: '#8B949E', mb: 1 }}>
                Ask a question to generate SQL
              </Typography>
              <Typography variant="caption" sx={{ mt: 1, display: 'block', color: '#6E7681', fontSize: '0.75rem' }}>
                Example: "Show me all loans from outstanding_daily that have been written off"
              </Typography>
            </Box>
          )}

          {reasoningSteps.map((step) => {
            const isExpanded = expandedSteps.has(step.id);
            const isUserMessage = step.type === 'info' && step.id.startsWith('user-');
            
            return (
              <Paper
                key={step.id}
                sx={{
                  p: 1.5,
                  bgcolor: isUserMessage ? '#1F6FEB' : '#0D1117',
                  border: isUserMessage ? 'none' : `1px solid ${getStepColor(step.type)}40`,
                  borderRadius: 1,
                  boxShadow: isUserMessage ? 'none' : 'none',
                }}
              >
                <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
                  <Box sx={{ mt: 0.5 }}>
                    {getStepIcon(step.type)}
                  </Box>
                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    {isUserMessage ? (
                      <Typography sx={{ color: '#E6EDF3', fontSize: '0.875rem' }}>
                        {step.content}
                      </Typography>
                    ) : (
                      <>
                        {step.content.length > 200 ? (
                          <>
                            <Box
                              onClick={() => toggleStepExpansion(step.id)}
                              sx={{
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                gap: 0.5,
                                mb: 0.5,
                              }}
                            >
                              {isExpanded ? (
                                <ExpandLessIcon sx={{ color: '#8B949E', fontSize: 16 }} />
                              ) : (
                                <ExpandMoreIcon sx={{ color: '#8B949E', fontSize: 16 }} />
                              )}
                              <Typography
                                variant="caption"
                                sx={{ color: '#8B949E', fontSize: '0.75rem' }}
                              >
                                {isExpanded ? 'Collapse' : 'Expand'}
                              </Typography>
                            </Box>
                            <Collapse in={isExpanded}>
                              <Box>{formatStepContent(step.content)}</Box>
                            </Collapse>
                            {!isExpanded && (
                              <Typography
                                sx={{
                                  color: '#8B949E',
                                  fontSize: '0.75rem',
                                  fontStyle: 'italic',
                                }}
                              >
                                {step.content.substring(0, 200)}...
                              </Typography>
                            )}
                          </>
                        ) : (
                          <Box>{formatStepContent(step.content)}</Box>
                        )}
                      </>
                    )}
                  </Box>
                </Box>
              </Paper>
            );
          })}

          {isLoading && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, p: 2 }}>
              <CircularProgress size={16} sx={{ color: '#58A6FF' }} />
              <Typography variant="body2" sx={{ color: '#8B949E' }}>
                Analyzing query and generating SQL...
              </Typography>
            </Box>
          )}

          {error && (
            <Alert severity="error" sx={{ mt: 1 }}>
              {error}
            </Alert>
          )}

          <div ref={chatEndRef} />
        </Box>

        {/* Chat Input */}
        <Box
          sx={{
            p: 2,
            borderTop: '1px solid #30363D',
            bgcolor: '#0D1117',
          }}
        >
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-end' }}>
            <TextField
              fullWidth
              multiline
              maxRows={4}
              placeholder="Ask a question to generate SQL..."
              value={chatInput}
              onChange={(e) => setChatInput(e.target.value)}
              onKeyPress={handleKeyPress}
              disabled={isLoading}
              sx={{
                '& .MuiOutlinedInput-root': {
                  bgcolor: '#161B22',
                  color: '#E6EDF3',
                  borderRadius: 1,
                  '& fieldset': {
                    borderColor: '#30363D',
                  },
                  '&:hover fieldset': {
                    borderColor: '#484F58',
                  },
                  '&.Mui-focused fieldset': {
                    borderColor: '#58A6FF',
                  },
                },
                '& .MuiInputBase-input': {
                  color: '#E6EDF3',
                  '&::placeholder': {
                    color: '#6E7681',
                    opacity: 1,
                  },
                },
              }}
            />
            <IconButton
              onClick={handleSendQuery}
              disabled={!chatInput.trim() || isLoading}
              sx={{
                bgcolor: '#238636',
                color: '#E6EDF3',
                minWidth: 40,
                height: 40,
                '&:hover': {
                  bgcolor: '#2EA043',
                },
                '&:disabled': {
                  bgcolor: '#21262D',
                  color: '#6E7681',
                },
              }}
            >
              <SendIcon />
            </IconButton>
          </Box>
          <Typography variant="caption" sx={{ color: '#6E7681', mt: 0.5, display: 'block', fontSize: '0.75rem' }}>
            Press Enter to send, Shift+Enter for new line
          </Typography>
        </Box>
      </Box>
    </Box>
  );
};

