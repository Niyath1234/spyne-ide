import React, { useState, useRef, useEffect } from 'react';
import { Box, TextField, IconButton, Typography } from '@mui/material';
import { Send as SendIcon } from '@mui/icons-material';
import ReactMarkdown from 'react-markdown';
import { assistantAPI } from '../api/client';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

export const ChatPanel: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const data = await assistantAPI.ask(userMessage.content);
      let answer = '';
      
      if (data.response_type === 'Answer') {
        answer = data.answer || '';
      } else if (data.response_type === 'QueryResult') {
        answer = data.answer || data.result || 'Query executed successfully.';
      } else if (data.response_type === 'NeedsClarification') {
        answer = `**Clarification Needed**\n\n${data.clarification?.question || 'I need more information.'}\n\n`;
        if (data.clarification?.missing_pieces) {
          answer += '**Missing information:**\n';
          data.clarification.missing_pieces.forEach((piece: any) => {
            answer += `- ${piece.field}: ${piece.description}\n`;
          });
        }
      } else if (data.response_type === 'Error') {
        answer = `**Error:** ${data.answer || data.error || 'An error occurred'}`;
      } else if (data.status === 'success' && data.intent) {
        // Intent compilation success - show validation info if available
        answer = `✅ **Query Compiled Successfully**\n\n`;
        if (data.validation) {
          if (data.validation.validated) {
            if (data.validation.is_valid) {
              answer += `✅ **Validation Passed** (Hallucination-Free)\n\n`;
              if (data.validation.warnings && data.validation.warnings.length > 0) {
                answer += `⚠️ **Validation Warnings:**\n`;
                data.validation.warnings.forEach((warning: string) => {
                  // Check if warning contains fuzzy match suggestion
                  const fuzzyMatch = warning.match(/resolved to '([^']+)' \(fuzzy match\)/);
                  if (fuzzyMatch) {
                    const original = warning.match(/Table '([^']+)'/)?.[1] || warning.match(/Column '([^']+)'/)?.[1] || '';
                    const corrected = fuzzyMatch[1];
                    answer += `- ${warning}\n`;
                    answer += `  💡 **Suggestion:** Approve correction "${original}" → "${corrected}"?\n`;
                    answer += `  [Approve] [Dismiss]\n\n`;
                  } else {
                    answer += `- ${warning}\n`;
                  }
                });
                answer += '\n';
              }
              if (data.validation.resolved_tables && data.validation.resolved_tables.length > 0) {
                answer += `**Resolved Tables:** ${data.validation.resolved_tables.join(', ')}\n`;
              }
            } else {
              answer += `❌ **Validation Failed** (Hallucination Detected)\n\n`;
              if (data.validation.errors && data.validation.errors.length > 0) {
                answer += `**Errors:**\n`;
                data.validation.errors.forEach((error: string) => {
                  answer += `- ${error}\n`;
                });
              }
            }
          }
        }
        answer += `\n**Intent:** ${JSON.stringify(data.intent, null, 2)}`;
      } else if (data.status === 'failed' || data.error) {
        // Compilation or validation failed
        answer = `❌ **Compilation Failed**\n\n`;
        if (data.error) {
          answer += `**Error:** ${data.error}\n\n`;
        }
        if (data.message) {
          answer += data.message;
        }
      } else {
        answer = data.answer || JSON.stringify(data, null, 2);
      }

      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: 'assistant',
        content: answer,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error: any) {
      const errorMessage: Message = {
        id: `error-${Date.now()}`,
        role: 'assistant',
        content: `**Error:** ${error.message || 'Failed to get response'}`,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', backgroundColor: '#1E1E1E' }}>
      <Box sx={{ flex: 1, overflowY: 'auto', px: 4, py: 4 }}>
        {messages.length === 0 ? (
          <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#858585' }}>
            <Typography variant="h6" sx={{ color: '#CCCCCC', mb: 1, fontFamily: 'ui-monospace, "Courier New", monospace' }}>
              Ask me anything about your data
            </Typography>
            <Typography variant="body2" sx={{ color: '#858585', textAlign: 'center', maxWidth: 500, fontFamily: 'ui-monospace, "Courier New", monospace' }}>
              I can help you query data, understand your systems, find mismatches, and answer questions about your metadata.
            </Typography>
          </Box>
        ) : (
          <Box sx={{ maxWidth: '800px', mx: 'auto' }}>
            {messages.map((message) => (
              <Box key={message.id} sx={{ mb: 4 }}>
                {message.role === 'user' && (
                  <Box sx={{ display: 'flex', justifyContent: 'flex-end' }}>
                    <Box
                      sx={{
                        maxWidth: '85%',
                        backgroundColor: '#252526',
                        color: '#CCCCCC',
                        border: '1px solid #3E3E42',
                        borderRadius: 0,
                        px: 2,
                        py: 1.25,
                        fontFamily: 'ui-monospace, "Courier New", monospace',
                        fontSize: '0.9375rem',
                      }}
                    >
                      {message.content}
                    </Box>
                  </Box>
                )}
                {message.role === 'assistant' && (
                  <Box sx={{ display: 'flex', justifyContent: 'flex-start' }}>
                    <Box
                      sx={{
                        maxWidth: '85%',
                        backgroundColor: '#252526',
                        border: '1px solid #3E3E42',
                        borderRadius: 0,
                        px: 2.5,
                        py: 2,
                        fontFamily: 'ui-monospace, "Courier New", monospace',
                      }}
                    >
                      <ReactMarkdown
                        components={{
                          p: ({ children }) => (
                            <Typography sx={{ color: '#CCCCCC', fontSize: '0.9375rem', lineHeight: 1.7, mb: 1.5, fontFamily: 'inherit' }}>
                              {children}
                            </Typography>
                          ),
                          strong: ({ children }) => (
                            <Box component="span" sx={{ fontWeight: 700, color: '#FFFFFF' }}>
                              {children}
                            </Box>
                          ),
                          ul: ({ children }) => (
                            <Box component="ul" sx={{ color: '#CCCCCC', pl: 2.5, mb: 1.5, fontFamily: 'inherit' }}>
                              {children}
                            </Box>
                          ),
                          code: ({ children }) => (
                            <Box
                              component="code"
                              sx={{
                                backgroundColor: '#1E1E1E',
                                color: '#CCCCCC',
                                padding: '2px 6px',
                                fontSize: '0.875em',
                                fontFamily: 'inherit',
                              }}
                            >
                              {children}
                            </Box>
                          ),
                          h3: ({ children }) => (
                            <Typography variant="h6" sx={{ color: '#FFFFFF', fontSize: '1rem', fontWeight: 600, mb: 1, mt: 2, fontFamily: 'inherit' }}>
                              {children}
                            </Typography>
                          ),
                          h4: ({ children }) => (
                            <Typography variant="subtitle1" sx={{ color: '#FFFFFF', fontSize: '0.9375rem', fontWeight: 600, mb: 0.5, mt: 1.5, fontFamily: 'inherit' }}>
                              {children}
                            </Typography>
                          ),
                        }}
                      >
                        {message.content}
                      </ReactMarkdown>
                    </Box>
                  </Box>
                )}
              </Box>
            ))}
            {isLoading && (
              <Box sx={{ display: 'flex', justifyContent: 'flex-start', mb: 4 }}>
                <Box
                  sx={{
                    backgroundColor: '#252526',
                    border: '1px solid #3E3E42',
                    borderRadius: 0,
                    px: 2.5,
                    py: 2,
                    fontFamily: 'ui-monospace, "Courier New", monospace',
                  }}
                >
                  <Typography sx={{ color: '#858585', fontSize: '0.875rem' }}>Thinking...</Typography>
                </Box>
              </Box>
            )}
            <div ref={messagesEndRef} />
          </Box>
        )}
      </Box>

      <Box sx={{ borderTop: '1px solid #3E3E42', backgroundColor: '#252526', px: 4, py: 2 }}>
        <Box sx={{ maxWidth: '800px', mx: 'auto', display: 'flex', gap: 1, alignItems: 'flex-end' }}>
          <TextField
            fullWidth
            multiline
            maxRows={6}
            placeholder="Ask a question..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                handleSend();
              }
            }}
            disabled={isLoading}
            sx={{
              '& .MuiOutlinedInput-root': {
                backgroundColor: '#1E1E1E',
                color: '#CCCCCC',
                borderRadius: 0,
                border: '1px solid #3E3E42',
                fontSize: '0.9375rem',
                fontFamily: 'ui-monospace, "Courier New", monospace',
                '& fieldset': { borderColor: '#3E3E42' },
                '&:hover fieldset': { borderColor: '#464647' },
                '&.Mui-focused fieldset': { borderColor: '#808080' },
                '&::placeholder': { color: '#606060' },
              },
            }}
          />
          <IconButton
            onClick={handleSend}
            disabled={!input.trim() || isLoading}
            sx={{
              backgroundColor: '#1E1E1E',
              color: '#CCCCCC',
              border: '1px solid #3E3E42',
              width: 40,
              height: 40,
              borderRadius: 0,
              '&:hover': { backgroundColor: '#2A2D2E', borderColor: '#464647' },
              '&:disabled': { backgroundColor: '#1E1E1E', color: '#606060', borderColor: '#3E3E42' },
            }}
          >
            <SendIcon sx={{ fontSize: 20 }} />
          </IconButton>
        </Box>
      </Box>
    </Box>
  );
};

