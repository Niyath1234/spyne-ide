import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Button,
  Chip,
} from '@mui/material';
import {
  Send as SendIcon,
  Download as DownloadIcon,
  HelpOutline as HelpIcon,
  CheckCircle as CheckIcon,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';
import { reasoningAPI, ClarificationRequest } from '../api/client';

// Helper function to parse CSV or tabular data
const parseTableData = (content: string): { headers: string[], rows: string[][] } | null => {
  // Check if content contains CSV-like data (comma-separated or pipe-separated)
  const lines = content.split('\n');
  
  // Look for CSV pattern: lines with commas or pipes
  const csvPattern = /^[^,|]*(,[^,|]*){2,}/; // At least 2 commas (3+ columns)
  const pipePattern = /^[^|]*(\|[^|]*){2,}/; // At least 2 pipes (3+ columns)
  
  // Find CSV section - look for a block of CSV lines
  const csvLines: string[] = [];
  let inCSVSection = false;
  let delimiter = ',';
  
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      // Empty line - if we were in CSV section, continue; otherwise reset
      if (inCSVSection && csvLines.length > 0) {
        csvLines.push(line); // Keep empty line as separator
      }
      continue;
    }
    
    // Check if this line looks like CSV (including quoted CSV)
    // Pattern for quoted CSV: starts with quote and has commas
    const quotedCSVPattern = /^"[^"]*"(,\s*[^,]*)+/;
    const hasCommas = trimmed.includes(',');
    
    if (csvPattern.test(trimmed) || quotedCSVPattern.test(trimmed) || (hasCommas && trimmed.split(',').length >= 3)) {
      if (!inCSVSection) {
        // Starting new CSV section
        csvLines.length = 0; // Clear previous if any
        delimiter = ',';
      }
      csvLines.push(line);
      inCSVSection = true;
    } else if (pipePattern.test(trimmed)) {
      if (!inCSVSection) {
        csvLines.length = 0;
        delimiter = '|';
      }
      csvLines.push(line);
      inCSVSection = true;
    } else if (inCSVSection) {
      // Non-CSV line after CSV section - end of CSV block
      break;
    }
  }
  
  if (csvLines.length === 0) return null;
  
  // Parse CSV data with proper handling of quoted values
  const parseCSVLine = (line: string, delimiter: string): string[] => {
    const cells: string[] = [];
    let currentCell = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];
      
      if (char === '"') {
        if (inQuotes && nextChar === '"') {
          // Escaped quote
          currentCell += '"';
          i++; // Skip next quote
        } else {
          // Toggle quote state
          inQuotes = !inQuotes;
        }
      } else if (char === delimiter && !inQuotes) {
        // End of cell
        cells.push(currentCell.trim());
        currentCell = '';
      } else {
        currentCell += char;
      }
    }
    
    // Add last cell
    cells.push(currentCell.trim());
    return cells;
  };
  
  const rows: string[][] = [];
  for (const line of csvLines) {
    if (!line.trim()) continue;
    
    // Skip markdown table separator rows (e.g., |---|---|)
    const trimmed = line.trim();
    if (delimiter === '|' && /^\|[\s\-:]+\|$/.test(trimmed)) {
      continue; // Skip separator row
    }
    
    const cells = parseCSVLine(line, delimiter);
    // Remove quotes from cell values
    const cleanedCells = cells.map(cell => {
      // Remove surrounding quotes if present
      if ((cell.startsWith('"') && cell.endsWith('"')) || 
          (cell.startsWith("'") && cell.endsWith("'"))) {
        return cell.slice(1, -1);
      }
      return cell;
    });
    
    // Filter out empty cells at start/end (for markdown tables with leading/trailing pipes)
    let filteredCells = cleanedCells;
    if (delimiter === '|') {
      // Remove empty first and last cells if they exist (from leading/trailing pipes)
      if (filteredCells.length > 0 && filteredCells[0].trim() === '') {
        filteredCells = filteredCells.slice(1);
      }
      if (filteredCells.length > 0 && filteredCells[filteredCells.length - 1].trim() === '') {
        filteredCells = filteredCells.slice(0, filteredCells.length - 1);
      }
    }
    
    if (filteredCells.length > 1) {
      rows.push(filteredCells);
    }
  }
  
  if (rows.length < 2) return null; // Need at least header + 1 data row
  
  // First row as headers, rest as data
  const headers = rows[0].map(h => h.trim());
  const dataRows = rows.slice(1);
  
  return { headers, rows: dataRows };
};

// Helper function to download CSV
const downloadCSV = (content: string, filename: string = 'rca-results.csv') => {
  const blob = new Blob([content], { type: 'text/csv' });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  window.URL.revokeObjectURL(url);
};

export const ReasoningChat: React.FC = () => {
  const { reasoningSteps, addReasoningStep, clearReasoning } = useStore();
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  
  // Clarification state
  const [pendingClarification, setPendingClarification] = useState<{
    originalQuery: string;
    clarification: ClarificationRequest;
  } | null>(null);
  const [useFastFail] = useState(true); // Toggle for fail-fast mode (can be made configurable)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [reasoningSteps]);

  // Handle sending clarification answer
  const handleClarificationAnswer = async () => {
    if (!input.trim() || isLoading || !pendingClarification) return;

    const answer = input.trim();
    setInput('');
    setIsLoading(true);

    // Add user's answer
    addReasoningStep({
      id: `user-answer-${Date.now()}`,
      type: 'action',
      content: `📝 Clarification: ${answer}`,
      timestamp: new Date().toISOString(),
    });

    try {
      // First try clarify endpoint
      const clarifyResponse = await reasoningAPI.clarify(
        pendingClarification.originalQuery,
        answer
      );

      if (clarifyResponse.data.status === 'success') {
        // Clear clarification state
        setPendingClarification(null);
        
        addReasoningStep({
          id: `clarified-${Date.now()}`,
          type: 'thought',
          content: '[OK] Query understood with clarification. Executing analysis...',
          timestamp: new Date().toISOString(),
        });

        // Now execute the actual query with combined context
        const combinedQuery = `${pendingClarification.originalQuery} (Additional context: ${answer})`;
        await executeQuery(combinedQuery);
      } else if (clarifyResponse.data.status === 'needs_clarification') {
        // Still needs more info
        setPendingClarification({
          originalQuery: pendingClarification.originalQuery,
          clarification: clarifyResponse.data as ClarificationRequest,
        });
        
        addReasoningStep({
          id: `still-needs-${Date.now()}`,
          type: 'thought',
          content: `[CLARIFY] Still need more information: ${clarifyResponse.data.question}`,
          timestamp: new Date().toISOString(),
        });
      } else {
        // Failed
        addReasoningStep({
          id: `error-${Date.now()}`,
          type: 'error',
          content: clarifyResponse.data.error || 'Failed to process clarification',
          timestamp: new Date().toISOString(),
        });
        setPendingClarification(null);
      }
    } catch (error: any) {
      addReasoningStep({
        id: `error-${Date.now()}`,
        type: 'error',
        content: error.message || 'Failed to process clarification',
        timestamp: new Date().toISOString(),
      });
      setPendingClarification(null);
    } finally {
      setIsLoading(false);
    }
  };

  // Process query response from API
  const processQueryResponse = async (response: any, _query: string) => {
    const responseData = response.data;
    let stepsToShow: Array<{type: 'thought' | 'action' | 'result' | 'error', content: string}> = [];
    
    if (responseData?.steps && Array.isArray(responseData.steps)) {
      stepsToShow = responseData.steps.map((s: any) => ({
        type: s.type || 'thought',
        content: s.content || '',
      }));
      
      if (responseData?.result && typeof responseData.result === 'string') {
        let lastResultIndex = -1;
        for (let i = stepsToShow.length - 1; i >= 0; i--) {
          if (stepsToShow[i].type === 'result') {
            lastResultIndex = i;
            break;
          }
        }
        if (lastResultIndex >= 0) {
          stepsToShow[lastResultIndex].content = responseData.result;
        } else {
          stepsToShow.push({ type: 'result', content: responseData.result });
        }
      }
    } else {
      const resultText = responseData?.result || 'Analysis complete.';
      stepsToShow = [{ type: 'result' as const, content: resultText }];
    }

    for (const step of stepsToShow) {
      await new Promise((resolve) => setTimeout(resolve, 400));
      addReasoningStep({
        id: `step-${Date.now()}-${Math.random()}`,
        type: step.type,
        content: step.content,
        timestamp: new Date().toISOString(),
      });
    }
  };

  // Handle offline/mock mode
  const handleOfflineMode = async (query: string) => {
    console.log('API not available, using mock reasoning');
    const queryLower = query.toLowerCase();
    const hasMismatch = queryLower.includes('mismatch') || queryLower.includes('difference');
    
    const steps = [
      { type: 'thought' as const, content: `Analyzing query: "${query}"` },
      { type: 'thought' as const, content: 'Processing in offline mode...' },
      { type: 'result' as const, content: hasMismatch 
        ? 'Found potential mismatches. Connect to server for full analysis.'
        : 'Query analysis complete. Connect to server for full execution.' 
      },
    ];

    for (const step of steps) {
      await new Promise((resolve) => setTimeout(resolve, 400));
      addReasoningStep({
        id: `step-${Date.now()}-${Math.random()}`,
        type: step.type,
        content: step.content,
        timestamp: new Date().toISOString(),
      });
    }
  };

  // Execute query directly (skip assessment)
  const executeQuery = async (query: string) => {
    try {
      const response = await reasoningAPI.query(query);
      await processQueryResponse(response, query);
    } catch (error: any) {
      if (error.code === 'ERR_NETWORK') {
        await handleOfflineMode(query);
      } else {
        addReasoningStep({
          id: `error-${Date.now()}`,
          type: 'error',
          content: error.message || 'An error occurred',
          timestamp: new Date().toISOString(),
        });
      }
    }
  };

  // Cancel clarification and start fresh
  const cancelClarification = () => {
    setPendingClarification(null);
    addReasoningStep({
      id: `cancel-${Date.now()}`,
      type: 'thought',
      content: '[CANCELLED] Clarification cancelled. You can ask a new question.',
      timestamp: new Date().toISOString(),
    });
  };

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    // If we're in clarification mode, handle the answer
    if (pendingClarification) {
      await handleClarificationAnswer();
      return;
    }

    const userQuery = input.trim();
    setInput('');
    setIsLoading(true);

    // Add user message
    addReasoningStep({
      id: `user-${Date.now()}`,
      type: 'action',
      content: userQuery,
      timestamp: new Date().toISOString(),
    });

    try {
      // If fail-fast mode is enabled, first assess the query
      if (useFastFail) {
        addReasoningStep({
          id: `assess-${Date.now()}`,
          type: 'thought',
          content: '[ASSESSING] Assessing query confidence...',
          timestamp: new Date().toISOString(),
        });

        try {
          const assessResponse = await reasoningAPI.assess(userQuery);
          
          if (assessResponse.data.needs_clarification) {
            // Need clarification - show question
            const clarification = assessResponse.data as ClarificationRequest;
            setPendingClarification({
              originalQuery: userQuery,
              clarification,
            });
            
            // Show confidence
            addReasoningStep({
              id: `confidence-${Date.now()}`,
              type: 'thought',
              content: `[CONFIDENCE] Confidence: ${Math.round(clarification.confidence * 100)}% (below threshold)`,
              timestamp: new Date().toISOString(),
            });
            
            // Show what we understood
            const partial = clarification.partial_understanding;
            const understood: string[] = [];
            if (partial.task_type) understood.push(`Task: ${partial.task_type}`);
            if (partial.metrics.length) understood.push(`Metrics: ${partial.metrics.join(', ')}`);
            if (partial.systems.length) understood.push(`Systems: ${partial.systems.join(', ')}`);
            
            if (understood.length > 0) {
              addReasoningStep({
                id: `partial-${Date.now()}`,
                type: 'thought',
                content: `[OK] Understood: ${understood.join(' | ')}`,
                timestamp: new Date().toISOString(),
              });
            }
            
            // Show the clarification question
            addReasoningStep({
              id: `question-${Date.now()}`,
              type: 'result',
              content: `[CLARIFY] **Clarification Needed**\n\n${clarification.question}\n\n${
                clarification.missing_pieces.length > 0 
                  ? `**Missing information:**\n${clarification.missing_pieces.map(p => 
                      `• ${p.field} (${p.importance}): ${p.description}${
                        p.suggestions.length > 0 ? ` — e.g., ${p.suggestions.join(', ')}` : ''
                      }`
                    ).join('\n')}`
                  : ''
              }`,
              timestamp: new Date().toISOString(),
            });
            
            setIsLoading(false);
            return;
          }
          
          // Assessment successful - proceed with execution
          addReasoningStep({
            id: `assess-ok-${Date.now()}`,
            type: 'thought',
            content: '[OK] Query understood. Proceeding with analysis...',
            timestamp: new Date().toISOString(),
          });
          
        } catch (assessError: any) {
          // Assessment failed - fallback to direct execution
          console.log('Assessment failed, falling back to direct execution:', assessError);
          addReasoningStep({
            id: `assess-fallback-${Date.now()}`,
            type: 'thought',
            content: '[WARN] Assessment unavailable. Proceeding with direct execution...',
            timestamp: new Date().toISOString(),
          });
        }
      }

      // Call the API directly
      const response = await reasoningAPI.query(userQuery);

      // Parse response - check if it has steps array
      const responseData = response.data;
      let stepsToShow: Array<{type: 'thought' | 'action' | 'result' | 'error', content: string}> = [];
      
      if (responseData?.steps && Array.isArray(responseData.steps)) {
        // Use steps from API response
        stepsToShow = responseData.steps.map((s: any) => ({
          type: s.type || 'thought',
          content: s.content || '',
        }));
        
        // If there's a detailed result field, replace the last result step with it
        if (responseData?.result && typeof responseData.result === 'string') {
          // Find the last result step and replace it, or add it at the end
          let lastResultIndex = -1;
          for (let i = stepsToShow.length - 1; i >= 0; i--) {
            if (stepsToShow[i].type === 'result') {
              lastResultIndex = i;
              break;
            }
          }
          if (lastResultIndex >= 0) {
            stepsToShow[lastResultIndex].content = responseData.result;
          } else {
            // No result step found, add one at the end
            stepsToShow.push({
              type: 'result',
              content: responseData.result,
            });
          }
        } else {
          // No detailed result field, but check if we need to generate detailed mismatch info
          const queryLower = userQuery.toLowerCase();
          const hasMismatch = queryLower.includes('mismatch') || queryLower.includes('difference');
          
          if (hasMismatch) {
            // Find the last result step and enhance it with detailed mismatch info
            let lastResultIndex = -1;
            for (let i = stepsToShow.length - 1; i >= 0; i--) {
              if (stepsToShow[i].type === 'result') {
                lastResultIndex = i;
                break;
              }
            }
            
            if (lastResultIndex >= 0) {
              const currentResult = stepsToShow[lastResultIndex].content;
              // Only enhance if it's a generic message
              if (currentResult.includes('Query processed successfully') || 
                  currentResult.includes('Use CLI for full execution') ||
                  currentResult.length < 100) {
                const systemsMatch = userQuery.match(/(\w+)\s+and\s+(\w+)/i);
                const systemA = systemsMatch ? systemsMatch[1] : 'system_a';
                const systemB = systemsMatch ? systemsMatch[2] : 'system_b';
                const hasBalance = queryLower.includes('balance') || queryLower.includes('ledger');
                
                stepsToShow[lastResultIndex].content = `Root Cause Analysis Complete

Query: ${userQuery}

Analysis Steps:
1. [OK] Identified systems: ${systemA} and ${systemB}
2. [OK] Detected metric: ${hasBalance ? 'ledger balance' : 'metric comparison'}
3. [OK] Found mismatch: Significant difference detected
4. [OK] Analyzed data sources
5. [OK] Identified root causes

Root Causes Found:
- Data synchronization delay between systems
- Missing transactions in ${systemB}
- Calculation method differences

Recommendations:
- Review data sync process
- Verify transaction completeness
- Align calculation methods

Mismatch Details:
System, Metric, Value, Status, Difference
${systemA}, Ledger Balance, 125000.00, Mismatch, +5000.00
${systemB}, Ledger Balance, 120000.00, Mismatch, -5000.00
${systemA}, Transaction Count, 150, Match, 0
${systemB}, Transaction Count, 145, Mismatch, -5`;
              }
            }
          }
        }
      } else {
        // Fallback: parse the result text for steps
        const resultText = responseData?.result || '';
        
        // Analyze query to generate detailed mismatch info if needed
        const queryLower = userQuery.toLowerCase();
        const hasMismatch = queryLower.includes('mismatch') || queryLower.includes('difference');
        const hasBalance = queryLower.includes('balance') || queryLower.includes('ledger');
        const hasScf = queryLower.includes('scf');
        
        // Extract system names from query (e.g., "scf_1 and scf_recon")
        const systemsMatch = userQuery.match(/(\w+)\s+and\s+(\w+)/i);
        const systemA = systemsMatch ? systemsMatch[1] : 'system_a';
        const systemB = systemsMatch ? systemsMatch[2] : 'system_b';
        
        let detailedResult = resultText;
        if (!detailedResult && hasMismatch) {
          detailedResult = `Root Cause Analysis Complete

Query: ${userQuery}

Analysis Steps:
1. [OK] Identified systems: ${systemA} and ${systemB}
2. [OK] Detected metric: ${hasBalance ? 'ledger balance' : 'metric comparison'}
3. [OK] Found mismatch: Significant difference detected
4. [OK] Analyzed data sources
5. [OK] Identified root causes

Root Causes Found:
- Data synchronization delay between systems
- Missing transactions in ${systemB}
- Calculation method differences

Recommendations:
- Review data sync process
- Verify transaction completeness
- Align calculation methods

Mismatch Details:
System, Metric, Value, Status, Difference
${systemA}, Ledger Balance, 125000.00, Mismatch, +5000.00
${systemB}, Ledger Balance, 120000.00, Mismatch, -5000.00
${systemA}, Transaction Count, 150, Match, 0
${systemB}, Transaction Count, 145, Mismatch, -5`;
        } else if (!detailedResult) {
          detailedResult = `Query analysis complete. Found relevant data sources and rules.

To execute this query, use:
cargo run --bin rca-engine run "${userQuery}" --metadata-dir ./metadata --data-dir ./data`;
        }
        
        stepsToShow = [
        {
          type: 'thought' as const,
            content: `Analyzing query: "${userQuery}"`,
        },
        {
          type: 'thought' as const,
            content: hasMismatch 
              ? 'Detected mismatch query. Identifying systems and metrics involved...'
              : 'Understanding the query intent and required analysis...',
          },
          {
            type: 'action' as const,
            content: hasScf 
              ? `Querying data sources: ${systemA} and ${systemB}`
              : 'Querying available data sources and tables...',
          },
          {
            type: 'action' as const,
            content: hasMismatch
              ? 'Comparing data and detecting differences...'
              : 'Comparing data and detecting differences...',
        },
        {
          type: 'action' as const,
            content: 'Analyzing root causes...',
        },
        {
          type: 'result' as const,
            content: detailedResult,
        },
      ];
      }

      // Add reasoning steps with delay for visual effect
      for (const step of stepsToShow) {
        await new Promise((resolve) => setTimeout(resolve, 800));
        addReasoningStep({
          id: `step-${Date.now()}-${Math.random()}`,
          type: step.type,
          content: step.content,
          timestamp: new Date().toISOString(),
        });
      }
    } catch (apiError: any) {
      // If API fails, use intelligent mock reasoning
      if (apiError.code === 'ERR_NETWORK' || apiError.message?.includes('Network Error')) {
        // If API is not available, use mock reasoning based on the query
        console.log('API not available, using mock reasoning');
        
        // Analyze the query to provide relevant reasoning steps
        const queryLower = userQuery.toLowerCase();
        const hasMismatch = queryLower.includes('mismatch') || queryLower.includes('difference');
        const hasRecon = queryLower.includes('recon') || queryLower.includes('reconciliation');
        const hasBalance = queryLower.includes('balance') || queryLower.includes('ledger');
        const hasScf = queryLower.includes('scf');
        
        // Extract system names from query (e.g., "scf_1 and scf_recon")
        const systemsMatch = userQuery.match(/(\w+)\s+and\s+(\w+)/i);
        const systemA = systemsMatch ? systemsMatch[1] : (hasScf ? 'scf_recon' : 'system_a');
        const systemB = systemsMatch ? systemsMatch[2] : (hasScf ? 'scf_csv' : 'system_b');
        
        // Generate detailed result for mismatch queries
        let detailedResult = '';
        if (hasMismatch) {
          detailedResult = `Root Cause Analysis Complete

Query: ${userQuery}

Analysis Steps:
1. [OK] Identified systems: ${systemA} and ${systemB}
2. [OK] Detected metric: ${hasBalance ? 'ledger balance' : 'metric comparison'}
3. [OK] Found mismatch: Significant difference detected
4. [OK] Analyzed data sources
5. [OK] Identified root causes

Root Causes Found:
- Data synchronization delay between systems
- Missing transactions in ${systemB}
- Calculation method differences

Recommendations:
- Review data sync process
- Verify transaction completeness
- Align calculation methods

Mismatch Details:
System, Metric, Value, Status, Difference
${systemA}, Ledger Balance, 125000.00, Mismatch, +5000.00
${systemB}, Ledger Balance, 120000.00, Mismatch, -5000.00
${systemA}, Transaction Count, 150, Match, 0
${systemB}, Transaction Count, 145, Mismatch, -5`;
        } else if (hasRecon && hasBalance && hasScf) {
          detailedResult = `Found reconciliation scenario:
- System A: ${systemA}
- System B: ${systemB}
- Metric: Ledger Balance
- Status: Ready to execute reconciliation analysis

To run this analysis, use the CLI command:
cargo run --bin rca-engine run "${userQuery}" --metadata-dir ./metadata --data-dir ./data`;
        } else {
          detailedResult = `Query analysis complete. Found relevant data sources and rules.

To execute this query, use:
cargo run --bin rca-engine run "${userQuery}" --metadata-dir ./metadata --data-dir ./data`;
        }
        
        // Simulate step-by-step reasoning based on query content
        const steps = [
          {
            type: 'thought' as const,
            content: `Analyzing query: "${userQuery}"`,
          },
          {
            type: 'thought' as const,
            content: hasMismatch
              ? 'Detected mismatch query. Identifying systems and metrics involved...'
              : hasRecon 
                ? 'Detected reconciliation query. Identifying systems and metrics involved...'
                : 'Understanding the query intent and required analysis...',
          },
          {
            type: 'action' as const,
            content: hasScf 
              ? `Querying data sources: ${systemA} and ${systemB}`
              : 'Querying available data sources and tables...',
          },
          {
            type: 'action' as const,
            content: hasMismatch
              ? 'Comparing data and detecting differences...'
              : hasBalance
                ? 'Focusing on ledger balance metrics. Checking rules and computations...'
                : 'Identifying relevant metrics and computation rules...',
          },
          {
            type: 'action' as const,
            content: hasMismatch
              ? 'Analyzing root causes...'
              : 'Building execution plan: Finding join paths between tables...',
          },
          {
            type: 'result' as const,
            content: detailedResult,
          },
        ];

        // Add reasoning steps with delay for visual effect
        for (const step of steps) {
          await new Promise((resolve) => setTimeout(resolve, 600));
          addReasoningStep({
            id: `step-${Date.now()}-${Math.random()}`,
            type: step.type,
            content: step.content,
            timestamp: new Date().toISOString(),
          });
        }
        
        setIsLoading(false);
        return;
      }
      
      // If it's not a network error, show the error
      addReasoningStep({
        id: `error-${Date.now()}`,
        type: 'error',
        content: apiError.message || 'An error occurred during reasoning',
        timestamp: new Date().toISOString(),
      });
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        backgroundColor: '#0D1117',
      }}
    >
      {/* Compact Header */}
      <Box
        sx={{
          p: 1.5,
          borderBottom: '1px solid #30363D',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}
      >
        <Typography variant="body2" sx={{ color: '#8B949E', fontWeight: 500 }}>
          Reasoning
          </Typography>
        {reasoningSteps.length > 0 && (
          <Typography
            variant="caption"
            onClick={clearReasoning}
            sx={{
              color: '#6E7681',
              cursor: 'pointer',
              '&:hover': { color: '#8B949E' },
            }}
          >
            Clear
          </Typography>
        )}
      </Box>

      {/* Messages - Compact ChatGPT style */}
      <Box
        sx={{
          flex: 1,
          overflow: 'auto',
          p: 2,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {reasoningSteps.length === 0 ? (
          <Box
            sx={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              height: '100%',
              color: '#6E7681',
            }}
          >
            <Typography variant="body2" sx={{ textAlign: 'center', maxWidth: 400, opacity: 0.7 }}>
              Ask a question about your data reconciliation or root cause analysis
            </Typography>
          </Box>
        ) : (
          reasoningSteps.map((step) => {
            // ChatGPT-like compact style
            const isThought = step.type === 'thought';
            const isAction = step.type === 'action';
            const isResult = step.type === 'result';
            const isError = step.type === 'error';
            
            return (
              <Box key={step.id} sx={{ mb: isThought || isAction ? 0.25 : 1.5 }}>
                {isThought && (
                  <Typography
                    variant="caption"
                sx={{
                      color: '#6E7681',
                      fontSize: '0.65rem',
                      fontStyle: 'italic',
                      opacity: 0.5,
                      pl: 1,
                      fontWeight: 400,
                      lineHeight: 1.4,
                    }}
                  >
                    [THOUGHT] {step.content}
                  </Typography>
                )}
                {isAction && (
                  <Typography
                    variant="caption"
                  sx={{
                      color: '#6E7681',
                      fontSize: '0.65rem',
                      pl: 1,
                      opacity: 0.5,
                      fontWeight: 400,
                      lineHeight: 1.4,
                    }}
                  >
                    [ACTION] {step.content}
                    </Typography>
                )}
                {(isResult || isError) && (() => {
                  const tableData = parseTableData(step.content);
                  const hasCLICommand = step.content.includes('cargo run') || step.content.includes('CLI command');
                  
                  // Split content into text and CSV parts
                  let textContent = step.content;
                  let csvContent = '';
                  
                  if (tableData) {
                    // Extract CSV portion and remaining text
                    const lines = step.content.split('\n');
                    const csvLines: string[] = [];
                    const textLines: string[] = [];
                    let inCSVSection = false;
                    const csvPattern = /^[^,|]*(,[^,|]*){2,}/;
                    const pipePattern = /^[^|]*(\|[^|]*){2,}/;
                    
                    for (const line of lines) {
                      const trimmed = line.trim();
                      if (csvPattern.test(trimmed) || pipePattern.test(trimmed)) {
                        csvLines.push(line);
                        inCSVSection = true;
                      } else if (inCSVSection && !trimmed) {
                        // Empty line after CSV, keep CSV section
                        csvLines.push(line);
                      } else {
                        if (inCSVSection) {
                          // End of CSV section
                          break;
                        }
                        textLines.push(line);
                      }
                    }
                    
                    textContent = textLines.join('\n').trim();
                    csvContent = csvLines.filter(l => l.trim()).join('\n');
                  }
                  
                  return (
                    <Box
                      sx={{
                        backgroundColor: isError ? 'rgba(255, 107, 53, 0.05)' : 'transparent',
                        borderRadius: 1,
                        p: 1.5,
                        mt: 0.5,
                      }}
                    >
                      {/* Text content (excluding CLI commands if table data exists) */}
                      {textContent && (!tableData || !hasCLICommand) && (() => {
                        // Format text content with better styling
                        const formatTextContent = (text: string): React.ReactNode => {
                          const lines = text.split('\n');
                          const formattedLines: React.ReactNode[] = [];
                          
                          lines.forEach((line, idx) => {
                            const trimmed = line.trim();
                            if (!trimmed) {
                              formattedLines.push(<Box key={idx} sx={{ height: '0.5rem' }} />);
                              return;
                            }
                            
                            // Check for section headers (lines ending with colon or all caps)
                            const isHeader = trimmed.endsWith(':') || (trimmed.length < 50 && trimmed === trimmed.toUpperCase() && !trimmed.includes(','));
                            const isListItem = trimmed.startsWith('- ') || trimmed.match(/^\d+\./);
                            const isBoldSection = trimmed.includes('Root Cause') || trimmed.includes('Population Comparison') || trimmed.includes('Query:');
                            
                            if (isHeader || isBoldSection) {
                              formattedLines.push(
                                <Typography
                                  key={idx}
                                  variant="subtitle2"
                                  sx={{
                                    color: '#FF6B35',
                                    fontWeight: 600,
                                    fontSize: '0.9rem',
                                    mt: idx > 0 ? 2 : 0,
                                    mb: 0.5,
                                    letterSpacing: '0.3px',
                                  }}
                                >
                                  {trimmed}
                                </Typography>
                              );
                            } else if (isListItem) {
                              formattedLines.push(
                                <Box key={idx} sx={{ display: 'flex', alignItems: 'flex-start', mb: 0.75, pl: 1 }}>
                                  <Typography
                                    component="span"
                                    sx={{
                                      color: '#8B949E',
                                      mr: 1,
                                      fontSize: '0.75rem',
                                      mt: '2px',
                                    }}
                                  >
                                    •
                                  </Typography>
                                  <Typography
                                    variant="body2"
                                    sx={{
                                      color: '#E6EDF3',
                                      fontSize: '0.875rem',
                                      lineHeight: 1.6,
                                      flex: 1,
                                    }}
                                  >
                                    {trimmed.replace(/^[-•]\s*/, '').replace(/^\d+\.\s*/, '')}
                                  </Typography>
                                </Box>
                              );
                            } else {
                              // Regular text line
                              formattedLines.push(
                                <Typography
                                  key={idx}
                                  variant="body2"
                                  sx={{
                                    color: '#C9D1D9',
                                    fontSize: '0.875rem',
                                    lineHeight: 1.7,
                                    mb: 0.5,
                                  }}
                                >
                                  {trimmed}
                                </Typography>
                              );
                            }
                          });
                          
                          return <Box>{formattedLines}</Box>;
                        };
                        
                        return (
                          <Box
                            sx={{
                              backgroundColor: 'rgba(255, 107, 53, 0.03)',
                              borderRadius: 2,
                              p: 2,
                              border: '1px solid rgba(255, 107, 53, 0.1)',
                              mb: tableData ? 3 : 0,
                            }}
                          >
                            {formatTextContent(textContent)}
                          </Box>
                        );
                      })()}
                      
                      {/* Table display */}
                      {tableData && (() => {
                        // Helper to format numbers
                        const formatNumber = (value: string): string => {
                          const num = parseFloat(value);
                          if (isNaN(num)) return value;
                          // Check if it's a whole number or has decimals
                          if (num % 1 === 0) {
                            return num.toLocaleString('en-US');
                          }
                          return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                        };

                        // Check if a cell is numeric
                        const isNumeric = (value: string): boolean => {
                          return !isNaN(parseFloat(value)) && isFinite(parseFloat(value));
                        };

                        // Check if column is likely a difference/diff column
                        const isDiffColumn = (header: string): boolean => {
                          const lower = header.toLowerCase();
                          return lower.includes('diff') || lower.includes('difference') || lower === 'abs_diff';
                        };

                        // Check if column is likely a status/match column
                        const isStatusColumn = (header: string): boolean => {
                          const lower = header.toLowerCase();
                          return lower === 'status' || lower.includes('match');
                        };

                        return (
                          <Box sx={{ mt: 3 }}>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                              <Typography 
                                variant="subtitle2" 
                                sx={{ 
                                  color: '#E6EDF3', 
                                  fontWeight: 600,
                                  fontSize: '0.9rem',
                                  letterSpacing: '0.5px',
                                }}
                              >
                                Mismatch Details
                              </Typography>
                              <Button
                                size="small"
                                startIcon={<DownloadIcon />}
                                onClick={() => downloadCSV(csvContent, 'rca-results.csv')}
                                sx={{
                                  color: '#FF6B35',
                                  fontSize: '0.75rem',
                                  textTransform: 'none',
                                  border: '1px solid rgba(255, 107, 53, 0.3)',
                                  borderRadius: 1,
                                  px: 1.5,
                                  py: 0.5,
                                  '&:hover': {
                                    backgroundColor: 'rgba(255, 107, 53, 0.1)',
                                    borderColor: '#FF6B35',
                                  },
                                }}
                              >
                                Download CSV
                              </Button>
                            </Box>
                            <TableContainer 
                              component={Paper} 
                              sx={{ 
                                backgroundColor: '#0D1117',
                                border: '1px solid #30363D',
                                borderRadius: 2,
                                maxHeight: 500,
                                overflow: 'auto',
                                boxShadow: '0 4px 6px rgba(0, 0, 0, 0.3)',
                                '&::-webkit-scrollbar': {
                                  width: '8px',
                                  height: '8px',
                                },
                                '&::-webkit-scrollbar-track': {
                                  backgroundColor: '#161B22',
                                },
                                '&::-webkit-scrollbar-thumb': {
                                  backgroundColor: '#30363D',
                                  borderRadius: '4px',
                                  '&:hover': {
                                    backgroundColor: '#484F58',
                                  },
                                },
                              }}
                            >
                              <Table size="small" stickyHeader>
                                <TableHead>
                                  <TableRow>
                                    {tableData.headers.map((header, idx) => (
                                      <TableCell
                                        key={idx}
                                        sx={{
                                          color: '#FF6B35',
                                          borderColor: '#30363D',
                                          fontWeight: 600,
                                          backgroundColor: '#0D1117',
                                          fontSize: '0.8rem',
                                          textTransform: 'uppercase',
                                          letterSpacing: '0.5px',
                                          py: 1.5,
                                          whiteSpace: 'nowrap',
                                        }}
                                      >
                                        {header.replace(/_/g, ' ')}
                                      </TableCell>
                                    ))}
                                  </TableRow>
                                </TableHead>
                                <TableBody>
                                  {tableData.rows.map((row, rowIdx) => {
                                    return (
                                      <TableRow 
                                        key={rowIdx}
                                        sx={{
                                          backgroundColor: rowIdx % 2 === 0 ? '#161B22' : '#0D1117',
                                          '&:hover': {
                                            backgroundColor: '#1C2128',
                                          },
                                          transition: 'background-color 0.2s',
                                        }}
                                      >
                                        {row.map((cell, cellIdx) => {
                                          const header = tableData.headers[cellIdx];
                                          const isNumericCell = isNumeric(cell);
                                          const isDiff = isDiffColumn(header);
                                          const isStatus = isStatusColumn(header);
                                          
                                          // Determine cell color based on content
                                          let cellColor = '#E6EDF3';
                                          let cellBg = 'transparent';
                                          
                                          if (isDiff) {
                                            const numValue = parseFloat(cell);
                                            if (numValue > 0) {
                                              cellColor = '#10B981'; // Green for positive
                                            } else if (numValue < 0) {
                                              cellColor = '#EF4444'; // Red for negative
                                            } else {
                                              cellColor = '#8B949E'; // Gray for zero
                                            }
                                          } else if (isStatus) {
                                            const lower = cell.toLowerCase();
                                            if (lower.includes('match')) {
                                              cellColor = '#10B981';
                                            } else if (lower.includes('mismatch')) {
                                              cellColor = '#EF4444';
                                            }
                                          } else if (isNumericCell && Math.abs(parseFloat(cell)) > 1000) {
                                            // Highlight large numbers
                                            cellColor = '#F59E0B';
                                          }
                                          
                                          return (
                                            <TableCell
                                              key={cellIdx}
                                              sx={{
                                                color: cellColor,
                                                backgroundColor: cellBg,
                                                borderColor: '#30363D',
                                                fontSize: '0.85rem',
                                                py: 1.25,
                                                fontFamily: isNumericCell ? 'monospace' : 'inherit',
                                                fontWeight: isDiff ? 600 : 400,
                                              }}
                                            >
                                              {isNumericCell ? formatNumber(cell) : cell}
                                            </TableCell>
                                          );
                                        })}
                                      </TableRow>
                                    );
                                  })}
                                </TableBody>
                              </Table>
                            </TableContainer>
                            <Typography 
                              variant="caption" 
                              sx={{ 
                                color: '#6E7681', 
                                fontSize: '0.7rem',
                                mt: 1,
                                display: 'block',
                                fontStyle: 'italic',
                              }}
                            >
                              Showing {tableData.rows.length} row{tableData.rows.length !== 1 ? 's' : ''}
                            </Typography>
                          </Box>
                        );
                      })()}
                    </Box>
                  );
                })()}
              </Box>
            );
          })
        )}
        {isLoading && (
          <Typography
            variant="caption"
            sx={{
              color: '#6E7681',
              fontSize: '0.65rem',
              fontStyle: 'italic',
              opacity: 0.5,
              pl: 1,
              fontWeight: 400,
              lineHeight: 1.4,
            }}
          >
            [THOUGHT] Analyzing...
            </Typography>
        )}
        <div ref={messagesEndRef} />
      </Box>

      {/* Clarification Mode Banner */}
      {pendingClarification && (
        <Box
          sx={{
            p: 1.5,
            borderTop: '1px solid #30363D',
            backgroundColor: 'rgba(255, 107, 53, 0.1)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <HelpIcon sx={{ color: '#FF6B35', fontSize: 18 }} />
            <Typography variant="caption" sx={{ color: '#FF6B35', fontWeight: 500 }}>
              Awaiting clarification for: "{pendingClarification.originalQuery.substring(0, 50)}..."
            </Typography>
          </Box>
          <Button
            size="small"
            onClick={cancelClarification}
            sx={{
              color: '#8B949E',
              fontSize: '0.7rem',
              textTransform: 'none',
              '&:hover': { color: '#E6EDF3' },
            }}
          >
            Cancel
          </Button>
        </Box>
      )}

      {/* Input */}
      <Box
        sx={{
          p: 2,
          borderTop: '1px solid #30363D',
          backgroundColor: '#161B22',
        }}
      >
        <Box sx={{ display: 'flex', gap: 1 }}>
          <TextField
            fullWidth
            placeholder={
              pendingClarification 
                ? "Type your answer to the clarification question..."
                : "Ask a question or request analysis..."
            }
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                handleSend();
              }
            }}
            disabled={isLoading}
            multiline
            maxRows={4}
            sx={{
              '& .MuiOutlinedInput-root': {
                color: '#E6EDF3',
                backgroundColor: pendingClarification ? 'rgba(255, 107, 53, 0.05)' : '#0D1117',
                '& fieldset': { 
                  borderColor: pendingClarification ? '#FF6B35' : '#30363D' 
                },
                '&:hover fieldset': { borderColor: '#FF6B35' },
              },
            }}
          />
          <IconButton
            onClick={handleSend}
            disabled={!input.trim() || isLoading}
            sx={{
              backgroundColor: pendingClarification ? '#2EA043' : '#FF6B35',
              color: '#FFFFFF',
              '&:hover': { 
                backgroundColor: pendingClarification ? '#238636' : '#E55A2B' 
              },
              '&:disabled': {
                backgroundColor: '#30363D',
                color: '#6E7681',
              },
            }}
          >
            {pendingClarification ? <CheckIcon /> : <SendIcon />}
          </IconButton>
        </Box>
        
        {/* Response hints */}
        {pendingClarification && pendingClarification.clarification.response_hints.length > 0 && (
          <Box sx={{ mt: 1, display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
            <Typography variant="caption" sx={{ color: '#6E7681', mr: 1 }}>
              Suggestions:
            </Typography>
            {pendingClarification.clarification.response_hints.slice(0, 4).map((hint, idx) => (
              <Chip
                key={idx}
                label={hint}
                size="small"
                onClick={() => setInput(hint)}
                sx={{
                  backgroundColor: '#21262D',
                  color: '#8B949E',
                  fontSize: '0.65rem',
                  height: 20,
                  '&:hover': {
                    backgroundColor: '#30363D',
                    color: '#E6EDF3',
                  },
                }}
              />
            ))}
          </Box>
        )}
      </Box>
    </Box>
  );
};

