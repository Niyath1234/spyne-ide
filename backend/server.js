const express = require('express');
const cors = require('cors');
const { exec } = require('child_process');
const { promisify } = require('util');
const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const csv = require('csv-parser');

const execAsync = promisify(exec);
const app = express();
const PORT = 8080;

// Enterprise features: Logging middleware
const logRequest = (req, res, next) => {
  const startTime = Date.now();
  const requestId = `req-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  req.requestId = requestId;
  
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    level: 'INFO',
    request_id: requestId,
    method: req.method,
    path: req.path,
    ip: req.ip,
    user_agent: req.get('user-agent'),
  }));
  
  res.on('finish', () => {
    const duration = Date.now() - startTime;
    console.log(JSON.stringify({
      timestamp: new Date().toISOString(),
      level: res.statusCode >= 400 ? 'ERROR' : 'INFO',
      request_id: requestId,
      method: req.method,
      path: req.path,
      status: res.statusCode,
      duration_ms: duration,
    }));
  });
  
  next();
};

// Enterprise features: Error handling middleware
const errorHandler = (err, req, res, next) => {
  console.error(JSON.stringify({
    timestamp: new Date().toISOString(),
    level: 'ERROR',
    request_id: req.requestId || 'unknown',
    method: req.method,
    path: req.path,
    error: err.message,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined,
  }));
  
  res.status(err.status || 500).json({
    success: false,
    error: err.message || 'Internal server error',
    request_id: req.requestId,
  });
};

app.use(cors());
app.use(express.json());
app.use(logRequest);

// Store pipelines in memory (in production, use a database)
let pipelines = [];
let reasoningHistory = [];
let rules = [];

// Helper to run Rust CLI commands
async function runRustCommand(command, args = []) {
  const projectRoot = path.join(__dirname, '..');
  const fullCommand = `cd ${projectRoot} && cargo run -- ${command} ${args.join(' ')}`;
  
  try {
    const { stdout, stderr } = await execAsync(fullCommand, { 
      maxBuffer: 10 * 1024 * 1024 // 10MB buffer
    });
    return { success: true, output: stdout, error: stderr };
  } catch (error) {
    return { success: false, output: error.stdout, error: error.stderr || error.message };
  }
}

// Helper to read CSV and get summary
async function readCSVSummary(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => {
        resolve({
          rowCount: results.length,
          columns: Object.keys(results[0] || {}),
          sample: results.slice(0, 5),
        });
      })
      .on('error', reject);
  });
}

// API Routes

// Root route
app.get('/', (req, res) => {
  res.json({
    name: 'RCA Engine API',
    version: '1.0.0',
    status: 'running',
    endpoints: {
      query: {
        load_prerequisites: '/api/query/load-prerequisites',
        generate_sql: '/api/query/generate-sql',
      },
      agent: {
        run: '/api/agent/run',
        continue: '/api/agent/continue',
      },
      reasoning: {
        query: '/api/reasoning/query',
        assess: '/api/reasoning/assess',
        clarify: '/api/reasoning/clarify',
      },
      assistant: {
        ask: '/api/assistant/ask',
      },
      pipelines: '/api/pipelines',
      rules: '/api/rules',
      ingestion: '/api/ingestion',
      metadata: {
        ingest_table: '/api/metadata/ingest/table',
        ingest_join: '/api/metadata/ingest/join',
        ingest_rules: '/api/metadata/ingest/rules',
        ingest_complete: '/api/metadata/ingest/complete',
      },
    },
  });
});

// Rules API
app.get('/api/rules', (req, res) => {
  res.json({ rules });
});

app.get('/api/rules/:id', (req, res) => {
  const rule = rules.find(r => r.id === req.params.id);
  if (!rule) {
    return res.status(404).json({ error: 'Rule not found' });
  }
  res.json(rule);
});

app.post('/api/rules', (req, res) => {
  const rule = {
    id: `rule-${Date.now()}`,
    ...req.body,
    createdAt: new Date().toISOString(),
  };
  rules.push(rule);
  res.json(rule);
});

app.put('/api/rules/:id', (req, res) => {
  const index = rules.findIndex(r => r.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: 'Rule not found' });
  }
  rules[index] = { ...rules[index], ...req.body, updatedAt: new Date().toISOString() };
  res.json(rules[index]);
});

app.delete('/api/rules/:id', (req, res) => {
  const index = rules.findIndex(r => r.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: 'Rule not found' });
  }
  rules.splice(index, 1);
  res.json({ success: true });
});

// Pipelines
app.get('/api/pipelines', (req, res) => {
  res.json(pipelines);
});

app.post('/api/pipelines', async (req, res) => {
  const pipeline = {
    id: `pipeline-${Date.now()}`,
    ...req.body,
    status: 'inactive',
    createdAt: new Date().toISOString(),
  };
  pipelines.push(pipeline);
  res.json(pipeline);
});

app.put('/api/pipelines/:id', (req, res) => {
  const index = pipelines.findIndex(p => p.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: 'Pipeline not found' });
  }
  pipelines[index] = { ...pipelines[index], ...req.body };
  res.json(pipelines[index]);
});

app.delete('/api/pipelines/:id', (req, res) => {
  const index = pipelines.findIndex(p => p.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: 'Pipeline not found' });
  }
  pipelines.splice(index, 1);
  res.json({ success: true });
});

app.post('/api/pipelines/:id/run', async (req, res) => {
  const pipeline = pipelines.find(p => p.id === req.params.id);
  if (!pipeline) {
    return res.status(404).json({ error: 'Pipeline not found' });
  }

  try {
    // Update status
    pipeline.status = 'active';
    pipeline.lastRun = new Date().toISOString();

    // For CSV files, we can use the Rust CLI
    if (pipeline.type === 'csv' && pipeline.source) {
      // Copy CSV to data directory if needed
      const dataDir = path.join(__dirname, '..', 'data');
      const fileName = path.basename(pipeline.source);
      const destPath = path.join(dataDir, fileName);
      
      await fsPromises.mkdir(dataDir, { recursive: true });
      await fsPromises.copyFile(pipeline.source, destPath);

      // Get CSV summary
      const summary = await readCSVSummary(pipeline.source);
      
      res.json({
        success: true,
        message: `Ingested ${summary.rowCount} rows from ${fileName}`,
        summary,
        pipeline,
      });
    } else {
      res.json({
        success: true,
        message: 'Pipeline executed',
        pipeline,
      });
    }
  } catch (error) {
    pipeline.status = 'error';
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/pipelines/:id/status', (req, res) => {
  const pipeline = pipelines.find(p => p.id === req.params.id);
  if (!pipeline) {
    return res.status(404).json({ error: 'Pipeline not found' });
  }
  res.json({ status: pipeline.status, lastRun: pipeline.lastRun });
});

// Query Regeneration API - Load metadata and generate SQL from natural language
// Query Builder API - Load metadata and business rules, build SQL from natural language
app.get('/api/query/load-prerequisites', async (req, res) => {
  try {
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
    
    const { stdout, stderr } = await execAsync(`cd ${projectRoot} && python3 ${scriptPath} load`, {
      maxBuffer: 10 * 1024 * 1024,
    });
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.post('/api/query/generate-sql', async (req, res) => {
  try {
    const { query, use_llm } = req.body;
    
    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
    
    // Pass query via stdin, with use_llm flag (defaults to true if OPENAI_API_KEY is set)
    const useLLMFlag = use_llm !== false && process.env.OPENAI_API_KEY ? true : false;
    const inputData = JSON.stringify({ command: 'generate', query, use_llm: useLLMFlag });
    
    // Pass environment variables (especially OPENAI_API_KEY) to Python script
    const env = { ...process.env };
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && echo '${inputData}' | python3 ${scriptPath}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: env, // Pass all environment variables including OPENAI_API_KEY
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

// Reasoning API - Uses LLM Query Generator with Chain of Thought
app.post('/api/reasoning/query', async (req, res) => {
  const { query, context } = req.body;
  
  if (!query) {
    return res.status(400).json({ error: 'Query is required' });
  }
  
  try {
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
    
    // Use LLM by default if API key is available
    const useLLM = process.env.OPENAI_API_KEY ? true : false;
    const inputData = JSON.stringify({ command: 'generate', query, use_llm: useLLM });
    
    // Pass environment variables (especially OPENAI_API_KEY) to Python script
    const env = { ...process.env };
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && echo '${inputData}' | python3 ${scriptPath}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: env,
      }
    );
    
    const result = JSON.parse(stdout.trim());
    
    // Convert reasoning_steps to the format expected by UI
    const steps = [];
    
    // Add initial step
    steps.push({
      type: 'thought',
      content: `🔍 Analyzing query: "${query}"`,
      timestamp: new Date().toISOString(),
    });
    
    // Add reasoning steps from LLM if available
    if (result.reasoning_steps && Array.isArray(result.reasoning_steps)) {
      result.reasoning_steps.forEach((stepContent, index) => {
        // Determine step type based on content
        let stepType = 'thought';
        if (stepContent.includes('✅') || stepContent.includes('Generated')) {
          stepType = 'result';
        } else if (stepContent.includes('❌') || stepContent.includes('Error')) {
          stepType = 'error';
        } else if (stepContent.includes('🔧') || stepContent.includes('Building')) {
          stepType = 'action';
        } else if (stepContent.includes('📊') || stepContent.includes('SQL')) {
          stepType = 'result';
        }
        
        steps.push({
          type: stepType,
          content: stepContent,
          timestamp: new Date(Date.now() + index * 100).toISOString(), // Slight delay for ordering
        });
      });
    } else {
      // Fallback: add basic steps
      steps.push({
        type: 'thought',
        content: '📊 Loading metadata and analyzing available tables...',
        timestamp: new Date().toISOString(),
      });
      
      if (result.success) {
        steps.push({
          type: 'action',
          content: '🤖 Generating SQL using LLM with comprehensive context...',
          timestamp: new Date().toISOString(),
        });
        
        if (result.sql) {
          steps.push({
            type: 'result',
            content: `✅ Generated SQL:\n\n\`\`\`sql\n${result.sql}\n\`\`\``,
            timestamp: new Date().toISOString(),
          });
        }
      } else {
        steps.push({
          type: 'error',
          content: `❌ Error: ${result.error || 'Unknown error'}`,
          timestamp: new Date().toISOString(),
        });
      }
    }
    
    // Add SQL result if available
    if (result.sql && !steps.some(s => s.content.includes(result.sql.substring(0, 50)))) {
      steps.push({
        type: 'result',
        content: `\`\`\`sql\n${result.sql}\n\`\`\``,
        timestamp: new Date().toISOString(),
      });
    }
    
    // Add warnings if any
    if (result.warnings) {
      steps.push({
        type: 'thought',
        content: `⚠️  Warnings: ${result.warnings}`,
        timestamp: new Date().toISOString(),
      });
    }
    
    reasoningHistory.push(...steps);
    
    res.json({
      result: result.sql || result.error || 'Query processed',
      steps,
      sql: result.sql,
      intent: result.intent,
      method: result.method || 'llm_with_full_context',
    });
  } catch (error) {
    const errorSteps = [
      {
        type: 'error',
        content: `❌ Error processing query: ${error.message}`,
        timestamp: new Date().toISOString(),
      },
    ];
    
    res.status(500).json({
      result: `Error: ${error.message}`,
      steps: errorSteps,
      error: error.message,
    });
  }
});

// Ingestion API
app.post('/api/ingestion/ingest', async (req, res) => {
  const { config } = req.body;
  res.json({ success: true, message: 'Ingestion started' });
});

app.post('/api/ingestion/validate', async (req, res) => {
  const { config } = req.body;
  res.json({ valid: true, message: 'Configuration is valid' });
});

app.post('/api/ingestion/preview', async (req, res) => {
  const { config } = req.body;
  res.json({ preview: 'Preview data' });
});

// Metadata Ingestion API - Natural Language to Structured JSON
app.post('/api/metadata/ingest/table', async (req, res) => {
  try {
    const { table_description, system, output_file } = req.body;
    
    if (!table_description) {
      return res.status(400).json({ error: 'table_description is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'metadata_ingestion_api.py');
    
    // Build command arguments
    const args = ['table', JSON.stringify(table_description)];
    if (system) args.push(system);
    
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && python3 ${scriptPath} ${args.join(' ')}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env },
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.post('/api/metadata/ingest/join', async (req, res) => {
  try {
    const { join_condition, output_file } = req.body;
    
    if (!join_condition) {
      return res.status(400).json({ error: 'join_condition is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'metadata_ingestion_api.py');
    
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && python3 ${scriptPath} join ${JSON.stringify(join_condition)}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env },
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.post('/api/metadata/ingest/rules', async (req, res) => {
  try {
    const { rules_text, output_file } = req.body;
    
    if (!rules_text) {
      return res.status(400).json({ error: 'rules_text is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'metadata_ingestion_api.py');
    
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && python3 ${scriptPath} rules ${JSON.stringify(rules_text)}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env },
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.post('/api/metadata/ingest/complete', async (req, res) => {
  try {
    const { metadata_text, system } = req.body;
    
    if (!metadata_text) {
      return res.status(400).json({ error: 'metadata_text is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'metadata_ingestion_api.py');
    
    // Build command arguments
    const args = ['complete', JSON.stringify(metadata_text)];
    if (system) args.push(system);
    
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && python3 ${scriptPath} ${args.join(' ')}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env },
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

// Agent API - Agent-based query processing
// Store agent sessions in memory (in production, use a database)
let agentSessions = {};

app.post('/api/agent/run', async (req, res) => {
  try {
    const { session_id, user_query, ui_context } = req.body;
    
    if (!session_id || !user_query) {
      return res.status(400).json({ 
        status: 'error',
        error: 'session_id and user_query are required' 
      });
    }
    
    // Initialize session if it doesn't exist
    if (!agentSessions[session_id]) {
      agentSessions[session_id] = {
        id: session_id,
        history: [],
        context: ui_context || {},
        createdAt: new Date().toISOString(),
      };
    }
    
    // Add user query to history
    agentSessions[session_id].history.push({
      role: 'user',
      content: user_query,
      timestamp: new Date().toISOString(),
    });
    
    // Use query generation API to process the query
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
    const useLLM = process.env.OPENAI_API_KEY ? true : false;
    const inputData = JSON.stringify({ command: 'generate', query: user_query, use_llm: useLLM });
    
    const env = { ...process.env };
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && echo '${inputData}' | python3 ${scriptPath}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: env,
      }
    );
    
    const result = JSON.parse(stdout.trim());
    
    // Add assistant response to history
    const assistantResponse = {
      role: 'assistant',
      content: result.sql || result.error || 'Query processed',
      sql: result.sql,
      intent: result.intent,
      timestamp: new Date().toISOString(),
    };
    agentSessions[session_id].history.push(assistantResponse);
    
    // Determine response type
    if (result.success && result.sql) {
      res.json({
        status: 'success',
        message: 'Query generated successfully',
        data: {
          sql: result.sql,
          intent: result.intent,
        },
        final_answer: result.sql,
        trace: result.reasoning_steps || [],
      });
    } else {
      res.json({
        status: 'needs_clarification',
        message: result.error || 'Query needs clarification',
        clarification: {
          query: user_query,
          question: result.error || 'Could you provide more details?',
          confidence: 0.5,
          missing_pieces: result.error ? [{
            field: 'query',
            importance: 'high',
            description: result.error,
          }] : [],
        },
      });
    }
  } catch (error) {
    res.status(500).json({
      status: 'error',
      error: error.message,
    });
  }
});

app.post('/api/agent/continue', async (req, res) => {
  try {
    const { session_id, choice_id, ui_context } = req.body;
    
    if (!session_id || !choice_id) {
      return res.status(400).json({ 
        status: 'error',
        error: 'session_id and choice_id are required' 
      });
    }
    
    const session = agentSessions[session_id];
    if (!session) {
      return res.status(404).json({ 
        status: 'error',
        error: 'Session not found' 
      });
    }
    
    // Update context with choice
    session.context = { ...session.context, ...ui_context };
    
    // Continue processing based on choice
    // For now, return success - in production, implement choice handling logic
    res.json({
      status: 'success',
      message: 'Choice processed',
      data: {
        choice_id,
        session_id,
      },
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      error: error.message,
    });
  }
});

// Reasoning API - Additional endpoints
app.post('/api/reasoning/assess', async (req, res) => {
  try {
    const { query } = req.body;
    
    if (!query) {
      return res.status(400).json({ error: 'query is required' });
    }
    
    // Use query generation to assess the query
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
    const useLLM = process.env.OPENAI_API_KEY ? true : false;
    const inputData = JSON.stringify({ command: 'generate', query, use_llm: useLLM });
    
    const env = { ...process.env };
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && echo '${inputData}' | python3 ${scriptPath}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: env,
      }
    );
    
    const result = JSON.parse(stdout.trim());
    
    // Assess query quality
    const assessment = {
      query,
      clarity: result.success ? 'high' : 'low',
      completeness: result.sql ? 'complete' : 'incomplete',
      confidence: result.success ? 0.9 : 0.3,
      intent: result.intent || null,
      sql_generated: !!result.sql,
      warnings: result.warnings || [],
      suggestions: result.error ? [`Consider: ${result.error}`] : [],
    };
    
    res.json(assessment);
  } catch (error) {
    res.status(500).json({
      error: error.message,
      assessment: {
        query: req.body.query,
        clarity: 'unknown',
        completeness: 'unknown',
        confidence: 0.0,
        error: error.message,
      },
    });
  }
});

app.post('/api/reasoning/clarify', async (req, res) => {
  try {
    const { query, answer } = req.body;
    
    if (!query) {
      return res.status(400).json({ error: 'query is required' });
    }
    
    // Generate clarification based on query and previous answer
    const clarification = {
      query,
      answer: answer || null,
      needs_clarification: true,
      clarification_questions: [
        'Could you specify which metrics you want to see?',
        'What time range are you interested in?',
        'Are there any specific filters you want to apply?',
      ],
      suggestions: [
        'Try rephrasing your query with more specific details',
        'Include metric names if you know them',
        'Specify date ranges or filters',
      ],
    };
    
    res.json(clarification);
  } catch (error) {
    res.status(500).json({
      error: error.message,
    });
  }
});

// Assistant API - General assistant queries
app.post('/api/assistant/ask', async (req, res) => {
  try {
    const { question } = req.body;
    
    if (!question) {
      return res.status(400).json({ 
        response_type: 'Error',
        error: 'question is required' 
      });
    }
    
    // Check if it's a SQL query request
    const isQueryRequest = question.toLowerCase().includes('query') || 
                          question.toLowerCase().includes('sql') ||
                          question.toLowerCase().includes('show') ||
                          question.toLowerCase().includes('get') ||
                          question.toLowerCase().includes('find');
    
    if (isQueryRequest) {
      // Use query generation API
      const path = require('path');
      const { exec } = require('child_process');
      const { promisify } = require('util');
      const execAsync = promisify(exec);
      
      const projectRoot = path.join(__dirname, '..');
      const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
      const useLLM = process.env.OPENAI_API_KEY ? true : false;
      const inputData = JSON.stringify({ command: 'generate', query: question, use_llm: useLLM });
      
      const env = { ...process.env };
      const { stdout, stderr } = await execAsync(
        `cd ${projectRoot} && echo '${inputData}' | python3 ${scriptPath}`,
        {
          maxBuffer: 10 * 1024 * 1024,
          env: env,
        }
      );
      
      const result = JSON.parse(stdout.trim());
      
      if (result.success && result.sql) {
        res.json({
          response_type: 'QueryResult',
          status: 'success',
          answer: `Here's the SQL query:\n\n\`\`\`sql\n${result.sql}\n\`\`\``,
          result: result.sql,
          intent: result.intent,
          validation: result.validation || null,
        });
      } else {
        res.json({
          response_type: 'NeedsClarification',
          status: 'failed',
          answer: result.error || 'I need more information to generate the query.',
          clarification: {
            query: question,
            question: result.error || 'Could you provide more details about what you want to query?',
            missing_pieces: result.error ? [{
              field: 'query',
              importance: 'high',
              description: result.error,
            }] : [],
          },
        });
      }
    } else {
      // General question - provide helpful response
      res.json({
        response_type: 'Answer',
        status: 'success',
        answer: `I can help you with SQL queries and data analysis. Try asking me things like:
- "Show me sales by region"
- "Query the customer table"
- "Get revenue metrics"
- "Find orders from last month"

For your question: "${question}", I can help you generate a SQL query if you provide more details about what data you want to retrieve.`,
      });
    }
  } catch (error) {
    res.status(500).json({
      response_type: 'Error',
      status: 'error',
      error: error.message,
      answer: `Sorry, I encountered an error: ${error.message}`,
    });
  }
});

// Error handler must be last
app.use(errorHandler);

app.listen(PORT, () => {
  console.log(`🚀 Backend API server running on http://localhost:${PORT}`);
  console.log(`📊 Ready to handle pipeline and reasoning requests`);
  console.log(`✨ New endpoints: /api/agent/*, /api/reasoning/assess, /api/reasoning/clarify, /api/assistant/ask`);
});

