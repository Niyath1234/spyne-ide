# Project Structure

This document provides a complete index of all files and folders in the Spyne IDE repository.

## Directory Layout

```
RCA-Engine/
├── backend/                    # Python backend application
│   ├── __init__.py            # Backend package initialization
│   ├── .dockerignore          # Docker ignore rules for backend
│   ├── app.py                 # Development Flask application
│   ├── app_production.py      # Production Flask application
│   ├── gunicorn.conf.py       # Gunicorn configuration
│   ├── package.json           # Node.js dependencies (if any)
│   ├── package-lock.json      # Node.js lock file
│   ├── requirements.txt       # Python dependencies
│   ├── server.js              # Node.js server (if any)
│   │
│   ├── agentic/               # Agentic AI components
│   │   ├── __init__.py
│   │   ├── filter_agent.py    # Filter agent for query filtering
│   │   ├── intent_agent.py    # Intent extraction agent
│   │   ├── metric_agent.py    # Metric resolution agent
│   │   ├── metric_registry.py # Metric registry management
│   │   ├── orchestrator.py    # Agent orchestration
│   │   ├── shape_agent.py     # Query shape agent
│   │   ├── sql_renderer.py    # SQL rendering agent
│   │   ├── table_agent.py     # Table selection agent
│   │   └── verifier_agent.py  # Query verification agent
│   │
│   ├── api/                   # API endpoints and routes
│   │   ├── __init__.py
│   │   ├── clarification.py  # Clarification API endpoints
│   │   ├── drift.py           # Data drift detection API
│   │   ├── health.py          # Health check endpoints
│   │   ├── ingestion.py       # Metadata ingestion API
│   │   ├── joins.py           # Join-related API endpoints
│   │   ├── metrics.py         # Metrics API endpoints
│   │   ├── query_preview.py   # Query preview API
│   │   ├── query.py           # Main query API endpoints
│   │   └── table_state.py     # Table state management API
│   │
│   ├── auth/                  # Authentication and authorization
│   │   ├── __init__.py
│   │   ├── authenticator.py   # Authentication logic
│   │   ├── middleware.py      # Auth middleware
│   │   └── rate_limiter.py    # Rate limiting implementation
│   │
│   ├── config/                # Configuration management
│   │   ├── __init__.py
│   │   └── config_manager.py  # Configuration manager
│   │
│   ├── deployment/            # Deployment features
│   │   ├── __init__.py
│   │   ├── feature_flags.py   # Feature flag management
│   │   └── shadow_mode.py    # Shadow mode deployment
│   │
│   ├── execution/             # Query execution engines
│   │   ├── __init__.py
│   │   ├── kill_switch.py     # Execution kill switch
│   │   ├── query_firewall.py  # Query firewall for security
│   │   ├── rust_execution.py  # Rust execution engine bridge
│   │   └── sandbox.py         # Sandboxed execution environment
│   │
│   ├── failure_handling/       # Failure handling strategies
│   │   ├── __init__.py
│   │   ├── llm_failure.py     # LLM failure handling
│   │   ├── metadata_drift.py  # Metadata drift handling
│   │   └── partial_results.py # Partial result handling
│   │
│   ├── implementations/       # Interface implementations
│   │   ├── __init__.py
│   │   ├── cache.py           # Cache implementation
│   │   ├── database_executor.py # Database executor implementation
│   │   └── llm_provider.py    # LLM provider implementation
│   │
│   ├── interfaces/            # Interface definitions
│   │   ├── __init__.py
│   │   ├── cache.py           # Cache interface
│   │   ├── database_executor.py # Database executor interface
│   │   ├── llm_provider.py    # LLM provider interface
│   │   └── vector_db.py       # Vector database interface
│   │
│   ├── invariants/            # System invariants enforcement
│   │   ├── __init__.py
│   │   ├── boundary_enforcer.py # Boundary enforcement
│   │   ├── determinism.py     # Determinism guarantees
│   │   ├── fail_closed.py     # Fail-closed behavior
│   │   ├── fail_open.py       # Fail-open behavior
│   │   └── reproducibility.py # Reproducibility guarantees
│   │
│   ├── models/                # Data models
│   │   ├── __init__.py
│   │   └── table_state.py     # Table state model
│   │
│   ├── observability/         # Logging, metrics, tracing
│   │   ├── __init__.py
│   │   ├── correlation.py     # Correlation ID management
│   │   ├── golden_signals.py  # Golden signals monitoring
│   │   ├── metrics.py         # Metrics collection
│   │   └── structured_logging.py # Structured logging
│   │
│   ├── planes/                # Four-plane architecture components
│   │   ├── __init__.py
│   │   ├── execution.py       # Execution plane
│   │   ├── ingress.py         # Ingress plane
│   │   ├── planning.py        # Planning plane
│   │   └── presentation.py    # Presentation plane
│   │
│   ├── planning/              # Query planning logic
│   │   ├── __init__.py
│   │   ├── clarification_agent.py      # Clarification agent
│   │   ├── clarification_example.py    # Clarification examples
│   │   ├── clarification_metrics.py    # Clarification metrics
│   │   ├── clarification_resolver.py  # Clarification resolution
│   │   ├── guardrails.py      # Planning guardrails
│   │   ├── intent_extractor.py # Intent extraction
│   │   ├── join_type_resolver.py # Join type resolution
│   │   ├── metric_resolver.py  # Metric resolution
│   │   ├── multi_step_planner.py # Multi-step planning
│   │   ├── query_builder.py   # Query builder
│   │   ├── schema_selector.py # Schema selection
│   │   └── validator.py        # Query validation
│   │
│   ├── presentation/          # Result presentation
│   │   ├── __init__.py
│   │   ├── explainer.py       # Query explanation
│   │   └── formatter.py       # Result formatting
│   │
│   ├── rag/                   # Retrieval-Augmented Generation
│   │   ├── __init__.py
│   │   ├── retrieval.py       # RAG retrieval logic
│   │   └── versioning.py      # RAG versioning
│   │
│   ├── security/              # Security features
│   │   ├── __init__.py
│   │   ├── data_exfiltration.py # Data exfiltration prevention
│   │   └── prompt_injection.py  # Prompt injection prevention
│   │
│   ├── services/              # Business logic services
│   │   ├── __init__.py
│   │   ├── drift_detection.py # Drift detection service
│   │   └── query_resolution.py # Query resolution service
│   │
│   ├── stores/                # Data stores
│   │   ├── __init__.py
│   │   ├── contract_store.py  # Contract storage
│   │   ├── db_connection.py    # Database connection management
│   │   └── table_store.py      # Table metadata storage
│   │
│   ├── conversational_context.py # Conversational context management
│   ├── dimension_resolver.py     # Dimension resolution
│   ├── enterprise_pipeline.py    # Enterprise pipeline
│   ├── hybrid_knowledge_retriever.py # Hybrid knowledge retrieval
│   ├── join_learner.py            # Join learning
│   ├── knowledge_base_client.py   # Knowledge base client
│   ├── knowledge_graph.py         # Knowledge graph
│   ├── knowledge_register_rules.py # Knowledge register rules
│   ├── llm_query_generator.py     # LLM query generator
│   ├── metadata_ingestion_api.py   # Metadata ingestion API
│   ├── metadata_provider.py        # Metadata provider
│   ├── multi_stage_pipeline.py     # Multi-stage pipeline
│   ├── natural_language_metadata_parser.py # NL metadata parser
│   ├── node_level_metadata_accessor.py     # Node-level metadata access
│   ├── node_registry_client.py             # Node registry client
│   ├── orchestrator.py                    # Main orchestrator
│   ├── query_intent.py                    # Query intent
│   ├── query_modifier.py                  # Query modification
│   └── query_regeneration_api.py          # Query regeneration API
│   └── sql_builder.py                     # SQL builder
│   └── sql_completion.py                  # SQL completion
│
├── frontend/                  # Frontend UI (React/TypeScript)
│   ├── .dockerignore          # Docker ignore rules
│   ├── .gitignore             # Git ignore rules
│   ├── eslint.config.js       # ESLint configuration
│   ├── index.html             # Main HTML file
│   ├── nginx.conf             # Nginx configuration
│   ├── package.json           # Node.js dependencies
│   ├── package-lock.json      # Node.js lock file
│   ├── tsconfig.app.json      # TypeScript app config
│   ├── tsconfig.json          # TypeScript configuration
│   ├── tsconfig.node.json     # TypeScript node config
│   ├── vite.config.ts         # Vite configuration
│   │
│   ├── public/                # Static assets
│   │   └── vite.svg           # Vite logo
│   │
│   ├── src/                   # Source code
│   │   ├── api/               # API client
│   │   │   └── client.ts     # API client implementation
│   │   │
│   │   ├── assets/            # Static assets
│   │   │   └── react.svg     # React logo
│   │   │
│   │   ├── components/        # React components
│   │   │   ├── ChatPanel.tsx           # Chat panel component
│   │   │   ├── CursorLikeQueryBuilder.tsx # Cursor-like query builder
│   │   │   ├── GraphVisualizer.tsx     # Graph visualization
│   │   │   ├── HypergraphVisualizer.tsx # Hypergraph visualization
│   │   │   ├── KnowledgeRegister.tsx   # Knowledge registration UI
│   │   │   ├── MetadataRegister.tsx     # Metadata registration UI
│   │   │   ├── Monitoring.tsx          # Monitoring dashboard
│   │   │   ├── ObjectExplorer.tsx      # Object explorer
│   │   │   ├── PipelineManager.tsx      # Pipeline management UI
│   │   │   ├── QueryBuilder.tsx        # Query builder component
│   │   │   ├── QueryEditor.tsx         # Query editor component
│   │   │   ├── QueryRegeneration.tsx   # Query regeneration UI
│   │   │   ├── ReasoningChat.tsx       # Reasoning chat interface
│   │   │   ├── ResultsPanel.tsx        # Results display panel
│   │   │   ├── RulesView.tsx           # Rules view component
│   │   │   ├── Sidebar.tsx             # Sidebar component
│   │   │   └── TopBar.tsx              # Top bar component
│   │   │
│   │   ├── store/             # State management
│   │   │   └── useStore.ts   # Zustand store
│   │   │
│   │   ├── App.css            # App styles
│   │   ├── App.tsx            # Main App component
│   │   ├── index.css          # Global styles
│   │   ├── main.tsx           # Application entry point
│   │   └── theme.ts           # Theme configuration
│   │
│   └── hypergraph-visualizer/ # Hypergraph visualizer sub-project
│       ├── .gitignore
│       ├── backend/           # Backend for visualizer
│       │   ├── Cargo.toml
│       │   └── src/
│       │       └── main.rs
│       └── frontend/          # Frontend for visualizer
│           ├── index.html
│           ├── package.json
│           ├── tsconfig.json
│           ├── tsconfig.node.json
│           ├── vite.config.ts
│           └── src/
│               ├── api/
│               │   └── client.ts
│               ├── components/
│               │   └── HypergraphVisualizer.tsx
│               ├── App.tsx
│               ├── main.tsx
│               └── vite-env.d.ts
│
├── rust/                      # Rust core library
│   ├── __init__.py            # Python bindings initialization
│   ├── lib.rs                 # Library entry point
│   ├── main.rs                # Main binary entry point
│   │
│   ├── agent/                 # Agent system
│   │   ├── mod.rs
│   │   ├── catalog.rs         # Agent catalog
│   │   ├── contracts.rs       # Agent contracts
│   │   ├── executor.rs        # Agent executor
│   │   ├── grounding.rs       # Agent grounding
│   │   ├── memory.rs          # Agent memory
│   │   ├── planner.rs         # Agent planner
│   │   ├── runtime.rs         # Agent runtime
│   │   ├── service.rs         # Agent service
│   │   └── tools/             # Agent tools
│   │       ├── mod.rs
│   │       ├── kb_tools.rs    # Knowledge base tools
│   │       └── table_tools.rs # Table tools
│   │
│   ├── api/                   # API layer
│   │   └── user_api.rs        # User-facing API
│   │
│   ├── bin/                   # Binary executables
│   │   ├── build_rule.rs      # Rule builder binary
│   │   ├── convert_parquet_csv.rs # Parquet/CSV converter
│   │   ├── create_dummy_data.rs # Dummy data generator
│   │   ├── migrate_metadata.rs # Metadata migration tool
│   │   ├── node_registry_server.rs # Node registry server
│   │   └── server.rs          # Main server binary
│   │
│   ├── compiler/              # Query compiler
│   │   ├── mod.rs
│   │   ├── join_planner.rs    # Join planning
│   │   ├── join_resolver.rs   # Join resolution
│   │   └── join_validator.rs  # Join validation
│   │
│   ├── core/                  # Core functionality
│   │   ├── mod.rs
│   │   │
│   │   ├── agent/             # Core agent functionality
│   │   │   ├── mod.rs
│   │   │   └── rca_cursor/    # RCA cursor implementation
│   │   │       ├── mod.rs
│   │   │       ├── attribution.rs    # Attribution logic
│   │   │       ├── confidence.rs      # Confidence calculation
│   │   │       ├── cursor.rs          # Cursor implementation
│   │   │       ├── cursor_old.rs      # Legacy cursor
│   │   │       ├── diff.rs           # Diff computation
│   │   │       ├── entity_graph.rs    # Entity graph
│   │   │       ├── executor.rs       # Cursor executor
│   │   │       ├── grain_resolver.rs  # Grain resolution
│   │   │       ├── logical_plan.rs    # Logical plan
│   │   │       ├── planner.rs        # Cursor planner
│   │   │       └── validator.rs      # Cursor validator
│   │   │
│   │   ├── engine/            # Query engine core
│   │   │   ├── mod.rs
│   │   │   ├── aggregate_reconcile.rs # Aggregate reconciliation
│   │   │   ├── canonicalize.rs        # Query canonicalization
│   │   │   ├── logical_plan.rs        # Logical plan representation
│   │   │   ├── materialize.rs         # Query materialization
│   │   │   ├── row_diff.rs           # Row-level diff
│   │   │   └── storage.rs             # Storage abstraction
│   │   │
│   │   ├── identity/          # Identity management
│   │   │   ├── mod.rs
│   │   │   └── identity_graph.rs # Identity graph
│   │   │
│   │   ├── lineage/          # Data lineage
│   │   │   ├── mod.rs
│   │   │   ├── filter_trace.rs # Filter tracing
│   │   │   ├── join_trace.rs   # Join tracing
│   │   │   └── rule_trace.rs   # Rule tracing
│   │   │
│   │   ├── llm/               # LLM integration
│   │   │   ├── mod.rs
│   │   │   └── strategy.rs    # LLM strategy
│   │   │
│   │   ├── metrics/           # Metrics handling
│   │   │   ├── mod.rs
│   │   │   └── normalize.rs   # Metric normalization
│   │   │
│   │   ├── models/            # Core models
│   │   │   ├── mod.rs
│   │   │   └── canonical.rs   # Canonical model representation
│   │   │
│   │   ├── observability/     # Observability core
│   │   │   ├── mod.rs
│   │   │   ├── execution_trace.rs # Execution tracing
│   │   │   ├── trace_collector.rs  # Trace collection
│   │   │   └── trace_store.rs      # Trace storage
│   │   │
│   │   ├── performance/       # Performance optimizations
│   │   │   ├── mod.rs
│   │   │   ├── chunked_extraction.rs # Chunked extraction
│   │   │   ├── hash_diff.rs         # Hash-based diff
│   │   │   ├── parallel_executor.rs # Parallel execution
│   │   │   ├── parquet_cache.rs     # Parquet caching
│   │   │   ├── pushdown.rs          # Predicate pushdown
│   │   │   └── sampling.rs          # Query sampling
│   │   │
│   │   ├── rca/               # Root Cause Analysis
│   │   │   ├── mod.rs
│   │   │   ├── attribution.rs        # Attribution logic
│   │   │   ├── dimension_aggregation.rs # Dimension aggregation
│   │   │   ├── formatter_v2.rs       # Result formatting v2
│   │   │   ├── mode.rs               # RCA modes
│   │   │   ├── narrative.rs          # Narrative generation
│   │   │   ├── result_formatter.rs   # Result formatting
│   │   │   └── result_v2.rs          # Result v2
│   │   │
│   │   ├── safety/            # Safety mechanisms
│   │   │   ├── mod.rs
│   │   │   ├── failure_recovery.rs   # Failure recovery
│   │   │   └── resource_limits.rs    # Resource limits
│   │   │
│   │   └── trust/             # Trust and verification
│   │       ├── mod.rs
│   │       ├── evidence.rs     # Evidence collection
│   │       ├── replay.rs      # Query replay
│   │       └── verification.rs # Verification logic
│   │
│   ├── db/                    # Database layer
│   │   ├── mod.rs
│   │   ├── connection.rs      # Database connections
│   │   ├── metadata_repo.rs    # Metadata repository
│   │   └── query_history.rs   # Query history storage
│   │
│   ├── execution/             # Execution engines
│   │   ├── mod.rs
│   │   ├── agent_decision.rs  # Agent decision making
│   │   ├── duckdb_engine.rs   # DuckDB engine
│   │   ├── engine.rs          # Engine abstraction
│   │   ├── polars_engine.rs   # Polars engine
│   │   ├── profile.rs         # Query profiling
│   │   ├── result.rs          # Execution results
│   │   ├── router.rs          # Engine router
│   │   └── trino_engine.rs    # Trino engine
│   │
│   ├── execution_loop/        # Execution loop
│   │   ├── mod.rs
│   │   ├── error_classifier.rs # Error classification
│   │   ├── error_recovery.rs   # Error recovery
│   │   ├── loop.rs            # Main execution loop
│   │   └── query_plan.rs      # Query plan representation
│   │
│   ├── hypergraph/            # Hypergraph implementation
│   │   └── mod.rs
│   │
│   ├── ingestion/             # Data ingestion
│   │   ├── mod.rs
│   │   ├── columnar_writer.rs # Columnar data writer
│   │   ├── connector.rs       # Connector abstraction
│   │   ├── csv_connector.rs   # CSV connector
│   │   ├── join_inference.rs  # Join inference
│   │   ├── join_validator.rs  # Join validation
│   │   ├── json_connector.rs  # JSON connector
│   │   ├── lineage.rs         # Ingestion lineage
│   │   ├── orchestrator.rs    # Ingestion orchestrator
│   │   ├── schema_inference.rs # Schema inference
│   │   ├── simulator.rs       # Ingestion simulator
│   │   └── table_builder.rs   # Table builder
│   │
│   ├── intent/                # Intent processing
│   │   ├── mod.rs
│   │   ├── function_schema.rs # Function schema
│   │   └── semantic_intent.rs # Semantic intent
│   │
│   ├── learning/              # Learning and adaptation
│   │   ├── mod.rs
│   │   └── failure_analyzer.rs # Failure analysis
│   │
│   ├── modules/               # Modular components
│   │   ├── mod.rs
│   │   └── contract_registry.rs # Contract registry
│   │
│   ├── observability/         # Observability
│   │   ├── mod.rs
│   │   ├── execution_log.rs   # Execution logging
│   │   ├── logger.rs          # Logger implementation
│   │   └── metrics.rs         # Metrics collection
│   │
│   ├── python_bindings/       # Python bindings
│   │   ├── mod.rs
│   │   ├── agent_decision.rs  # Agent decision bindings
│   │   ├── execution.rs      # Execution bindings
│   │   ├── lib.rs            # Python bindings library
│   │   └── profile.rs        # Profiling bindings
│   │
│   ├── schema_rag/            # Schema RAG
│   │   ├── mod.rs
│   │   ├── embedder.rs        # Schema embedding
│   │   ├── retriever.rs       # Schema retrieval
│   │   └── vector_store.rs    # Vector store
│   │
│   ├── security/              # Security features
│   │   ├── mod.rs
│   │   ├── access_control.rs  # Access control
│   │   ├── policy.rs          # Security policies
│   │   ├── query_guards.rs    # Query guards
│   │   └── user_manager.rs    # User management
│   │
│   ├── semantic/              # Semantic analysis
│   │   ├── mod.rs
│   │   ├── dimension.rs       # Dimension handling
│   │   ├── join_graph.rs     # Join graph
│   │   ├── loader.rs          # Semantic loader
│   │   ├── metric.rs          # Metric handling
│   │   └── registry.rs        # Semantic registry
│   │
│   ├── semantic_completeness/ # Semantic completeness
│   │   ├── mod.rs
│   │   ├── entity_extractor.rs # Entity extraction
│   │   ├── entity_mapper.rs    # Entity mapping
│   │   ├── regeneration_loop.rs # Regeneration loop
│   │   ├── sql_validator.rs    # SQL validation
│   │   └── tests.rs           # Tests
│   │
│   ├── agent_prompts.rs       # Agent prompts
│   ├── agentic_prompts.rs     # Agentic prompts
│   ├── agentic_reasoner.rs    # Agentic reasoning
│   ├── ambiguity.rs           # Ambiguity detection
│   ├── data_assistant.rs      # Data assistant
│   ├── data_engineering.rs    # Data engineering utilities
│   ├── data_utils.rs          # Data utilities
│   ├── de_executor.rs         # Data engineering executor
│   ├── diff.rs                # Diff utilities
│   ├── drilldown.rs           # Drilldown functionality
│   ├── engine.py              # Python engine wrapper
│   ├── error.rs               # Error types
│   ├── execution_engine.rs    # Execution engine
│   ├── execution_planner.rs   # Execution planning
│   ├── explain.rs             # Query explanation
│   ├── explainability.rs      # Explainability
│   ├── explanation.rs         # Explanation generation
│   ├── faiss_fuzzy_matcher.rs # FAISS fuzzy matching
│   ├── fuzzy_matcher.rs       # Fuzzy matching
│   ├── goal_directed_explorer.rs # Goal-directed exploration
│   ├── grain_resolver.rs      # Grain resolution
│   ├── granularity_understanding.rs # Granularity understanding
│   ├── graph_adapter.rs       # Graph adapter
│   ├── graph_traversal.rs     # Graph traversal
│   ├── graph.rs               # Graph implementation
│   ├── hybrid_reasoner.rs     # Hybrid reasoning
│   ├── identity.rs            # Identity handling
│   ├── intelligent_rule_builder.rs # Intelligent rule building
│   ├── intelligent_rule_parser.rs  # Intelligent rule parsing
│   ├── intent_compiler.rs     # Intent compilation
│   ├── intent_validator.rs    # Intent validation
│   ├── join_inference.rs      # Join inference
│   ├── knowledge_base.rs      # Knowledge base
│   ├── knowledge_hints.rs     # Knowledge hints
│   ├── llm_value_matcher.rs  # LLM value matching
│   ├── llm.rs                 # LLM integration
│   ├── metadata.rs            # Metadata handling
│   ├── metric_similarity.rs   # Metric similarity
│   ├── node_registry.rs       # Node registry
│   ├── one_shot_runner.rs     # One-shot execution
│   ├── operators.rs           # Query operators
│   ├── optimized_search.rs    # Optimized search
│   ├── query_engine.rs        # Query engine
│   ├── rca.rs                 # Root cause analysis
│   ├── root_cause.rs          # Root cause detection
│   ├── rule_compiler.rs       # Rule compilation
│   ├── rule_reasoner.rs       # Rule reasoning
│   ├── safety_guardrails.rs   # Safety guardrails
│   ├── semantic_column_resolver.rs # Semantic column resolution
│   ├── simplified_api.rs      # Simplified API
│   ├── simplified_intent.rs   # Simplified intent
│   ├── sql_compiler.rs        # SQL compilation
│   ├── sql_engine.rs          # SQL engine
│   ├── table_upload.rs        # Table upload
│   ├── task_grounder.rs       # Task grounding
│   ├── time.rs                # Time utilities
│   ├── tool_system.rs         # Tool system
│   ├── validation.rs          # Validation utilities
│   ├── vector_db.py           # Vector DB Python wrapper
│   └── world_state.rs         # World state management
│   │
│   └── [Python helper files]
│       ├── chunking.py
│       ├── config.py
│       ├── confluence_ingest.py
│       ├── confluence_mcp_adapter.py
│       ├── confluence_mcp_integrator.py
│       ├── confluence_slack_mcp_integrator.py
│       ├── confluence_to_knowledge_base.py
│       ├── document_mapper.py
│       ├── ingest.py
│       ├── knowledge_register_sync.py
│       ├── pipeline.py
│       ├── pipeline_api.py
│       ├── product_index.py
│       ├── slack_mcp_adapter.py
│       └── slack_mcp_integrator.py
│
├── components/                # Shared components and modules
│   │
│   ├── Hypergraph/            # Hypergraph implementation
│   │   ├── coarsening.rs      # Graph coarsening
│   │   ├── compression.rs     # Graph compression
│   │   ├── edge.rs            # Edge representation
│   │   ├── graph.rs           # Graph implementation
│   │   ├── mod.rs             # Module definition
│   │   ├── node.rs            # Node representation
│   │   ├── path.rs            # Path finding
│   │   ├── shortest_path.rs  # Shortest path algorithm
│   │   └── types.rs           # Type definitions
│   │
│   ├── KnowledgeBase/         # Knowledge base server
│   │   ├── Cargo.toml         # Rust dependencies
│   │   ├── api_server.rs      # API server
│   │   ├── business_term_resolver.rs # Business term resolution
│   │   ├── concepts.rs        # Concept handling
│   │   ├── contract_extractor.rs # Contract extraction
│   │   ├── mod.rs             # Module definition
│   │   ├── rules.rs           # Rules engine
│   │   ├── semantic_resolver.rs # Semantic resolution
│   │   ├── semantic_sync.rs   # Semantic synchronization
│   │   ├── src/
│   │   │   └── main.rs        # Main entry point
│   │   ├── timezone_rules.json # Timezone rules
│   │   ├── types.rs           # Type definitions
│   │   └── vector_store.rs    # Vector store
│   │
│   ├── WorldState/            # World state management
│   │   ├── Cargo.toml         # Rust dependencies
│   │   ├── aliases.rs         # Alias management
│   │   ├── contract.rs        # Contract handling
│   │   ├── keys.rs            # Key management
│   │   ├── knowledge_integration.rs # Knowledge integration
│   │   ├── lineage.rs         # Lineage tracking
│   │   ├── mod.rs             # Module definition
│   │   ├── policies.rs        # Policy management
│   │   ├── quality.rs         # Quality metrics
│   │   ├── reconciliation.rs  # Reconciliation logic
│   │   ├── rules.rs           # Rules engine
│   │   ├── schema.rs          # Schema management
│   │   ├── source_registry.rs # Source registry
│   │   ├── stats.rs           # Statistics
│   │   └── types.rs           # Type definitions
│   │
│   └── hypergraph-visualizer/ # Visualization component
│       ├── .gitignore
│       ├── backend/           # Backend server
│       │   ├── Cargo.toml
│       │   └── src/
│       │       └── main.rs
│       └── frontend/          # Frontend UI
│           ├── index.html
│           ├── package.json
│           ├── package-lock.json
│           ├── tsconfig.json
│           ├── tsconfig.node.json
│           ├── vite.config.ts
│           └── src/
│               ├── api/
│               │   └── client.ts
│               ├── components/
│               │   └── HypergraphVisualizer.tsx
│               ├── App.tsx
│               ├── main.tsx
│               └── vite-env.d.ts
│
├── config/                    # Configuration files
│   ├── config.yaml            # Main configuration
│   ├── config.yaml.example    # Configuration template
│   ├── document_mapping.yaml  # Document mapping config
│   ├── pipeline_config.yaml   # Pipeline configuration
│   ├── prometheus.yml         # Prometheus configuration
│   ├── rag_config.yaml        # RAG configuration
│   └── trino/                 # Trino configuration
│       ├── catalog/           # Trino catalog configs
│       │   ├── postgres.properties # PostgreSQL catalog
│       │   ├── tpcds.properties    # TPC-DS catalog
│       │   └── tpch.properties     # TPC-H catalog
│       ├── config.properties  # Trino config
│       ├── jvm.config         # JVM configuration
│       ├── node.properties    # Node properties
│       └── README.md          # Trino README
│
├── database/                  # Database schemas and migrations
│   ├── schema.sql             # Main database schema
│   ├── schema_advanced_planner.sql # Advanced planner schema
│   ├── schema_enterprise_safety.sql # Enterprise safety schema
│   └── schema_uploads.sql     # Uploads schema
│
├── docker/                    # Docker configuration
│   ├── docker-compose.dev.yml.example # Dev compose example
│   ├── docker-compose.yml     # Docker Compose configuration
│   ├── Dockerfile             # Backend Dockerfile
│   ├── Dockerfile.frontend    # Frontend Dockerfile
│   └── Dockerfile.rust        # Rust Dockerfile
│
├── docs/                      # Documentation
│   ├── CHANGELOG.md           # Change log
│   ├── CLARIFICATION_API_GUIDE.md # Clarification API guide
│   ├── CLARIFICATION_SUMMARY.md # Clarification summary
│   ├── CONTRIBUTING.md        # Contribution guidelines
│   ├── DATA_ENTRY_GUIDE.md    # Data entry guide
│   ├── DOCKER.md              # Docker documentation
│   ├── END_TO_END_PIPELINE.md # End-to-end pipeline guide
│   ├── ENTERPRISE_SAFETY_IMPLEMENTATION.md # Enterprise safety docs
│   ├── EXECUTION_PLAN.md      # Execution plan
│   ├── FEEDBACK_ASSESSMENT_AND_PLAN.md # Feedback assessment
│   ├── IMPLEMENTATION_COMPLETE.md # Implementation status
│   ├── IMPLEMENTATION_STATUS.md # Implementation status
│   ├── IMPLEMENTATION_SUMMARY.md # Implementation summary
│   ├── INTEGRATION_GUIDE.md    # Integration guide
│   ├── INTEGRATION_STATUS.md  # Integration status
│   ├── PERMISSIVE_MODE.md     # Permissive mode documentation
│   ├── PRODUCTION_FEATURES_CHECKLIST.md # Production checklist
│   ├── PRODUCTION_READINESS.md # Production readiness guide
│   ├── PROJECT_STRUCTURE.md   # This file
│   ├── README.md              # Documentation index
│   ├── REORGANIZATION_SUMMARY.md # Reorganization summary
│   ├── SETUP.md               # Setup instructions
│   ├── SHIP_READY_CHECKLIST.md # Ship-ready checklist
│   └── VERIFICATION_REPORT.md # Verification report
│
├── infrastructure/            # Infrastructure as code
│   └── airflow/               # Apache Airflow DAGs and configs
│       ├── dags/              # Airflow DAG definitions
│       │   └── document_retrieval_dag.py # Document retrieval DAG
│       └── plugins/           # Airflow plugins
│           └── .gitkeep
│
├── scripts/                   # Utility scripts
│   ├── fix_vendor_checksums.py # Fix vendor checksums
│   └── load_metadata.sh       # Load metadata script
│
├── tests/                     # Test suite
│   ├── __init__.py
│   ├── test_auth_integration.py # Auth integration tests
│   ├── test_clarification_agent.py # Clarification agent tests
│   ├── test_contract_store.py # Contract store tests
│   ├── test_observability.py  # Observability tests
│   └── test_table_store.py    # Table store tests
│
├── data/                      # Data files (gitignored)
│   └── [Various CSV, Parquet, JSON files]
│
├── vendor/                    # Vendored dependencies (Rust)
│   └── [Vendored Rust crates]
│
├── .cargo/                    # Cargo configuration
│   └── config.toml            # Cargo config
│
├── .structure                 # Structure marker file
├── .tool-versions             # Tool versions file
├── .env.example               # Environment variables template
├── .gitignore                 # Git ignore rules
├── Cargo.toml                 # Rust dependencies
├── Cargo.lock                 # Rust dependency lock file
├── requirements.txt           # Python dependencies
├── pyproject.toml             # Python project configuration
├── package.json               # Node.js project configuration (if exists)
└── README.md                  # Project README

```

## Detailed Directory Descriptions

### Backend (`backend/`)

Python Flask application containing the main backend logic:

- **API Layer** (`api/`): REST API endpoints for query execution, clarification, health checks, metrics, and metadata management
- **Agentic System** (`agentic/`): Multi-agent system for query decomposition and execution
- **Authentication** (`auth/`): Authentication, authorization, and rate limiting
- **Configuration** (`config/`): Configuration management
- **Deployment** (`deployment/`): Feature flags and shadow mode for gradual rollouts
- **Execution** (`execution/`): Query execution engines with security and sandboxing
- **Failure Handling** (`failure_handling/`): Strategies for handling LLM failures, metadata drift, and partial results
- **Interfaces & Implementations**: Abstract interfaces and concrete implementations for caching, database execution, and LLM providers
- **Invariants** (`invariants/`): System invariants enforcement (determinism, reproducibility, fail-open/closed)
- **Models** (`models/`): Data models for table state and other entities
- **Observability** (`observability/`): Structured logging, metrics, golden signals, and correlation IDs
- **Planes** (`planes/`): Four-plane architecture (Ingress, Planning, Execution, Presentation)
- **Planning** (`planning/`): Query planning, intent extraction, clarification, schema selection, and validation
- **Presentation** (`presentation/`): Result formatting and query explanation
- **RAG** (`rag/`): Retrieval-augmented generation for knowledge retrieval
- **Security** (`security/`): Security features including prompt injection and data exfiltration prevention
- **Services** (`services/`): Business logic services for drift detection and query resolution
- **Stores** (`stores/`): Data stores for contracts, tables, and database connections

### Frontend (`frontend/`)

React/TypeScript application providing the user interface:

- **Components** (`src/components/`): React components for query building, visualization, monitoring, and management
- **API Client** (`src/api/`): API client for backend communication
- **State Management** (`src/store/`): Zustand store for application state
- **Hypergraph Visualizer** (`hypergraph-visualizer/`): Standalone hypergraph visualization component with its own backend and frontend

### Rust Core (`rust/`)

High-performance Rust library providing core functionality:

- **Agent System** (`agent/`): Agent framework with tools, memory, planning, and execution
- **API Layer** (`api/`): User-facing API
- **Binaries** (`bin/`): Command-line tools for rule building, data conversion, metadata migration, and server execution
- **Compiler** (`compiler/`): Query compiler with join planning, resolution, and validation
- **Core** (`core/`): Core functionality including:
  - **Agent**: RCA cursor implementation with attribution, confidence, and validation
  - **Engine**: Query engine with canonicalization, materialization, and storage
  - **Identity**: Identity graph management
  - **Lineage**: Data lineage tracking for filters, joins, and rules
  - **LLM**: LLM integration strategies
  - **Metrics**: Metric normalization
  - **Models**: Canonical model representations
  - **Observability**: Execution tracing and trace storage
  - **Performance**: Optimizations including parallel execution, caching, pushdown, and sampling
  - **RCA**: Root cause analysis with attribution, dimension aggregation, and narrative generation
  - **Safety**: Failure recovery and resource limits
  - **Trust**: Evidence collection, replay, and verification
- **Database Layer** (`db/`): Database connections, metadata repository, and query history
- **Execution** (`execution/`): Multiple execution engines (DuckDB, Trino, Polars) with routing and profiling
- **Execution Loop** (`execution_loop/`): Execution loop with error classification and recovery
- **Ingestion** (`ingestion/`): Data ingestion with connectors (CSV, JSON), schema inference, and join inference
- **Intent** (`intent/`): Intent processing with function schemas and semantic intent
- **Learning** (`learning/`): Failure analysis and learning
- **Observability** (`observability/`): Execution logging, metrics, and structured logging
- **Python Bindings** (`python_bindings/`): Python bindings for agent decisions, execution, and profiling
- **Schema RAG** (`schema_rag/`): Schema retrieval-augmented generation with embedding and vector storage
- **Security** (`security/`): Access control, policies, query guards, and user management
- **Semantic** (`semantic/`): Semantic analysis for dimensions, metrics, join graphs, and registries
- **Semantic Completeness** (`semantic_completeness/`): Entity extraction, mapping, SQL validation, and regeneration loops

### Components (`components/`)

Shared components used across the application:

- **Hypergraph** (`Hypergraph/`): Hypergraph data structure with coarsening, compression, path finding, and shortest path algorithms
- **Knowledge Base** (`KnowledgeBase/`): Knowledge base server with business term resolution, semantic resolution, and vector storage
- **World State** (`WorldState/`): World state management with contracts, policies, quality metrics, reconciliation, and lineage
- **Hypergraph Visualizer** (`hypergraph-visualizer/`): Visualization component with backend server and frontend UI

### Configuration (`config/`)

Application configuration files:

- **Main Config** (`config.yaml`): Main application configuration
- **Pipeline Config** (`pipeline_config.yaml`): Pipeline configuration
- **RAG Config** (`rag_config.yaml`): RAG configuration
- **Document Mapping** (`document_mapping.yaml`): Document mapping configuration
- **Prometheus** (`prometheus.yml`): Prometheus monitoring configuration
- **Trino** (`trino/`): Trino query engine configuration with catalog definitions for PostgreSQL, TPC-DS, and TPC-H

### Database (`database/`)

Database schemas and migrations:

- **Main Schema** (`schema.sql`): Main database schema
- **Advanced Planner** (`schema_advanced_planner.sql`): Schema for advanced planner features
- **Enterprise Safety** (`schema_enterprise_safety.sql`): Schema for enterprise safety features
- **Uploads** (`schema_uploads.sql`): Schema for file uploads

### Docker (`docker/`)

Docker configuration files:

- **Docker Compose** (`docker-compose.yml`): Main Docker Compose configuration
- **Dev Compose** (`docker-compose.dev.yml.example`): Development Docker Compose example
- **Dockerfiles**: Separate Dockerfiles for backend, frontend, and Rust components

### Documentation (`docs/`)

Comprehensive documentation including:

- Setup and installation guides
- API documentation
- Production readiness checklists
- Implementation status and summaries
- Integration guides
- Feature documentation

### Infrastructure (`infrastructure/`)

Infrastructure as code:

- **Airflow** (`airflow/`): Apache Airflow DAGs for data pipelines and document retrieval

### Scripts (`scripts/`)

Utility scripts for:

- Fixing vendor checksums
- Loading metadata

### Tests (`tests/`)

Test suite including:

- Authentication integration tests
- Clarification agent tests
- Contract store tests
- Observability tests
- Table store tests

## File Naming Conventions

- **Python files**: `snake_case.py`
- **Rust files**: `snake_case.rs`
- **TypeScript files**: `PascalCase.tsx` (components), `camelCase.ts` (utilities)
- **Config files**: `kebab-case.yaml` or `snake_case.yaml`
- **Documentation**: `UPPER_SNAKE_CASE.md` or `PascalCase.md`

## Build Artifacts

The following directories are gitignored and contain build artifacts:

- `target/` - Rust build output
- `node_modules/` - Node.js dependencies
- `venv/` - Python virtual environment
- `dist/` - Frontend build output
- `logs/` - Application logs
- `data/` - Data files
- `tables/` - Table definitions
- `vendor/` - Vendored Rust dependencies (may be partially tracked)

## Development Workflow

1. **Backend Development**: Work in `backend/`
2. **Frontend Development**: Work in `frontend/`
3. **Rust Core Development**: Work in `rust/`
4. **Running Tests**: Use `tests/` directory
5. **Docker Development**: Use `docker/docker-compose.yml`
6. **Documentation**: Add to `docs/`

## Key Architectural Patterns

### Four-Plane Architecture

The system follows a four-plane architecture:

1. **Ingress Plane**: Request validation, authentication, rate limiting
2. **Planning Plane**: Intent extraction, SQL generation, clarification
3. **Execution Plane**: Query execution, engine selection
4. **Presentation Plane**: Result formatting, explanation generation

### Agentic System

Multi-agent system for query decomposition:
- **Intent Agent**: Extracts query intent
- **Table Agent**: Selects relevant tables
- **Metric Agent**: Resolves metrics
- **Filter Agent**: Applies filters
- **Shape Agent**: Determines query shape
- **Verifier Agent**: Validates queries
- **SQL Renderer**: Generates SQL

### Invariants

System invariants enforced throughout:
- **Determinism**: Queries produce consistent results
- **Reproducibility**: Queries can be replayed
- **Fail-Closed**: Security failures default to deny
- **Fail-Open**: Operational failures allow graceful degradation
- **Boundary Enforcement**: System boundaries are enforced

## Integration Points

- **Python-Rust Bridge**: Python bindings in `rust/python_bindings/`
- **Frontend-Backend**: REST API in `backend/api/`
- **Knowledge Base**: Separate service in `components/KnowledgeBase/`
- **World State**: Shared state management in `components/WorldState/`
- **Hypergraph**: Graph structure in `components/Hypergraph/`
