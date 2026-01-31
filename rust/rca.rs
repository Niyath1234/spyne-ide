use crate::ambiguity::{AmbiguityResolver, ResolvedInterpretation};
use crate::data_utils;
use crate::diff::{ComparisonResult, DiffEngine};
use crate::drilldown::{DivergencePoint, DrilldownEngine};
use crate::error::{RcaError, Result};
use crate::grain_resolver::GrainResolver;
use crate::graph::Hypergraph;
use crate::identity::IdentityResolver;
use crate::llm::{LlmClient, QueryInterpretation};
use crate::metadata::Metadata;
use crate::rule_compiler::{RuleCompiler, RuleExecutor};
use crate::time::TimeResolver;
use crate::tool_system::{ToolSystem, ExecutionContext, ToolExecutionContext};
use crate::de_executor::DeExecutor;
use chrono::NaiveDate;
use polars::prelude::*;
use std::path::PathBuf;
use tracing::{info, debug, trace};

pub struct RcaEngine {
    metadata: Metadata,
    llm: LlmClient,
    data_dir: PathBuf,
    tool_system: ToolSystem,
}

impl RcaEngine {
    pub fn new(metadata: Metadata, llm: LlmClient, data_dir: PathBuf) -> Self {
        let tool_system = ToolSystem::new(llm.clone());
        Self {
            metadata,
            llm,
            data_dir,
            tool_system,
        }
    }
    
    pub async fn run(&self, query: &str) -> Result<RcaResult> {
        println!("\n{}", "=".repeat(80));
        println!("🔍 RCA ENGINE: CHAIN OF THOUGHT EXECUTION");
        println!("{}\n", "=".repeat(80));
        
        println!("📥 INPUT QUERY: \"{}\"", query);
        println!("\n{}\n", "-".repeat(80));
        
        // Step 1: LLM interprets query
        println!("🤖 STEP 1: LLM QUERY INTERPRETATION");
        println!("   Knowledge Base Context:");
        println!("   - Available Systems: {:?}", 
            self.metadata.business_labels.systems.iter()
                .map(|s| format!("{} ({})", s.label, s.system_id))
                .collect::<Vec<_>>());
        println!("   - Available Metrics: {:?}", 
            self.metadata.business_labels.metrics.iter()
                .map(|m| format!("{} ({})", m.label, m.metric_id))
                .collect::<Vec<_>>());
        println!("   - Total Entities in Knowledge Base: {}", self.metadata.entities.len());
        println!("   - Total Tables in Knowledge Base: {}", self.metadata.tables.len());
        println!("   - Total Rules in Knowledge Base: {}", self.metadata.rules.len());
        println!("   - Total Lineage Edges: {}", self.metadata.lineage.edges.len());
        
        let interpretation = self.llm.interpret_query(
            query,
            &self.metadata.business_labels,
            &self.metadata.metrics,
        ).await?;
        
        println!("   ✅ LLM Interpretation:");
        println!("      - System A: {} (confidence: {:.2}%)", interpretation.system_a, interpretation.confidence * 100.0);
        println!("      - System B: {} (confidence: {:.2}%)", interpretation.system_b, interpretation.confidence * 100.0);
        println!("      - Metric: {}", interpretation.metric);
        if let Some(ref date) = interpretation.as_of_date {
            println!("      - As-of Date: {}", date);
        }
        println!("\n{}\n", "-".repeat(80));
        
        // Step 2: Resolve ambiguities (max 3 questions)
        println!("🔍 STEP 2: AMBIGUITY RESOLUTION");
        let ambiguity_resolver = AmbiguityResolver::new(self.metadata.clone());
        let resolved = ambiguity_resolver.resolve(&interpretation, None)?;
        println!("   ✅ Ambiguity Resolution Complete");
        println!("      - Final System A: {}", resolved.system_a);
        println!("      - Final System B: {}", resolved.system_b);
        if let Some(ref metric) = resolved.metric {
            println!("      - Final Metric: {}", metric);
        }
        if resolved.is_cross_metric {
            if let Some(ref metric_a) = resolved.metric_a {
                println!("      - Metric A: {}", metric_a);
            }
            if let Some(ref metric_b) = resolved.metric_b {
                println!("      - Metric B: {}", metric_b);
            }
        }
        println!("\n{}\n", "-".repeat(80));
        
        // Step 3: Resolve rules and subgraph
        println!("🕸️  STEP 3: HYPERGRAPH TRAVERSAL & SUBGRAPH EXTRACTION");
        println!("   Analyzing knowledge graph to find relevant nodes...");
        println!("   - Total nodes (tables) in graph: {}", self.metadata.tables.len());
        
        let graph = Hypergraph::new(self.metadata.clone());
        let metric_name = if resolved.is_cross_metric {
            resolved.metric_a.as_ref().or(resolved.metric_b.as_ref()).map(|s| s.as_str()).unwrap_or("")
        } else {
            resolved.metric.as_ref().map(|s| s.as_str()).unwrap_or("")
        };
        let subgraph = graph.get_reconciliation_subgraph(
            &resolved.system_a,
            &resolved.system_b,
            metric_name,
        )?;
        
        println!("   ✅ Subgraph Extracted:");
        println!("      - System A Tables: {} ({:?})", subgraph.tables_a.len(), subgraph.tables_a);
        println!("      - System B Tables: {} ({:?})", subgraph.tables_b.len(), subgraph.tables_b);
        println!("      - System A Rules: {} ({:?})", subgraph.rules_a.len(), subgraph.rules_a);
        println!("      - System B Rules: {} ({:?})", subgraph.rules_b.len(), subgraph.rules_b);
        
        // Show which nodes participate vs don't
        let all_tables: Vec<String> = self.metadata.tables.iter().map(|t| t.name.clone()).collect();
        let participating_tables: std::collections::HashSet<String> = 
            subgraph.tables_a.iter().chain(subgraph.tables_b.iter()).cloned().collect();
        let non_participating: Vec<String> = all_tables.iter()
            .filter(|t| !participating_tables.contains(*t))
            .cloned()
            .collect();
        
        println!("   📊 Graph Node Analysis:");
        println!("      - Participating Nodes: {} ({:?})", participating_tables.len(), 
            participating_tables.iter().collect::<Vec<_>>());
        println!("      - Non-Participating Nodes: {} ({:?})", non_participating.len(), non_participating);
        println!("\n{}\n", "-".repeat(80));
        
        // Step 4: Get rules (use resolved rule IDs or first available)
        println!("📋 STEP 4: RULE SELECTION");
        let rule_a_id = resolved.rule_a
            .unwrap_or_else(|| subgraph.rules_a[0].clone());
        let rule_b_id = resolved.rule_b
            .unwrap_or_else(|| subgraph.rules_b[0].clone());
        
        let rule_a = self.metadata.get_rule(&rule_a_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", rule_a_id)))?;
        let rule_b = self.metadata.get_rule(&rule_b_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", rule_b_id)))?;
        
        println!("   ✅ Selected Rules:");
        println!("      - System A Rule: {} ({})", rule_a_id, rule_a.computation.description);
        println!("        * Source Entities: {:?}", rule_a.computation.source_entities);
        println!("        * Formula: {}", rule_a.computation.formula);
        println!("        * Target Grain: {:?}", rule_a.target_grain);
        println!("      - System B Rule: {} ({})", rule_b_id, rule_b.computation.description);
        println!("        * Source Entities: {:?}", rule_b.computation.source_entities);
        println!("        * Formula: {}", rule_b.computation.formula);
        println!("        * Target Grain: {:?}", rule_b.target_grain);
        println!("\n{}\n", "-".repeat(80));
        
        // Step 5: Get metric metadata
        println!("📏 STEP 5: METRIC METADATA");
        let metric_name = resolved.metric.as_ref().ok_or_else(|| RcaError::Execution("Metric not found in resolved interpretation".to_string()))?;
        let metric = self.metadata
            .get_metric(metric_name)
            .ok_or_else(|| RcaError::Execution(format!("Metric not found: {}", metric_name)))?;
        println!("   ✅ Metric: {} ({})", metric.name, metric.id);
        println!("      - Grain: {:?}", metric.grain);
        println!("      - Precision: {}", metric.precision);
        println!("      - Null Policy: {}", metric.null_policy);
        println!("\n{}\n", "-".repeat(80));
        
        // Step 6: Parse as-of date
        println!("📅 STEP 6: TIME RESOLUTION");
        let as_of_date = resolved.as_of_date
            .and_then(|d| NaiveDate::parse_from_str(&d, "%Y-%m-%d").ok());
        if let Some(date) = as_of_date {
            println!("   ✅ As-of Date: {}", date);
        } else {
            println!("   ℹ️  No as-of date specified (using latest data)");
        }
        println!("\n{}\n", "-".repeat(80));
        
        // Step 7: Execute both pipelines
        println!("⚙️  STEP 7: PIPELINE EXECUTION");
        println!("   Compiling execution plans from rules...");
        
        let compiler = RuleCompiler::new(self.metadata.clone(), self.data_dir.clone());
        let compiler_clone = RuleCompiler::new(self.metadata.clone(), self.data_dir.clone());
        let executor = RuleExecutor::new(compiler_clone);
        
        println!("   🔄 Executing System A Pipeline (Rule: {})...", rule_a_id);
        println!("      Graph Traversal Path:");
        let compiler = RuleCompiler::new(self.metadata.clone(), self.data_dir.clone());
        let plan_a = compiler.compile(&rule_a_id)?;
        for (idx, step) in plan_a.steps.iter().enumerate() {
            match step {
                crate::metadata::PipelineOp::Scan { table } => {
                    println!("         Step {}: 📖 SCAN table '{}'", idx + 1, table);
                }
                crate::metadata::PipelineOp::Join { table, on, join_type } => {
                    println!("         Step {}: 🔗 JOIN table '{}' ON {:?} (type: {})", idx + 1, table, on, join_type);
                }
                crate::metadata::PipelineOp::Derive { expr, r#as } => {
                    println!("         Step {}: 🧮 DERIVE {} AS {}", idx + 1, expr, r#as);
                }
                crate::metadata::PipelineOp::Group { by, agg } => {
                    println!("         Step {}: 📊 GROUP BY {:?} AGGREGATE {:?}", idx + 1, by, agg);
                }
                crate::metadata::PipelineOp::Select { columns } => {
                    println!("         Step {}: ✅ SELECT {:?}", idx + 1, columns);
                }
                _ => {
                    println!("         Step {}: {:?}", idx + 1, step);
                }
            }
        }
        
        let df_a = executor.execute(&rule_a_id, as_of_date).await?;
        println!("      ✅ System A Result: {} rows, {} columns", df_a.height(), df_a.width());
        
        println!("   🔄 Executing System B Pipeline (Rule: {})...", rule_b_id);
        let plan_b = compiler.compile(&rule_b_id)?;
        for (idx, step) in plan_b.steps.iter().enumerate() {
            match step {
                crate::metadata::PipelineOp::Scan { table } => {
                    println!("         Step {}: 📖 SCAN table '{}'", idx + 1, table);
                }
                crate::metadata::PipelineOp::Join { table, on, join_type } => {
                    println!("         Step {}: 🔗 JOIN table '{}' ON {:?} (type: {})", idx + 1, table, on, join_type);
                }
                crate::metadata::PipelineOp::Derive { expr, r#as } => {
                    println!("         Step {}: 🧮 DERIVE {} AS {}", idx + 1, expr, r#as);
                }
                crate::metadata::PipelineOp::Group { by, agg } => {
                    println!("         Step {}: 📊 GROUP BY {:?} AGGREGATE {:?}", idx + 1, by, agg);
                }
                crate::metadata::PipelineOp::Select { columns } => {
                    println!("         Step {}: ✅ SELECT {:?}", idx + 1, columns);
                }
                _ => {
                    println!("         Step {}: {:?}", idx + 1, step);
                }
            }
        }
        
        let df_b = executor.execute(&rule_b_id, as_of_date).await?;
        println!("      ✅ System B Result: {} rows, {} columns", df_b.height(), df_b.width());
        println!("\n{}\n", "-".repeat(80));
        
        // Step 8: Intelligent Grain Normalization
        println!("🔑 STEP 8: INTELLIGENT GRAIN NORMALIZATION");
        let identity_resolver = IdentityResolver::new(self.metadata.clone(), self.data_dir.clone());
        let grain_resolver = GrainResolver::new(self.metadata.clone());
        
        let grain_a = graph.get_rule_grain(&rule_a_id)?;
        let grain_b = graph.get_rule_grain(&rule_b_id)?;
        
        println!("   - System A Grain: {:?}", grain_a);
        println!("   - System B Grain: {:?}", grain_b);
        
        // Find the target entity table for each system
        let table_a = self.metadata.tables
            .iter()
            .find(|t| t.entity == rule_a.target_entity && t.system == rule_a.system)
            .ok_or_else(|| RcaError::Execution(format!("No table found for target entity {} in system {}", rule_a.target_entity, rule_a.system)))?;
        
        let table_b = self.metadata.tables
            .iter()
            .find(|t| t.entity == rule_b.target_entity && t.system == rule_b.system)
            .ok_or_else(|| RcaError::Execution(format!("No table found for target entity {} in system {}", rule_b.target_entity, rule_b.system)))?;
        
        // Find common grain using intelligent resolver
        let common_grain = if grain_a == grain_b {
            println!("   ✅ Grains match - no normalization needed");
            grain_a.clone()
        } else {
            println!("   🔍 Grains differ - finding common grain...");
            match grain_resolver.find_common_grain(
                &rule_a.system,
                &grain_a,
                &rule_b.system,
                &grain_b,
                &table_a.name,
                &table_b.name,
            ) {
                Ok(common) => {
                    println!("   ✅ Found common grain: {:?}", common);
                    common
                }
                Err(e) => {
                    println!("   ⚠️  Could not find common grain automatically: {}", e);
                    // Try to use metric grain only if both systems can resolve to it
                    let can_a_resolve_to_metric = grain_resolver.can_resolve_to_grain(
                        &rule_a.system, &grain_a, &metric.grain, &table_a.name
                    ).unwrap_or(false);
                    let can_b_resolve_to_metric = grain_resolver.can_resolve_to_grain(
                        &rule_b.system, &grain_b, &metric.grain, &table_b.name
                    ).unwrap_or(false);
                    
                    if can_a_resolve_to_metric && can_b_resolve_to_metric {
                        println!("   ℹ️  Falling back to metric grain: {:?}", metric.grain);
                        metric.grain.clone()
                    } else {
                        return Err(RcaError::Execution(format!(
                            "Cannot find common grain between {:?} and {:?}, and metric grain {:?} is not compatible with both systems",
                            grain_a, grain_b, metric.grain
                        )));
                    }
                }
            }
        };
        
        // Resolve grain mismatches for each system
        println!("\n   📊 Resolving System A grain...");
        let mut df_a_normalized = df_a.clone();
        if grain_a != common_grain {
            if let Ok(Some(plan_a)) = grain_resolver.resolve_grain_mismatch(
                &rule_a.system,
                &grain_a,
                &common_grain,
                &table_a.name,
            ) {
                println!("   ✅ Resolution plan for System A: {}", plan_a.description);
                // Apply grain resolution
                df_a_normalized = grain_resolver.apply_grain_resolution(
                    df_a_normalized,
                    &plan_a,
                    &metric.id,
                    &table_a.name,
                    &self.data_dir,
                ).await?;
            } else {
                // Fallback to identity resolver
                df_a_normalized = identity_resolver.normalize_keys(df_a_normalized, &table_a.name, &common_grain).await?;
            }
        } else {
            df_a_normalized = identity_resolver.normalize_keys(df_a_normalized, &table_a.name, &common_grain).await?;
        }
        
        println!("\n   📊 Resolving System B grain...");
        let mut df_b_normalized = df_b.clone();
        if grain_b != common_grain {
            if let Ok(Some(plan_b)) = grain_resolver.resolve_grain_mismatch(
                &rule_b.system,
                &grain_b,
                &common_grain,
                &table_b.name,
            ) {
                println!("   ✅ Resolution plan for System B: {}", plan_b.description);
                // Apply grain resolution
                df_b_normalized = grain_resolver.apply_grain_resolution(
                    df_b_normalized,
                    &plan_b,
                    &metric.id,
                    &table_b.name,
                    &self.data_dir,
                ).await?;
            } else {
                // Fallback to identity resolver
                df_b_normalized = identity_resolver.normalize_keys(df_b_normalized, &table_b.name, &common_grain).await?;
            }
        } else {
            df_b_normalized = identity_resolver.normalize_keys(df_b_normalized, &table_b.name, &common_grain).await?;
        }
        
        println!("   ✅ Grain Normalization Complete");
        println!("\n{}\n", "-".repeat(80));
        
        // Step 9: Apply time logic
        println!("⏰ STEP 9: TEMPORAL ANALYSIS");
        let time_resolver = TimeResolver::new(self.metadata.clone());
        let temporal_misalignment = time_resolver.detect_temporal_misalignment(
            &df_a_normalized,
            &df_b_normalized,
            &subgraph.tables_a[0],
            &subgraph.tables_b[0],
        )?;
        if let Some(ref misalignment) = temporal_misalignment {
            println!("   ⚠️  Temporal Misalignment Detected:");
            let description = format!(
                "Table {} date range: {:?} to {:?}, Table {} date range: {:?} to {:?}",
                misalignment.table_a,
                misalignment.min_a,
                misalignment.max_a,
                misalignment.table_b,
                misalignment.min_b,
                misalignment.max_b
            );
            println!("      - {}", description);
        } else {
            println!("   ✅ No temporal misalignment detected");
        }
        println!("\n{}\n", "-".repeat(80));
        
        // Step 9.5: LLM Tool Selection (NEW)
        println!("🔧 STEP 9.5: LLM TOOL SELECTION");
        let execution_context = ExecutionContext {
            system_a: resolved.system_a.clone(),
            system_b: resolved.system_b.clone(),
            metric: resolved.metric.clone().unwrap_or_default(),
            grain_columns: common_grain.clone(),
            available_tables: subgraph.tables_a.iter().chain(subgraph.tables_b.iter())
                .map(|t| t.clone())
                .collect(),
        };
        
        let tool_plan = self.tool_system.plan_execution(query, &execution_context).await?;
        
        // Execute tool plan
        let mut tool_context = ToolExecutionContext::default();
        for tool_call in &tool_plan.steps {
            let _result = self.tool_system.execute_tool(tool_call, &mut tool_context).await?;
        }
        
        println!("\n{}\n", "-".repeat(80));
        
        // Step 10: Compare results
        println!("🔍 STEP 10: COMPARISON & DIFF ANALYSIS");
        
        // Enrich dataframes with entities from all tables in the system that have the target grain
        // This ensures we detect entities that exist in other tables (e.g., L999 in system_c_extra_loans)
        let df_a_enriched = self.enrich_with_system_entities(
            df_a_normalized.clone(),
            &rule_a.system,
            &common_grain,
        ).await?;
        let df_b_enriched = self.enrich_with_system_entities(
            df_b_normalized.clone(),
            &rule_b.system,
            &common_grain,
        ).await?;
        
        // Execute DE tools before comparison if requested
        let metric_name = resolved.metric.as_ref().ok_or_else(|| RcaError::Execution("Metric not found in resolved interpretation".to_string()))?;
        self.execute_de_tools_before_comparison(
            &df_a_enriched,
            &df_b_enriched,
            &rule_a.system,
            &rule_b.system,
            &common_grain,
            metric_name,
            &tool_context,
        )?;
        
        // Use tool context to configure diff engine (LLM may have selected fuzzy matching)
        let diff_engine = if !tool_context.fuzzy_columns.is_empty() {
            println!("   🔍 Using fuzzy matching (selected by LLM) for columns: {:?}", tool_context.fuzzy_columns);
            DiffEngine::new().with_fuzzy_matching(
                tool_context.fuzzy_threshold,
                tool_context.fuzzy_columns.clone()
            )
        } else {
            // Fallback to automatic detection if LLM didn't select fuzzy matching
            let fuzzy_columns: Vec<String> = common_grain.iter()
                .filter(|col| {
                    let col_lower = col.to_lowercase();
                    col_lower.contains("name") || 
                    col_lower.contains("customer") || 
                    col_lower.contains("entity") ||
                    col_lower.contains("description")
                })
                .cloned()
                .collect();
            
            if !fuzzy_columns.is_empty() {
                println!("   🔍 Auto-detected string columns for fuzzy matching: {:?}", fuzzy_columns);
                DiffEngine::new().with_fuzzy_matching(0.85, fuzzy_columns)
            } else {
                DiffEngine::new()
            }
        };
        
        let comparison = diff_engine.compare(
            df_a_enriched,
            df_b_enriched,
            &common_grain,
            metric_name,
            metric.precision,
        ).await?;
        println!("   ✅ Comparison Complete:");
        println!("      - Population Match: {} common entities", comparison.population_diff.common_count);
        println!("      - Missing in B: {} entities", comparison.population_diff.missing_in_b.len());
        println!("      - Extra in B: {} entities", comparison.population_diff.extra_in_b.len());
        println!("      - Data Matches: {} entities", comparison.data_diff.matches);
        println!("      - Data Mismatches: {} entities", comparison.data_diff.mismatches);
        println!("\n{}\n", "-".repeat(80));
        
        // Step 11: Classify mismatches
        println!("🏷️  STEP 11: ROOT CAUSE CLASSIFICATION");
        let classifications = self.classify_mismatches(&comparison, temporal_misalignment.as_ref(), rule_a, rule_b)?;
        println!("   ✅ Classifications:");
        for (idx, classification) in classifications.iter().enumerate() {
            println!("      {}. {} - {}: {} (count: {})", 
                idx + 1, 
                classification.root_cause, 
                classification.subtype,
                classification.description,
                classification.count);
        }
        println!("\n{}\n", "-".repeat(80));
        
        // Step 12: Drill-down for mismatched keys
        println!("🔬 STEP 12: DRILL-DOWN ANALYSIS");
        let mismatched_keys: Vec<Vec<String>> = if comparison.data_diff.mismatches > 0 {
            println!("   Analyzing {} mismatched entities...", comparison.data_diff.mismatches);
            let mut keys = Vec::new();
            let mismatch_df = &comparison.data_diff.mismatch_details;
            let limit = mismatch_df.height().min(100);
            for row_idx in 0..limit {
                let mut key = Vec::new();
                for col_name in &common_grain {
                    if let Ok(col_val) = mismatch_df.column(col_name) {
                        let val_str: String = match col_val.dtype() {
                            &DataType::String => {
                                col_val.str().unwrap().get(row_idx).unwrap_or("").to_string()
                            }
                            &DataType::Int64 => {
                                col_val.i64().unwrap().get(row_idx).unwrap_or(0).to_string()
                            }
                            &DataType::Float64 => {
                                col_val.f64().unwrap().get(row_idx).unwrap_or(0.0).to_string()
                            }
                            _ => format!("{:?}", col_val.get(row_idx)),
                        };
                        key.push(val_str);
                    }
                }
                if !key.is_empty() {
                    keys.push(key);
                }
            }
            println!("   - Selected {} keys for drill-down analysis", keys.len());
            keys
        } else {
            println!("   ℹ️  No mismatches found, skipping drill-down");
            Vec::new()
        };
        
        let divergence = if !mismatched_keys.is_empty() {
            println!("   🔍 Finding divergence points...");
            let mut drilldown = DrilldownEngine::new(executor)
                .with_llm(self.llm.clone())
                .with_metadata(self.metadata.clone())
                .with_data_dir(self.data_dir.clone());
            
            let mut div = drilldown.find_divergence(&rule_a_id, &rule_b_id, &mismatched_keys, as_of_date).await?;
            println!("   ✅ Divergence Found:");
            println!("      - Step Index: {}", div.step_index);
            println!("      - Divergence Type: {}", div.divergence_type);
            
            // Step 12.5: Analyze root causes with LLM to identify specific issues
            println!("\n   🤖 Analyzing root causes with LLM...");
            match drilldown.analyze_root_causes(&rule_a_id, &rule_b_id, &mismatched_keys, as_of_date).await {
                Ok(root_causes) => {
                    if !root_causes.is_empty() {
                        println!("   ✅ Root Cause Analysis Complete:");
                        for (idx, root_cause) in root_causes.iter().enumerate() {
                            println!("      {}. Loan {}: Difference of {:.2}", 
                                idx + 1, root_cause.loan_id, root_cause.difference);
                            println!("         System A: {:.2} | System B: {:.2}", 
                                root_cause.system_a_value, root_cause.system_b_value);
                            
                            if !root_cause.specific_issues.is_empty() {
                                println!("         🔍 Specific Issues Identified:");
                                for issue in &root_cause.specific_issues {
                                    println!("            • {}", issue);
                                }
                            }
                        }
                        div.root_cause_details = Some(root_causes);
                    } else {
                        println!("   ℹ️  No specific root causes identified");
                    }
                }
                Err(e) => {
                    println!("   ⚠️  Root cause analysis failed: {}", e);
                    println!("   ℹ️  Continuing with divergence point only");
                }
            }
            
            Some(div)
        } else {
            None
        };
        println!("\n{}\n", "-".repeat(80));
        
        // Step 13: Generate explanation
        println!("📊 STEP 13: FINAL RESULT SUMMARY");
        let result = RcaResult {
            query: query.to_string(),
            system_a: resolved.system_a.clone(),
            system_b: resolved.system_b.clone(),
            metric: resolved.metric.clone().unwrap_or_default(),
            as_of_date,
            comparison,
            classifications,
            divergence,
            temporal_misalignment,
        };
        
        println!("{}", "=".repeat(80));
        println!("✅ RCA EXECUTION COMPLETE");
        println!("{}\n", "=".repeat(80));
        
        Ok(result)
    }
    
    /// Execute DE tools before comparison based on tool context
    fn execute_de_tools_before_comparison(
        &self,
        df_a: &DataFrame,
        df_b: &DataFrame,
        system_a: &str,
        system_b: &str,
        grain: &[String],
        metric: &str,
        tool_context: &ToolExecutionContext,
    ) -> Result<()> {
        // Execute inspection requests
        for inspection in &tool_context.inspection_requests {
            if inspection.table == format!("{}_result", system_a) || inspection.table.contains(system_a) {
                let _ = DeExecutor::execute_inspect(df_a, system_a, &inspection.columns, inspection.top_n);
            }
            if inspection.table == format!("{}_result", system_b) || inspection.table.contains(system_b) {
                let _ = DeExecutor::execute_inspect(df_b, system_b, &inspection.columns, inspection.top_n);
            }
        }
        
        // Execute schema validation
        for validation in &tool_context.schema_validations {
            let _ = DeExecutor::execute_validate_schema(
                df_a,
                df_b,
                &validation.left_table,
                &validation.right_table,
                &validation.join_columns,
            );
        }
        
        // Execute anomaly detection
        for detection in &tool_context.anomaly_detections {
            if detection.table.contains(system_a) {
                let _ = DeExecutor::execute_detect_anomalies(
                    df_a,
                    system_a,
                    detection.columns.as_ref().map(|v| v.as_slice()),
                    &detection.checks,
                );
            }
            if detection.table.contains(system_b) {
                let _ = DeExecutor::execute_detect_anomalies(
                    df_b,
                    system_b,
                    detection.columns.as_ref().map(|v| v.as_slice()),
                    &detection.checks,
                );
            }
        }
        
        Ok(())
    }
    
    /// Enrich dataframe with entities from all tables in the system that have the target grain
    /// This ensures we detect entities that exist in other tables (e.g., extra loans in separate tables)
    async fn enrich_with_system_entities(
        &self,
        df: DataFrame,
        system: &str,
        grain: &[String],
    ) -> Result<DataFrame> {
        // Get all tables in the system that have the target grain columns
        let system_tables: Vec<_> = self.metadata.tables
            .iter()
            .filter(|t| t.system == system)
            .filter(|t| {
                // Check if table has all grain columns
                // First check if columns metadata exists, then check if grain columns are present
                if let Some(ref columns) = t.columns {
                    grain.iter().all(|grain_col| {
                        columns.iter().any(|c| c.name == *grain_col)
                    })
                } else {
                    // If columns metadata is not available, assume table might have the columns
                    // We'll verify when reading the actual parquet file
                    true
                }
            })
            .collect();
        
        if system_tables.is_empty() {
            return Ok(df);
        }
        
        // Start with the original dataframe's grain columns
        let grain_cols: Vec<Expr> = grain.iter().map(|c| col(c)).collect();
        let mut all_entities_lazy = df
            .clone()
            .lazy()
            .select(grain_cols.clone())
            .unique(None, UniqueKeepStrategy::First);
        
        // Load entities from each table in the system and union them
        for table in system_tables {
            let table_path = self.data_dir.join(&table.path);
            if !table_path.exists() {
                continue;
            }
            
            // Try to read the file (CSV or Parquet based on extension)
            let table_df_result = if table_path.extension().and_then(|s| s.to_str()) == Some("csv") {
                // Load CSV file
                LazyCsvReader::new(&table_path)
                    .with_try_parse_dates(true)
                    .with_infer_schema_length(Some(1000))
                    .finish()
                    .and_then(|lf| lf.collect())
            } else {
                // Load Parquet file (default)
                LazyFrame::scan_parquet(&table_path, ScanArgsParquet::default())
                    .and_then(|lf| lf.collect())
            };
            
            if let Ok(mut table_df) = table_df_result {
                // Convert any string columns containing scientific notation to numeric
                table_df = data_utils::convert_scientific_notation_columns(table_df)?;
                // Check if table has all grain columns
                let has_all_grain_cols = grain.iter().all(|grain_col| {
                    table_df.column(grain_col).is_ok()
                });
                
                if has_all_grain_cols {
                    // Select grain columns and union
                    let table_entities = table_df
                        .lazy()
                        .select(grain_cols.clone())
                        .unique(None, UniqueKeepStrategy::First);
                    
                    all_entities_lazy = polars::prelude::concat(
                        [all_entities_lazy, table_entities],
                        UnionArgs::default(),
                    )?
                    .unique(None, UniqueKeepStrategy::First);
                }
            }
        }
        
        // Get all unique entities
        let all_entities = all_entities_lazy.collect()?;
        
        // If original df has metric column, left join to preserve metric values
        let metric_col_name = df.get_column_names()
            .iter()
            .find(|c| !grain.contains(&c.to_string()))
            .map(|s| s.to_string());
        
        if let Some(metric_col) = metric_col_name {
            // Left join original df to preserve metric values
            let grain_exprs: Vec<Expr> = grain.iter().map(|c| col(c)).collect();
            let enriched_df = all_entities
                .lazy()
                .join(
                    df.lazy(),
                    grain_exprs.clone(),
                    grain_exprs.clone(),
                    JoinArgs::new(JoinType::Left),
                )
                .collect()?;
            Ok(enriched_df)
        } else {
            // No metric column, just return entities
            Ok(all_entities)
        }
    }
    
    fn classify_mismatches(
        &self,
        comparison: &ComparisonResult,
        temporal_misalignment: Option<&crate::time::TemporalMisalignment>,
        rule_a: &crate::metadata::Rule,
        rule_b: &crate::metadata::Rule,
    ) -> Result<Vec<RootCauseClassification>> {
        let mut classifications = Vec::new();
        
        // Population mismatch
        if !comparison.population_diff.missing_in_b.is_empty() {
            classifications.push(RootCauseClassification {
                root_cause: "Population Mismatch".to_string(),
                subtype: "Missing Entities".to_string(),
                description: format!("{} entities missing in system B", comparison.population_diff.missing_in_b.len()),
                count: comparison.population_diff.missing_in_b.len(),
            });
        }
        
        if !comparison.population_diff.extra_in_b.is_empty() {
            classifications.push(RootCauseClassification {
                root_cause: "Population Mismatch".to_string(),
                subtype: "Extra Entities".to_string(),
                description: format!("{} extra entities in system B", comparison.population_diff.extra_in_b.len()),
                count: comparison.population_diff.extra_in_b.len(),
            });
        }
        
        // Check for Logic Mismatch by comparing formulas and source entities
        let formulas_differ = rule_a.computation.formula != rule_b.computation.formula;
        let source_entities_differ = rule_a.computation.source_entities != rule_b.computation.source_entities;
        let is_logic_mismatch = formulas_differ || source_entities_differ;
        
        // Data mismatch vs Logic Mismatch
        if comparison.data_diff.mismatches > 0 {
            if is_logic_mismatch {
                // Logic Mismatch: Different calculation methods/formulas
                let mismatch_type = if formulas_differ && source_entities_differ {
                    "Formula and Source Difference"
                } else if formulas_differ {
                    "Formula Difference"
                } else {
                    "Source Entity Difference"
                };
                
                classifications.push(RootCauseClassification {
                    root_cause: "Logic Mismatch".to_string(),
                    subtype: mismatch_type.to_string(),
                    description: format!("{} entities have different metric values due to different calculation methods (formulas or source entities differ)", comparison.data_diff.mismatches),
                    count: comparison.data_diff.mismatches,
                });
            } else {
                // Data Mismatch: Same formula but different values
                classifications.push(RootCauseClassification {
                    root_cause: "Data Mismatch".to_string(),
                    subtype: "Value Difference".to_string(),
                    description: format!("{} entities have different metric values (same calculation method)", comparison.data_diff.mismatches),
                    count: comparison.data_diff.mismatches,
                });
            }
        }
        
        // Time misalignment
        if temporal_misalignment.is_some() {
            classifications.push(RootCauseClassification {
                root_cause: "Data Mismatch".to_string(),
                subtype: "Time Misalignment".to_string(),
                description: "Temporal misalignment detected between systems".to_string(),
                count: 1,
            });
        }
        
        Ok(classifications)
    }
}

#[derive(Debug, Clone)]
pub struct RcaResult {
    pub query: String,
    pub system_a: String,
    pub system_b: String,
    pub metric: String,
    pub as_of_date: Option<NaiveDate>,
    pub comparison: ComparisonResult,
    pub classifications: Vec<RootCauseClassification>,
    pub divergence: Option<DivergencePoint>,
    pub temporal_misalignment: Option<crate::time::TemporalMisalignment>,
}

impl std::fmt::Display for RcaResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "RCA Result for: {}", self.query)?;
        writeln!(f, "System A: {} | System B: {} | Metric: {}", 
            self.system_a, self.system_b, self.metric)?;
        
        if let Some(date) = self.as_of_date {
            writeln!(f, "As-of Date: {}", date)?;
        }
        
        writeln!(f, "\n=== Classifications ===")?;
        for classification in &self.classifications {
            writeln!(f, "- {} ({})", classification.root_cause, classification.subtype)?;
            writeln!(f, "  {}", classification.description)?;
        }
        
        writeln!(f, "\n=== Population Diff ===")?;
        writeln!(f, "Missing in B: {}", self.comparison.population_diff.missing_in_b.len())?;
        writeln!(f, "Extra in B: {}", self.comparison.population_diff.extra_in_b.len())?;
        writeln!(f, "Common: {}", self.comparison.population_diff.common_count)?;
        
        writeln!(f, "\n=== Data Diff ===")?;
        writeln!(f, "Matches: {}", self.comparison.data_diff.matches)?;
        writeln!(f, "Mismatches: {}", self.comparison.data_diff.mismatches)?;
        
        // Show detailed mismatches
        if self.comparison.data_diff.mismatches > 0 {
            writeln!(f, "\n=== Mismatched Entities ===")?;
            let mismatch_df = &self.comparison.data_diff.mismatch_details;
            
            // Get grain columns (all columns except metric_a, metric_b, diff, abs_diff)
            let all_cols: Vec<String> = mismatch_df.get_column_names().iter().map(|s| s.to_string()).collect();
            let metric_cols = vec!["metric_a", "metric_b", "diff", "abs_diff"];
            let grain_cols: Vec<String> = all_cols.iter()
                .filter(|c| !metric_cols.contains(&c.as_str()))
                .cloned()
                .collect();
            
            // For display, we'll show mismatches in order (can be improved with sorting later)
            // Limit to top 50 mismatches for display
            let display_limit = mismatch_df.height().min(50);
            
            // Print header
            write!(f, "\n{:<40} {:<25} {:<25} {:<15}", 
                "Entity ID", 
                format!("{} (System A)", self.system_a),
                format!("{} (System B)", self.system_b),
                "Difference")?;
            writeln!(f)?;
            writeln!(f, "{}", "-".repeat(105))?;
            
            // Print each mismatch
            for row_idx in 0..display_limit {
                // Get entity ID (combine all grain columns)
                let entity_id: String = grain_cols.iter()
                    .filter_map(|col_name| {
                        if let Ok(col_val) = mismatch_df.column(col_name) {
                            let val_str = match col_val.dtype() {
                                DataType::String => {
                                    col_val.str().unwrap().get(row_idx).unwrap_or("").to_string()
                                }
                                DataType::Int64 => {
                                    col_val.i64().unwrap().get(row_idx).unwrap_or(0).to_string()
                                }
                                DataType::Float64 => {
                                    col_val.f64().unwrap().get(row_idx).unwrap_or(0.0).to_string()
                                }
                                _ => format!("{:?}", col_val.get(row_idx)),
                            };
                            Some(val_str)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                
                // Get metric values
                let metric_a = if let Ok(col_val) = mismatch_df.column("metric_a") {
                    match col_val.dtype() {
                        DataType::Float64 => {
                            format!("{:.2}", col_val.f64().unwrap().get(row_idx).unwrap_or(0.0))
                        }
                        DataType::Int64 => {
                            format!("{}", col_val.i64().unwrap().get(row_idx).unwrap_or(0))
                        }
                        _ => format!("{:?}", col_val.get(row_idx)),
                    }
                } else {
                    "N/A".to_string()
                };
                
                let metric_b = if let Ok(col_val) = mismatch_df.column("metric_b") {
                    match col_val.dtype() {
                        DataType::Float64 => {
                            format!("{:.2}", col_val.f64().unwrap().get(row_idx).unwrap_or(0.0))
                        }
                        DataType::Int64 => {
                            format!("{}", col_val.i64().unwrap().get(row_idx).unwrap_or(0))
                        }
                        _ => format!("{:?}", col_val.get(row_idx)),
                    }
                } else {
                    "N/A".to_string()
                };
                
                let diff = if let Ok(col_val) = mismatch_df.column("diff") {
                    match col_val.dtype() {
                        DataType::Float64 => {
                            format!("{:.2}", col_val.f64().unwrap().get(row_idx).unwrap_or(0.0))
                        }
                        DataType::Int64 => {
                            format!("{}", col_val.i64().unwrap().get(row_idx).unwrap_or(0))
                        }
                        _ => format!("{:?}", col_val.get(row_idx)),
                    }
                } else {
                    "N/A".to_string()
                };
                
                writeln!(f, "{:<45} {:<30} {:<30} {:<20}", entity_id, metric_a, metric_b, diff)?;
            }
            
            if mismatch_df.height() > display_limit {
                writeln!(f, "\n... and {} more mismatches (showing first {})", 
                    mismatch_df.height() - display_limit, display_limit)?;
            }
        }
        
        if let Some(divergence) = &self.divergence {
            writeln!(f, "\n=== Divergence Point ===")?;
            writeln!(f, "Step: {} | Type: {}", divergence.step_index, divergence.divergence_type)?;
            
            // Display LLM-generated root cause details
            if let Some(ref root_causes) = divergence.root_cause_details {
                if !root_causes.is_empty() {
                    writeln!(f, "\n=== Root Cause Analysis (LLM-Generated) ===")?;
                    for (idx, root_cause) in root_causes.iter().enumerate() {
                        writeln!(f, "\n{}. Entity: {}", idx + 1, root_cause.loan_id)?;
                        writeln!(f, "   System A Value: {:.2}", root_cause.system_a_value)?;
                        writeln!(f, "   System B Value: {:.2}", root_cause.system_b_value)?;
                        writeln!(f, "   Difference: {:.2}", root_cause.difference)?;
                        
                        if !root_cause.specific_issues.is_empty() {
                            writeln!(f, "   Specific Issues Identified:")?;
                            for issue in &root_cause.specific_issues {
                                writeln!(f, "      • {}", issue)?;
                            }
                        }
                        
                        if !root_cause.table_contributions.is_empty() {
                            writeln!(f, "   Table Contributions:")?;
                            for (table, contribution) in &root_cause.table_contributions {
                                writeln!(f, "      - {}: {:.2}", table, contribution)?;
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RootCauseClassification {
    pub root_cause: String,
    pub subtype: String,
    pub description: String,
    pub count: usize,
}

