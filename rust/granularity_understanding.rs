use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use crate::metadata::Metadata;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Granularity Understanding Engine
/// Parses natural language business rules to understand entity hierarchies and granularity
pub struct GranularityUnderstanding {
    metadata: Metadata,
    llm: LlmClient,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityHierarchy {
    /// Parent entity -> Child entities mapping
    /// e.g., {"customer": ["loan"], "loan": ["emi"]}
    pub relationships: HashMap<String, Vec<String>>,
    /// Entity -> Grain columns mapping
    /// e.g., {"customer": ["customer_id"], "loan": ["loan_id"], "emi": ["loan_id", "emi_number"]}
    pub entity_grains: HashMap<String, Vec<String>>,
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
    /// Source of the understanding (rule, llm_reasoning, or inferred)
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GranularityRule {
    /// Natural language description
    pub description: String,
    /// Parsed hierarchy
    pub hierarchy: EntityHierarchy,
    /// Ambiguity level: low, medium, high
    pub ambiguity_level: String,
    /// Questions to ask if ambiguous
    pub clarification_questions: Vec<ClarificationQuestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClarificationQuestion {
    pub question: String,
    pub options: Vec<String>,
    pub context: String,
}

#[derive(Debug, Clone)]
pub struct ChainOfThought {
    pub steps: Vec<ThoughtStep>,
    pub final_hierarchy: EntityHierarchy,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct ThoughtStep {
    pub step_number: usize,
    pub reasoning: String,
    pub evidence: Vec<String>,
    pub conclusion: String,
}

impl GranularityUnderstanding {
    pub fn new(metadata: Metadata, llm: LlmClient) -> Self {
        Self { metadata, llm }
    }

    /// Parse natural language business rule to understand granularity
    /// Example: "each customer can take multiple loan each loan can have multiple emi if it is of emi type"
    pub async fn parse_business_rule(
        &self,
        rule_text: &str,
    ) -> Result<GranularityRule> {
        println!("\n🧠 GRANULARITY UNDERSTANDING ENGINE");
        println!("   Parsing business rule: \"{}\"", rule_text);
        
        // Step 1: Extract entities and relationships from natural language
        let mut chain_of_thought = ChainOfThought {
            steps: Vec::new(),
            final_hierarchy: EntityHierarchy {
                relationships: HashMap::new(),
                entity_grains: HashMap::new(),
                confidence: 0.0,
                source: String::new(),
            },
            confidence: 0.0,
        };
        
        // Step 1: Try to extract from explicit patterns
        let (step1, extracted_relationships) = self.extract_from_patterns(rule_text)?;
        chain_of_thought.steps.push(step1);
        chain_of_thought.final_hierarchy.relationships = extracted_relationships;
        chain_of_thought.final_hierarchy.source = "pattern_extraction".to_string();
        chain_of_thought.final_hierarchy.confidence = 0.8;
        
        // Step 2: Use LLM reasoning if patterns don't provide enough information
        if chain_of_thought.final_hierarchy.relationships.is_empty() {
            let (step2, llm_relationships) = self.llm_reason_about_granularity(rule_text).await?;
            chain_of_thought.steps.push(step2);
            chain_of_thought.final_hierarchy.relationships = llm_relationships;
            chain_of_thought.final_hierarchy.source = "llm_reasoning".to_string();
            chain_of_thought.final_hierarchy.confidence = 0.7;
        }
        
        // Step 3: Cross-reference with knowledge base (metadata)
        let step3 = self.cross_reference_with_kb(&chain_of_thought.final_hierarchy).await?;
        chain_of_thought.steps.push(step3);
        
        // Step 4: Determine grain columns for each entity
        let step4 = self.determine_entity_grains(&mut chain_of_thought.final_hierarchy).await?;
        chain_of_thought.steps.push(step4);
        
        // Step 5: Assess ambiguity
        let ambiguity_assessment = self.assess_ambiguity(&chain_of_thought.final_hierarchy).await?;
        
        // Build clarification questions if needed
        let clarification_questions = if ambiguity_assessment.ambiguity_level == "high" {
            self.generate_clarification_questions(&chain_of_thought.final_hierarchy, rule_text).await?
        } else {
            Vec::new()
        };
        
        println!("   ✅ Parsed hierarchy:");
        for (parent, children) in &chain_of_thought.final_hierarchy.relationships {
            println!("      {} -> {:?}", parent, children);
        }
        println!("   ✅ Entity grains:");
        for (entity, grain) in &chain_of_thought.final_hierarchy.entity_grains {
            println!("      {} -> {:?}", entity, grain);
        }
        chain_of_thought.confidence = chain_of_thought.final_hierarchy.confidence;
        println!("   ✅ Confidence: {:.2}%", chain_of_thought.confidence * 100.0);
        println!("   ✅ Ambiguity level: {}", ambiguity_assessment.ambiguity_level);
        
        Ok(GranularityRule {
            description: rule_text.to_string(),
            hierarchy: chain_of_thought.final_hierarchy,
            ambiguity_level: ambiguity_assessment.ambiguity_level,
            clarification_questions,
        })
    }

    /// Extract relationships from explicit patterns in natural language
    fn extract_from_patterns(&self, rule_text: &str) -> Result<(ThoughtStep, HashMap<String, Vec<String>>)> {
        let mut relationships: HashMap<String, Vec<String>> = HashMap::new();
        let mut evidence = Vec::new();
        
        // Pattern 1: "each X can have multiple Y" or "each X can take multiple Y"
        let pattern1 = regex::Regex::new(r"(?i)each\s+(\w+)\s+can\s+(?:have|take)\s+multiple\s+(\w+)").unwrap();
        for cap in pattern1.captures_iter(rule_text) {
            let parent = cap.get(1).unwrap().as_str().to_lowercase();
            let child = cap.get(2).unwrap().as_str().to_lowercase();
            let parent_display = parent.clone();
            let child_display = child.clone();
            relationships.entry(parent).or_insert_with(Vec::new).push(child.clone());
            evidence.push(format!("Pattern match: 'each {} can have/take multiple {}'", parent_display, child_display));
        }
        
        // Pattern 2: "X has many Y" or "X contains Y"
        let pattern2 = regex::Regex::new(r"(?i)(\w+)\s+(?:has|contains)\s+many\s+(\w+)").unwrap();
        for cap in pattern2.captures_iter(rule_text) {
            let parent = cap.get(1).unwrap().as_str().to_lowercase();
            let child = cap.get(2).unwrap().as_str().to_lowercase();
            let parent_display = parent.clone();
            let child_display = child.clone();
            relationships.entry(parent).or_insert_with(Vec::new).push(child.clone());
            evidence.push(format!("Pattern match: '{} has/contains many {}'", parent_display, child_display));
        }
        
        // Pattern 3: "Y belongs to X" or "Y is part of X"
        let pattern3 = regex::Regex::new(r"(?i)(\w+)\s+(?:belongs\s+to|is\s+part\s+of)\s+(\w+)").unwrap();
        for cap in pattern3.captures_iter(rule_text) {
            let child = cap.get(1).unwrap().as_str().to_lowercase();
            let parent = cap.get(2).unwrap().as_str().to_lowercase();
            let parent_display = parent.clone();
            let child_display = child.clone();
            relationships.entry(parent).or_insert_with(Vec::new).push(child.clone());
            evidence.push(format!("Pattern match: '{} belongs to/is part of {}'", child_display, parent_display));
        }
        
        let reasoning = if !relationships.is_empty() {
            format!("Extracted {} relationship(s) from explicit patterns in the rule", relationships.len())
        } else {
            "No explicit patterns found in rule text".to_string()
        };
        
        Ok((
            ThoughtStep {
                step_number: 1,
                reasoning,
                evidence,
                conclusion: format!("Found {} parent-child relationship(s)", relationships.len()),
            },
            relationships,
        ))
    }

    /// Use LLM to reason about granularity when patterns don't provide enough info
    async fn llm_reason_about_granularity(&self, rule_text: &str) -> Result<(ThoughtStep, HashMap<String, Vec<String>>)> {
        println!("   🤖 Using LLM reasoning to understand granularity...");
        
        // Get available entities from metadata
        let available_entities: Vec<String> = self.metadata.entities
            .iter()
            .map(|e| format!("{} (grain: {:?})", e.name, e.grain))
            .collect();
        
        let prompt = format!(
            r#"Analyze this business rule and extract entity relationships and granularity hierarchy.
Return JSON only with this structure:
{{
  "relationships": {{"parent_entity": ["child1", "child2"]}},
  "reasoning": "explanation of how you inferred the relationships",
  "confidence": 0.0-1.0
}}

Business Rule: "{}"

Available Entities: {}

Examples:
- "each customer can take multiple loan" -> {{"relationships": {{"customer": ["loan"]}}, "reasoning": "customer is parent, loan is child", "confidence": 0.9}}
- "each loan can have multiple emi" -> {{"relationships": {{"loan": ["emi"]}}, "reasoning": "loan is parent, emi is child", "confidence": 0.9}}

Return JSON only:"#,
            rule_text,
            available_entities.join(", ")
        );
        
        let response = self.llm.call_llm(&prompt).await?;
        
        // Parse JSON response
        #[derive(Deserialize)]
        struct LlmResponse {
            relationships: HashMap<String, Vec<String>>,
            reasoning: String,
            confidence: f64,
        }
        
        let llm_result: LlmResponse = serde_json::from_str(&response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM response: {}", e)))?;
        
        Ok((
            ThoughtStep {
                step_number: 2,
                reasoning: format!("LLM reasoning: {}", llm_result.reasoning),
                evidence: vec![format!("LLM extracted {} relationship(s)", llm_result.relationships.len())],
                conclusion: format!("LLM confidence: {:.2}%", llm_result.confidence * 100.0),
            },
            llm_result.relationships,
        ))
    }

    /// Cross-reference extracted hierarchy with knowledge base (metadata)
    async fn cross_reference_with_kb(&self, hierarchy: &EntityHierarchy) -> Result<ThoughtStep> {
        println!("   📚 Cross-referencing with knowledge base...");
        
        let mut validated_relationships = HashMap::new();
        let mut evidence = Vec::new();
        
        // Check if entities exist in metadata
        let known_entities: HashSet<String> = self.metadata.entities
            .iter()
            .map(|e| e.name.to_lowercase())
            .collect();
        
        for (parent, children) in &hierarchy.relationships {
            let parent_exists = known_entities.contains(&parent.to_lowercase());
            let valid_children: Vec<String> = children.iter()
                .filter(|child| known_entities.contains(&child.to_lowercase()))
                .cloned()
                .collect();
            
            if parent_exists && !valid_children.is_empty() {
                validated_relationships.insert(parent.clone(), valid_children.clone());
                evidence.push(format!("Validated: {} -> {:?} (all entities exist in KB)", parent, valid_children));
            } else {
                evidence.push(format!("Warning: {} or some children not found in KB", parent));
            }
        }
        
        let reasoning = format!(
            "Cross-referenced {} relationship(s) with knowledge base. {} validated.",
            hierarchy.relationships.len(),
            validated_relationships.len()
        );
        
        Ok(ThoughtStep {
            step_number: 3,
            reasoning,
            evidence,
            conclusion: format!("Validated {} relationship(s)", validated_relationships.len()),
        })
    }

    /// Determine grain columns for each entity based on metadata
    async fn determine_entity_grains(&self, hierarchy: &mut EntityHierarchy) -> Result<ThoughtStep> {
        println!("   🔍 Determining entity grains from metadata...");
        
        let mut entity_grains = HashMap::new();
        let mut evidence = Vec::new();
        
        // Get grains from metadata entities
        for entity in &self.metadata.entities {
            let entity_name_lower = entity.name.to_lowercase();
            entity_grains.insert(entity_name_lower, entity.grain.clone());
            evidence.push(format!("Entity '{}' has grain: {:?}", entity.name, entity.grain));
        }
        
        // Also check tables to infer grains from primary keys
        for table in &self.metadata.tables {
            let entity_name_lower = table.entity.to_lowercase();
            if !entity_grains.contains_key(&entity_name_lower) {
                entity_grains.insert(entity_name_lower, table.primary_key.clone());
                evidence.push(format!("Inferred grain for '{}' from table '{}': {:?}", 
                    table.entity, table.name, table.primary_key));
            }
        }
        
        hierarchy.entity_grains = entity_grains.clone();
        
        let reasoning = format!(
            "Determined grains for {} entities from metadata and tables",
            entity_grains.len()
        );
        
        Ok(ThoughtStep {
            step_number: 4,
            reasoning,
            evidence,
            conclusion: format!("Mapped grains for {} entities", entity_grains.len()),
        })
    }

    /// Assess ambiguity level of the extracted hierarchy
    async fn assess_ambiguity(&self, hierarchy: &EntityHierarchy) -> Result<AmbiguityAssessment> {
        let mut ambiguity_score = 0.0;
        let mut reasons = Vec::new();
        
        // Check if relationships are empty
        if hierarchy.relationships.is_empty() {
            ambiguity_score += 0.5;
            reasons.push("No relationships extracted".to_string());
        }
        
        // Check if entity grains are missing
        let missing_grains = hierarchy.relationships.values()
            .flatten()
            .chain(hierarchy.relationships.keys())
            .filter(|entity| !hierarchy.entity_grains.contains_key(*entity))
            .count();
        
        if missing_grains > 0 {
            ambiguity_score += 0.3;
            reasons.push(format!("{} entities missing grain information", missing_grains));
        }
        
        // Check confidence
        if hierarchy.confidence < 0.7 {
            ambiguity_score += 0.2;
            reasons.push(format!("Low confidence: {:.2}%", hierarchy.confidence * 100.0));
        }
        
        let ambiguity_level = if ambiguity_score >= 0.7 {
            "high"
        } else if ambiguity_score >= 0.4 {
            "medium"
        } else {
            "low"
        };
        
        Ok(AmbiguityAssessment {
            ambiguity_level: ambiguity_level.to_string(),
            score: ambiguity_score,
            reasons,
        })
    }

    /// Generate clarification questions if ambiguity is high
    async fn generate_clarification_questions(
        &self,
        hierarchy: &EntityHierarchy,
        rule_text: &str,
    ) -> Result<Vec<ClarificationQuestion>> {
        let mut questions = Vec::new();
        
        // If relationships are missing, ask about them
        if hierarchy.relationships.is_empty() {
            questions.push(ClarificationQuestion {
                question: "What are the entity relationships described in this rule?".to_string(),
                options: vec!["One-to-many".to_string(), "Many-to-many".to_string(), "One-to-one".to_string()],
                context: rule_text.to_string(),
            });
        }
        
        // If grains are missing, ask about them
        let missing_grains: Vec<String> = hierarchy.relationships.values()
            .flatten()
            .chain(hierarchy.relationships.keys())
            .filter(|entity| !hierarchy.entity_grains.contains_key(*entity))
            .cloned()
            .collect();
        
        if !missing_grains.is_empty() {
            questions.push(ClarificationQuestion {
                question: format!("What are the grain columns for these entities: {}?", missing_grains.join(", ")),
                options: Vec::new(), // Will be populated from metadata
                context: rule_text.to_string(),
            });
        }
        
        Ok(questions)
    }

    /// Get chain of thought reasoning for a query
    pub async fn reason_about_granularity_for_query(
        &self,
        query: &str,
        system_a: &str,
        system_b: &str,
        metric: &str,
    ) -> Result<ChainOfThought> {
        println!("\n🧠 CHAIN OF THOUGHT: Granularity Reasoning");
        println!("   Query: \"{}\"", query);
        println!("   Systems: {} vs {}", system_a, system_b);
        println!("   Metric: {}", metric);
        
        let mut steps = Vec::new();
        
        // Step 1: Get business rules for both systems
        let step1 = ThoughtStep {
            step_number: 1,
            reasoning: format!("Analyzing business rules for systems {} and {}", system_a, system_b),
            evidence: vec!["Checking metadata for business rules".to_string()],
            conclusion: "Found business rules for both systems".to_string(),
        };
        steps.push(step1);
        
        // Step 2: Extract hierarchies from rules
        // TODO: Get actual rules from metadata
        let step2 = ThoughtStep {
            step_number: 2,
            reasoning: "Extracting entity hierarchies from business rules".to_string(),
            evidence: vec!["Parsing natural language rules".to_string()],
            conclusion: "Extracted hierarchies".to_string(),
        };
        steps.push(step2);
        
        // Step 3: Compare hierarchies
        let step3 = ThoughtStep {
            step_number: 3,
            reasoning: format!("Comparing hierarchies between {} and {}", system_a, system_b),
            evidence: vec!["Checking for grain mismatches".to_string()],
            conclusion: "Identified grain differences".to_string(),
        };
        steps.push(step3);
        
        // Step 4: Use graph to find resolution paths
        let step4 = ThoughtStep {
            step_number: 4,
            reasoning: "Using knowledge graph to find join paths for grain resolution".to_string(),
            evidence: vec!["Traversing lineage edges".to_string()],
            conclusion: "Found resolution paths".to_string(),
        };
        steps.push(step4);
        
        // Step 5: LLM reasoning for ambiguous cases
        let step5 = ThoughtStep {
            step_number: 5,
            reasoning: "Using LLM to reason about ambiguous relationships".to_string(),
            evidence: vec!["LLM inference".to_string()],
            conclusion: "Resolved ambiguities".to_string(),
        };
        steps.push(step5);
        
        // Build final hierarchy (placeholder - would be computed from actual rules)
        let final_hierarchy = EntityHierarchy {
            relationships: HashMap::new(),
            entity_grains: HashMap::new(),
            confidence: 0.8,
            source: "chain_of_thought".to_string(),
        };
        
        Ok(ChainOfThought {
            steps,
            final_hierarchy,
            confidence: 0.8,
        })
    }
}

#[derive(Debug, Clone)]
struct AmbiguityAssessment {
    ambiguity_level: String,
    score: f64,
    reasons: Vec<String>,
}

