//! Semantic Completeness Gate
//! 
//! Validates that generated SQL includes all entities required by the user's question
//! before execution. This prevents irrecoverable data loss from missing joins, tables, or entities.

pub mod entity_extractor;
pub mod entity_mapper;
pub mod sql_validator;
pub mod regeneration_loop;

#[cfg(test)]
mod tests;

pub use entity_extractor::{EntityExtractor, RequiredEntitySet};
pub use entity_mapper::EntityMapper;
pub use sql_validator::{SqlValidator, ValidationResult};
pub use regeneration_loop::SemanticCompletenessValidator;

