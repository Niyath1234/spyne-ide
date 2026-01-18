//! Query history repository for storing RCA query results

use sqlx::PgPool;
use uuid::Uuid;
use chrono::NaiveDate;

pub struct QueryHistoryRepository {
    pool: PgPool,
}

impl QueryHistoryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    
    pub async fn save_query(&self, query_text: &str) -> Result<Uuid, sqlx::Error> {
        let id = Uuid::new_v4();
        
        sqlx::query!(
            r#"
            INSERT INTO rca_queries (id, query_text, status)
            VALUES ($1, $2, 'pending')
            "#,
            id,
            query_text
        )
        .execute(&self.pool)
        .await?;
        
        Ok(id)
    }
    
    pub async fn update_query_status(
        &self,
        query_id: Uuid,
        status: &str,
        error_message: Option<&str>
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE rca_queries
            SET status = $1, error_message = $2, completed_at = NOW()
            WHERE id = $3
            "#,
            status,
            error_message,
            query_id
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
}

