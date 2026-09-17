//! CockroachDB compatibility helpers for the TibaneLabs fork of DAS.
//!
//! SeaORM runs every migration inside a single transaction. CockroachDB refuses
//! several schema operations in that context which Postgres permits:
//!
//!   * `ALTER COLUMN TYPE` that rewrites on-disk data
//!     -> "unimplemented: ... not supported inside a transaction"
//!   * referencing a column added earlier in the same transaction
//!     -> "no data source matches prefix: ..."
//!
//! Both are lifted by running the statement on its own connection, outside the
//! migration's transaction. That is safe here only because these migrations do
//! not depend on rolling the statement back together with the rest of the
//! migration - if one fails mid-way the migration must be re-run, and each
//! statement below is written to be idempotent or naturally re-runnable.
//!
//! Keeping this in one module is deliberate: it holds the fork's divergence in a
//! single file that upstream will never touch, so rebases stay clean.

use {sea_orm_migration::sea_orm::DbErr, sqlx::Executor};

/// Execute DDL on a fresh connection, outside any migration transaction.
///
/// Uses sqlx directly with a plain `&str`: with no bind arguments sqlx sends a simple
/// `Query` message. Going through SeaORM's `Statement` instead uses the extended
/// protocol (Parse/Bind/Execute), which CockroachDB still treats as a transaction for
/// rewriting schema changes - that is why `cockroach sql` could run these statements
/// while the SeaORM route failed with the very same error.
pub async fn exec_out_of_band(sql: &str) -> Result<(), DbErr> {
    let url = std::env::var("DATABASE_URL").map_err(|_| {
        DbErr::Custom("DATABASE_URL must be set for out-of-band DDL".to_string())
    })?;
    let pool = sqlx::PgPool::connect(&url)
        .await
        .map_err(|e| DbErr::Custom(format!("out-of-band connect: {e}")))?;
    let result = pool.execute(sql).await;
    pool.close().await;
    result.map_err(|e| DbErr::Custom(format!("out-of-band DDL `{sql}`: {e}")))?;
    Ok(())
}

/// Run several statements out of band, in order, stopping at the first error.
pub async fn exec_all_out_of_band(stmts: &[&str]) -> Result<(), DbErr> {
    for s in stmts {
        exec_out_of_band(s).await?;
    }
    Ok(())
}
