use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // TibaneLabs fork: answers fetched from an upstream DAS provider for assets this
        // node has not indexed. Not authoritative - the local index always wins; this only
        // avoids asking upstream again for the same thing.
        crate::crdb::exec_out_of_band(
            "CREATE TABLE IF NOT EXISTS das_fallback_cache ( \
               method TEXT NOT NULL, \
               cache_key TEXT NOT NULL, \
               response JSONB NOT NULL, \
               fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
               PRIMARY KEY (method, cache_key))",
        )
        .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        crate::crdb::exec_out_of_band("DROP TABLE IF EXISTS das_fallback_cache").await
    }
}
