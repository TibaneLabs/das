use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // TibaneLabs fork. `reindex = true` means "metadata download owed". A partial
        // index lets the ingester's re-drive loop find those rows without scanning
        // asset_data. Both statements run out of band (see crate::crdb) so the index is
        // committed before the data change, which CockroachDB requires.
        crate::crdb::exec_all_out_of_band(&[
            "CREATE INDEX IF NOT EXISTS asset_data_reindex_pending ON asset_data (id) WHERE reindex = true",
            // Token-2022 metadata rows used to be written as "processing" without the
            // flag; mark the ones already stored.
            "UPDATE asset_data SET reindex = true \
             WHERE reindex IS NOT TRUE AND metadata_url <> '' AND metadata::text = '\"processing\"'",
        ])
        .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        crate::crdb::exec_out_of_band("DROP INDEX IF EXISTS asset_data_reindex_pending").await
    }
}
