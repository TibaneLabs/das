use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

use crate::model::table::Asset;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .add_column(
                        ColumnDef::new(Asset::SlotUpdatedMetadataAccount)
                            .big_integer()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .add_column(
                        ColumnDef::new(Asset::SlotUpdatedTokenAccount)
                            .big_integer()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .add_column(
                        ColumnDef::new(Asset::SlotUpdatedMintAccount)
                            .big_integer()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .add_column(
                        ColumnDef::new(Asset::SlotUpdatedCnftTransaction)
                            .big_integer()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        // TibaneLabs fork: upstream creates update_slot_updated_trigger here. CockroachDB
        // cannot express a BEFORE UPDATE trigger that mutates NEW, and we keep logic out
        // of the database anyway: program_transformers maintains asset.slot_updated in
        // each upsert instead.

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .drop_column(Asset::SlotUpdatedMetadataAccount)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .drop_column(Asset::SlotUpdatedTokenAccount)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .drop_column(Asset::SlotUpdatedMintAccount)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .drop_column(Asset::SlotUpdatedCnftTransaction)
                    .to_owned(),
            )
            .await?;

        let connection = manager.get_connection();

        connection
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                "
                DROP TRIGGER IF EXISTS update_slot_updated_trigger ON asset;
                "
                .to_string(),
            ))
            .await?;

        connection
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                "
                DROP FUNCTION IF EXISTS update_slot_updated();
                "
                .to_string(),
            ))
            .await?;

        Ok(())
    }
}
