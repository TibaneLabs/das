#![allow(unused_imports)]
use crate::model::table::{Asset, Tokens};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // CockroachDB: ALTER COLUMN TYPE that rewrites on-disk data cannot run in a
        // transaction, and SeaORM wraps every migration in an explicit BEGIN. Run it
        // on its own connection. Requires `autocommit_before_ddl = on` too, since
        // sqlx uses the extended protocol, which is itself an implicit transaction.
        crate::crdb::exec_all_out_of_band(&[
            "ALTER TABLE asset ALTER COLUMN supply TYPE DECIMAL(20,0)",
            "ALTER TABLE tokens ALTER COLUMN supply TYPE DECIMAL(20,0)",
        ])
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .modify_column(ColumnDef::new(Asset::Supply).big_integer().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Tokens::Table)
                    .modify_column(ColumnDef::new(Tokens::Supply).big_integer().not_null())
                    .to_owned(),
            )
            .await
    }
}
