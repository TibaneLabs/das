//! Yellowstone protobuf updates -> the structs `program_transformers` consumes.
//!
//! Mirrors what nft_ingester's plerkle deserializer produced, minus the flatbuffers.

use {
    anyhow::{anyhow, Context},
    program_transformers::{AccountInfo, TransactionInfo},
    solana_message::compiled_instruction::CompiledInstruction,
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    solana_transaction_status::{InnerInstruction, InnerInstructions},
    yellowstone_grpc_proto::prelude::{SubscribeUpdateAccount, SubscribeUpdateTransaction},
};

pub fn account(update: SubscribeUpdateAccount) -> anyhow::Result<AccountInfo> {
    let account = update.account.context("account update carries no account")?;
    Ok(AccountInfo {
        slot: update.slot,
        pubkey: pubkey(&account.pubkey)?,
        owner: pubkey(&account.owner)?,
        data: account.data,
    })
}

pub fn transaction(update: SubscribeUpdateTransaction) -> anyhow::Result<TransactionInfo> {
    let info = update
        .transaction
        .context("transaction update carries no transaction")?;
    let message = info
        .transaction
        .and_then(|tx| tx.message)
        .context("transaction carries no message")?;
    let meta = info.meta.context("transaction carries no status meta")?;

    // Same order as Solana's AccountKeys: static keys, then the addresses loaded from
    // lookup tables - writable before readonly. Instructions in v0 transactions index
    // into this combined list, so dropping the loaded addresses silently mis-resolves
    // accounts for any transaction that uses a lookup table.
    let account_keys = message
        .account_keys
        .iter()
        .chain(&meta.loaded_writable_addresses)
        .chain(&meta.loaded_readonly_addresses)
        .map(|key| pubkey(key))
        .collect::<anyhow::Result<Vec<_>>>()?;

    let message_instructions = message
        .instructions
        .into_iter()
        .map(|ix| {
            Ok(CompiledInstruction {
                program_id_index: index(ix.program_id_index)?,
                accounts: ix.accounts,
                data: ix.data,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let meta_inner_instructions = if meta.inner_instructions_none {
        Vec::new()
    } else {
        meta.inner_instructions
            .into_iter()
            .map(|inner| {
                Ok(InnerInstructions {
                    index: index(inner.index)?,
                    instructions: inner
                        .instructions
                        .into_iter()
                        .map(|ix| {
                            Ok(InnerInstruction {
                                instruction: CompiledInstruction {
                                    program_id_index: index(ix.program_id_index)?,
                                    accounts: ix.accounts,
                                    data: ix.data,
                                },
                                stack_height: ix.stack_height,
                            })
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    };

    Ok(TransactionInfo {
        slot: update.slot,
        signature: Signature::try_from(info.signature.as_slice())
            .map_err(|_| anyhow!("signature has {} bytes", info.signature.len()))?,
        account_keys,
        message_instructions,
        meta_inner_instructions,
    })
}

fn pubkey(bytes: &[u8]) -> anyhow::Result<Pubkey> {
    Pubkey::try_from(bytes).map_err(|_| anyhow!("pubkey has {} bytes", bytes.len()))
}

fn index(value: u32) -> anyhow::Result<u8> {
    u8::try_from(value).map_err(|_| anyhow!("instruction index {value} does not fit in u8"))
}
