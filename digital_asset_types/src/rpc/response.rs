use schemars::JsonSchema;

use {
    crate::rpc::{Asset, TokenAccount},
    serde::{Deserialize, Serialize},
};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct DasError {
    pub id: String,
    pub error: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct GetGroupingResponse {
    pub group_key: String,
    pub group_name: String,
    pub group_size: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct AssetList {
    pub total: u32,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub items: Vec<Asset>,
    /// TibaneLabs fork: present only when `showNativeBalance` was requested.
    /// `AssetList` has no `rename_all`, so the camelCase name is explicit here - clients
    /// written against Helius look for `nativeBalance`.
    #[serde(rename = "nativeBalance", skip_serializing_if = "Option::is_none")]
    pub native_balance: Option<NativeBalance>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<DasError>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct TransactionSignatureList {
    pub total: u32,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub items: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct TokenAccountList {
    pub total: u32,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub token_accounts: Vec<TokenAccount>,
    pub cursor: Option<String>,
    pub errors: Vec<DasError>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct NftEdition {
    pub mint_address: String,
    pub edition_address: String,
    pub edition_number: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct NftEditions {
    pub total: u32,
    pub limit: u32,
    pub master_edition_address: String,
    pub supply: u64,
    pub max_supply: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub editions: Vec<NftEdition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// The owner's SOL balance, as Helius returns it alongside a list of assets.
/// `price_per_sol`/`total_price` need a price feed we do not run, so they are omitted.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NativeBalance {
    pub lamports: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_per_sol: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_price: Option<f64>,
}
