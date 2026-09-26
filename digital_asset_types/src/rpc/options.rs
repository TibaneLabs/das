use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Options {
    #[serde(default)]
    pub show_unverified_collections: bool,
    #[serde(default)]
    pub show_collection_metadata: bool,
    #[serde(default)]
    pub show_zero_balance: bool,
    #[serde(default)]
    pub show_inscription: bool,
    #[serde(default)]
    pub show_fungible: bool,
    /// TibaneLabs fork: Helius extension, not in the DAS spec. When set, list responses
    /// carry the owner's SOL balance in `nativeBalance`.
    #[serde(default)]
    pub show_native_balance: bool,
}
