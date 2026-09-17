//! Send IPFS content to one configured gateway.
//!
//! Public gateways rate-limit hard (ipfs.io answers most of a mainnet firehose with
//! HTTP 429), so production should point this at a gateway we run. The URI stored in
//! the database is never changed - only the URL that gets fetched.

use url::Url;

/// Public gateways whose `/ipfs/<cid>` paths and `<cid>.ipfs.<host>` subdomains name
/// plain IPFS content that any gateway can serve.
const PUBLIC_GATEWAYS: &[&str] = &[
    "ipfs.io",
    "dweb.link",
    "cloudflare-ipfs.com",
    "cf-ipfs.com",
    "gateway.pinata.cloud",
    "nftstorage.link",
    "w3s.link",
    "4everland.io",
    "ipfs.infura.io",
    "infura-ipfs.io",
    "ipfs.filebase.io",
    "gateway.lighthouse.storage",
];

pub struct Gateway {
    base: Url,
    rewrite_public: bool,
}

impl Gateway {
    pub fn new(base: &str, rewrite_public: bool) -> anyhow::Result<Self> {
        let base = Url::parse(base.trim_end_matches('/'))?;
        anyhow::ensure!(
            matches!(base.scheme(), "http" | "https") && base.host_str().is_some(),
            "IPFS gateway must be an http(s) URL, got {base}"
        );
        Ok(Self { base, rewrite_public })
    }

    /// Requests to our own gateway are trusted: it is operator-configured and may well
    /// live on a private address.
    pub fn is_gateway(&self, url: &Url) -> bool {
        url.scheme() == self.base.scheme()
            && url.host_str() == self.base.host_str()
            && url.port_or_known_default() == self.base.port_or_known_default()
    }

    /// The gateway URL for `url` if it names IPFS content, `None` to fetch as-is.
    /// `ipfs://` is always rewritten (nothing else can fetch it). Other public gateways
    /// only when `rewrite_public` is set - while the gateway *is* a public one, moving
    /// their traffic onto it would just concentrate the rate limiting.
    pub fn rewrite(&self, url: &Url) -> Option<Url> {
        let content = match url.scheme() {
            "ipfs" => {
                let joined = format!("{}{}", url.host_str()?, url.path());
                joined.strip_prefix("ipfs/").map_or(joined.clone(), str::to_owned)
            }
            "http" | "https" if self.rewrite_public && !self.is_gateway(url) => {
                let host = url.host_str()?.to_ascii_lowercase();
                if let Some((cid, gateway)) = host.split_once(".ipfs.") {
                    if !PUBLIC_GATEWAYS.contains(&gateway) {
                        return None;
                    }
                    format!("{cid}{}", url.path())
                } else if PUBLIC_GATEWAYS.contains(&host.as_str()) {
                    url.path().strip_prefix("/ipfs/")?.to_owned()
                } else {
                    return None;
                }
            }
            _ => return None,
        };
        let content = content.trim_start_matches('/');
        let cid = content.split('/').next()?;
        if !looks_like_cid(cid) {
            return None;
        }
        let mut out = self.base.clone();
        out.set_path(&format!("{}/ipfs/{content}", self.base.path().trim_end_matches('/')));
        out.set_query(url.query());
        Some(out)
    }
}

/// CIDv0 (base58btc "Qm...", 46 chars) or CIDv1 in base32 ("b..."). Loose on purpose:
/// it only has to avoid rewriting things that clearly aren't content addresses.
fn looks_like_cid(s: &str) -> bool {
    let v0 = s.len() == 46
        && s.starts_with("Qm")
        && s.bytes().all(|b| b.is_ascii_alphanumeric() && !matches!(b, b'0' | b'O' | b'I' | b'l'));
    let v1 = s.len() >= 50 && s.starts_with('b') && s.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'));
    v0 || v1
}

#[cfg(test)]
mod tests {
    use super::*;

    const V0: &str = "QmZg9jBiceyo7Mhy3EmQ9k3r8hS4oQ9Ld8Lb4BdWc1Q6xT";
    const V1: &str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";

    fn rw(gateway: &str, public: bool, url: &str) -> Option<String> {
        Gateway::new(gateway, public).unwrap().rewrite(&Url::parse(url).unwrap()).map(|u| u.to_string())
    }

    #[test]
    fn ipfs_scheme_always_rewritten() {
        let want = format!("https://ipfs.io/ipfs/{V0}/1.json");
        assert_eq!(rw("https://ipfs.io", false, &format!("ipfs://{V0}/1.json")).as_deref(), Some(want.as_str()));
        assert_eq!(rw("https://ipfs.io", false, &format!("ipfs://ipfs/{V0}/1.json")).as_deref(), Some(want.as_str()));
        assert_eq!(
            rw("http://10.1.2.3:8080/", false, &format!("ipfs://{V1}")).as_deref(),
            Some(format!("http://10.1.2.3:8080/ipfs/{V1}").as_str())
        );
    }

    #[test]
    fn public_gateways_only_when_enabled() {
        let pinata = format!("https://gateway.pinata.cloud/ipfs/{V0}/meta.json?x=1");
        assert_eq!(rw("http://10.1.2.3:8080", false, &pinata), None);
        assert_eq!(
            rw("http://10.1.2.3:8080", true, &pinata).as_deref(),
            Some(format!("http://10.1.2.3:8080/ipfs/{V0}/meta.json?x=1").as_str())
        );
        assert_eq!(
            rw("http://10.1.2.3:8080", true, &format!("https://{V1}.ipfs.dweb.link/0.json")).as_deref(),
            Some(format!("http://10.1.2.3:8080/ipfs/{V1}/0.json").as_str())
        );
    }

    #[test]
    fn leaves_everything_else_alone() {
        assert_eq!(rw("http://gw", true, "https://arweave.net/abc"), None);
        assert_eq!(rw("http://gw", true, "https://example.com/ipfs/not-a-cid"), None);
        assert_eq!(rw("http://gw", true, "https://ipfs.io/ipns/example.eth"), None);
        assert_eq!(rw("https://ipfs.io", true, &format!("https://ipfs.io/ipfs/{V0}")), None); // already ours
    }
}
