//! Where metadata downloads may connect.
//!
//! Metadata URIs are arbitrary on-chain data, so anyone can mint an asset whose URI
//! points at a loopback or private address - on a DAS node that reaches the private
//! validator RPC, the database console, or anything on the internal network. Upstream's
//! downloader has no guard (`// Need to check for malicious sites ?`). Connections made
//! through the public client - including every redirect hop - may only reach globally
//! routable addresses, and proxies are disabled so the check can't be bypassed.

use {
    std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    },
    url::{Host, Url},
};

pub const BLOCKED: &str = "resolves only to non-global addresses";

pub fn public_client(timeout: Duration) -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .user_agent("das-grpc-ingester")
        .connect_timeout(timeout)
        .timeout(timeout)
        .dns_resolver(Arc::new(GlobalOnlyResolver))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 {
                attempt.error(TOO_MANY_REDIRECTS)
            } else if !matches!(attempt.url().scheme(), "http" | "https") || !host_allowed(attempt.url()) {
                attempt.error(BLOCKED)
            } else {
                attempt.follow()
            }
        }))
        .build()
        .map_err(Into::into)
}

pub const TOO_MANY_REDIRECTS: &str = "too many redirects";

/// Literal IP hosts never reach the DNS resolver, so they are checked here. Domain
/// names are checked by [`GlobalOnlyResolver`] when they are actually resolved.
pub fn host_allowed(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(ip)) => is_global(IpAddr::V4(ip)),
        Some(Host::Ipv6(ip)) => is_global(IpAddr::V6(ip)),
        Some(Host::Domain(_)) => true,
        None => false,
    }
}

pub struct GlobalOnlyResolver;

impl reqwest::dns::Resolve for GlobalOnlyResolver {
    fn resolve(&self, name: hyper::client::connect::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let allowed: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .filter(|addr| is_global(addr.ip()))
                .collect();
            if allowed.is_empty() {
                return Err(format!("{host} {BLOCKED}").into());
            }
            let addrs: reqwest::dns::Addrs = Box::new(allowed.into_iter());
            Ok(addrs)
        })
    }
}

const fn is_global(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_global_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4() {
            // IPv4-mapped and IPv4-compatible forms reach the embedded IPv4 address
            Some(v4) => is_global_v4(v4),
            None => is_global_v6(v6),
        },
    }
}

const fn is_global_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || o[0] == 0                                // 0.0.0.0/8
        || (o[0] == 100 && (o[1] & 0xc0) == 64)     // 100.64.0.0/10 carrier-grade NAT
        || (o[0] == 192 && o[1] == 0 && o[2] == 0)  // 192.0.0.0/24 protocol assignments
        || (o[0] == 198 && (o[1] & 0xfe) == 18)     // 198.18.0.0/15 benchmarking
        || o[0] >= 240) // 240.0.0.0/4 reserved
}

const fn is_global_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (s[0] & 0xfe00) == 0xfc00                // fc00::/7 unique local
        || (s[0] & 0xffc0) == 0xfe80                // fe80::/10 link local
        || (s[0] == 0x2001 && s[1] == 0x0db8)       // 2001:db8::/32 documentation
        || (s[0] == 0x0064 && s[1] == 0xff9b)) // 64:ff9b::/96 NAT64 reaches IPv4 space
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowed(url: &str) -> bool {
        host_allowed(&Url::parse(url).unwrap())
    }

    #[test]
    fn blocks_internal_literals() {
        for url in [
            "http://127.0.0.1:8899/",
            "http://localhost.:8899/", // domain: left to the resolver
            "http://10.0.0.1/",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]:26257/",
            "http://[::ffff:127.0.0.1]/",
            "http://[fd00::1]/",
            "http://100.64.1.1/",
            "http://0.0.0.0:8090/",
            "http://2130706433/", // 127.0.0.1 as an integer, normalised by the URL parser
            "http://0x7f.1/",
        ] {
            let expect_allowed = url.starts_with("http://localhost.");
            assert_eq!(allowed(url), expect_allowed, "{url}");
        }
    }

    #[test]
    fn allows_public() {
        assert!(allowed("https://arweave.net/abc"));
        assert!(allowed("https://1.1.1.1/"));
        assert!(allowed("https://[2606:4700:4700::1111]/"));
    }

    #[tokio::test]
    async fn resolver_refuses_loopback_names() {
        use reqwest::dns::Resolve;
        let name: hyper::client::connect::dns::Name = "localhost".parse().unwrap();
        let err = GlobalOnlyResolver.resolve(name).await.err().expect("must refuse");
        assert!(err.to_string().contains(BLOCKED));
    }
}
