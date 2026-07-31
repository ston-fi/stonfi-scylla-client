//! Address translation for single-endpoint deployments.
//!
//! The Scylla driver discovers nodes from server-advertised RPC addresses.
//! Those addresses may be unreachable when the configured endpoint is a proxy,
//! a port mapping, or another network boundary. With one configured endpoint,
//! the client therefore pins both the initial contact point and every discovered
//! peer to the same resolved socket address. Multiple endpoints retain the
//! driver's advertised topology.

use std::net::SocketAddr;
use std::sync::Arc;

use scylla::client::session_builder::SessionBuilder;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};

use crate::errors::{ScyllaClientError, ScyllaClientResult};

#[derive(Debug)]
struct RpcAddressTranslator {
    known_node_address: SocketAddr,
}

#[async_trait::async_trait]
impl AddressTranslator for RpcAddressTranslator {
    async fn translate_address(
        &self,
        _untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Ok(self.known_node_address)
    }
}

pub(crate) async fn configure_known_nodes(
    mut session_builder: SessionBuilder,
    endpoints: &[&str],
) -> ScyllaClientResult<SessionBuilder> {
    if let [endpoint] = endpoints {
        // Resolve once so the initial contact point and translated peers cannot
        // diverge because of DNS ordering or changes between lookups.
        let known_node_address = resolve_single_endpoint(endpoint).await?;
        session_builder = session_builder
            .known_node_addr(known_node_address)
            .address_translator(Arc::new(RpcAddressTranslator { known_node_address }));
    } else {
        for endpoint in endpoints {
            session_builder = session_builder.known_node(endpoint);
        }
    }

    Ok(session_builder)
}

async fn resolve_single_endpoint(endpoint: &str) -> ScyllaClientResult<SocketAddr> {
    let known_node_addresses = match tokio::net::lookup_host(endpoint).await {
        Ok(addresses) => addresses.collect::<Vec<_>>(),
        Err(source) => tokio::net::lookup_host((endpoint, 9042))
            .await
            .map(|addresses| addresses.collect::<Vec<_>>())
            .map_err(|_| ScyllaClientError::resolve_endpoint(endpoint, source))?,
    };
    let first_known_node_address = known_node_addresses.first().copied().ok_or_else(|| {
        ScyllaClientError::resolve_endpoint(
            endpoint,
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "resolver returned no addresses",
            ),
        )
    })?;

    // IPv4 is preferred because dual-stack names such as localhost commonly
    // front IPv4-only port mappings. IPv6 remains the fallback.
    Ok(known_node_addresses
        .into_iter()
        .find(SocketAddr::is_ipv4)
        .unwrap_or(first_known_node_address))
}

#[cfg(test)]
mod tests {
    use super::resolve_single_endpoint;

    #[tokio::test]
    async fn test_resolve_single_endpoint_uses_default_port() -> anyhow::Result<()> {
        let address = resolve_single_endpoint("localhost").await?;

        assert_eq!(address.port(), 9042);
        Ok(())
    }
}
