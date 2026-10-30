//! The gRPC stubs for the services a remote component serves, written by
//! build.rs on angzarr-router's types, and the Unix-socket channel they run
//! on.

use std::path::PathBuf;

use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};

/// `io.angzarr.v1.DescribeRequest`, an empty message: `()`, which prost
/// encodes as one.
pub type DescribeRequest = ();

/// `io.angzarr.v1.CommandHandlerService` (command_handler.proto): `Handle`,
/// a contextual command in, the events it emits out.
#[allow(clippy::all, missing_docs)]
pub mod command_handler {
    include!(concat!(
        env!("OUT_DIR"),
        "/io.angzarr.v1.CommandHandlerService.rs"
    ));
}

/// `io.angzarr.v1.PolicyService` (policy.proto): `Handle`, a source
/// event in, the commands it answers out; `OnRefused`, a refusal of one of
/// them in, the refusal fact for its source out.
#[allow(clippy::all, missing_docs)]
pub mod policy {
    include!(concat!(env!("OUT_DIR"), "/io.angzarr.v1.PolicyService.rs"));
}

/// `io.angzarr.v1.ProjectorService` (projector.proto): `Handle`, events
/// in, a projection out.
#[allow(clippy::all, missing_docs)]
pub mod projector {
    include!(concat!(
        env!("OUT_DIR"),
        "/io.angzarr.v1.ProjectorService.rs"
    ));
}

/// `io.angzarr.v1.UpcasterService` (upcaster.proto): `Upcast`, a
/// domain's pages in, upcast; what an upcaster sidecar serves (decision
/// 14.39).
#[allow(clippy::all, missing_docs)]
pub mod upcaster {
    include!(concat!(
        env!("OUT_DIR"),
        "/io.angzarr.v1.UpcasterService.rs"
    ));
}

/// gRPC health checking, `grpc.health.v1` (grpc's health.proto): what an
/// upcaster sidecar serves beside `UpcasterService` (decision 14.39), and
/// the host watches for its readiness. Its two messages are written here,
/// as the proto has them; no crate in the graph has them.
#[allow(clippy::all, missing_docs)]
pub mod grpc_health {
    /// `grpc.health.v1.HealthCheckRequest`: the service asked about; empty
    /// for the server as a whole.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct HealthCheckRequest {
        #[prost(string, tag = "1")]
        pub service: ::prost::alloc::string::String,
    }

    /// `grpc.health.v1.HealthCheckResponse`: the service's status.
    #[derive(Clone, Copy, PartialEq, ::prost::Message)]
    pub struct HealthCheckResponse {
        #[prost(enumeration = "ServingStatus", tag = "1")]
        pub status: i32,
    }

    /// `grpc.health.v1.HealthCheckResponse.ServingStatus`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ServingStatus {
        Unknown = 0,
        Serving = 1,
        NotServing = 2,
        ServiceUnknown = 3,
    }

    include!(concat!(env!("OUT_DIR"), "/grpc.health.v1.Health.rs"));
}

/// `io.angzarr.v1.ComponentService` (component.proto): `Describe`, the
/// component's descriptor.
#[allow(clippy::all, missing_docs)]
pub mod component {
    include!(concat!(
        env!("OUT_DIR"),
        "/io.angzarr.v1.ComponentService.rs"
    ));
}

/// A channel to the server listening on the Unix socket at `socket`. The
/// endpoint's URI is a placeholder tonic requires; every connection is the
/// socket.
pub(crate) async fn connect_unix(socket: PathBuf) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_static("http://[::]:0")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(socket).await?)) }
        }))
        .await
}

/// google.rpc's `Status` (google/rpc/status.proto) and `ErrorInfo`
/// (google/rpc/error_details.proto): how a gRPC status's details travel.
/// A non-OK status from a child carries a `Status` in its details, whose
/// own details are an `ErrorInfo` (the coded error's reason and metadata)
/// and, for a refusal, the typed refusal. The messages are tonic-types'
/// maintained prost types, re-exported here.
pub mod google_rpc {
    pub use tonic_types::pb::{ErrorInfo, Status};

    /// `ErrorInfo`'s fully-qualified name, as a type URL ends.
    pub const ERROR_INFO_NAME: &str = "google.rpc.ErrorInfo";

    /// The type URL a status detail packs an `ErrorInfo` under.
    pub const ERROR_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.ErrorInfo";
}
