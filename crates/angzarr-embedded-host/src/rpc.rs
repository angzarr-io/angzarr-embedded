//! The gRPC stubs for the two services a remote domain serves, written by
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
#[allow(dead_code)] // red: used by the supervisor
pub(crate) async fn connect_unix(socket: PathBuf) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_static("http://[::]:0")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(socket).await?)) }
        }))
        .await
}
