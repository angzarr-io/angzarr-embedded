//! Writes the gRPC client and server stubs for the services a remote
//! component serves (angzarr-project's command_handler.proto,
//! component.proto, policy.proto and projector.proto, package `io.angzarr.v1`), with tonic-build's manual
//! builder: angzarr-proto compiles no services and ships no service protos,
//! so the stubs are written from the Rust types instead, and those are
//! angzarr-router's `pb` (angzarr-proto's `v1` re-exported, plus the
//! service messages the router generates against it). No `io.angzarr.v1`
//! message is generated here, so none exists twice.
//!
//! `DescribeRequest` is the one message neither crate generates; it is empty,
//! so it is `crate::rpc::DescribeRequest`, an alias of `()`, which prost
//! encodes as the empty message it is.

use tonic_build::manual::{Builder, Method, Service};

const CODEC: &str = "tonic_prost::ProstCodec";

fn main() {
    let command_handler = Service::builder()
        .name("CommandHandlerService")
        .package("io.angzarr.v1")
        .method(
            Method::builder()
                .name("handle")
                .route_name("Handle")
                .input_type("::angzarr_router::pb::ContextualCommand")
                .output_type("::angzarr_router::pb::CommandHandlerResponse")
                .codec_path(CODEC)
                .build(),
        )
        .build();
    let component = Service::builder()
        .name("ComponentService")
        .package("io.angzarr.v1")
        .method(
            Method::builder()
                .name("describe")
                .route_name("Describe")
                .input_type("crate::rpc::DescribeRequest")
                .output_type("::angzarr_router::pb::ComponentDescriptor")
                .codec_path(CODEC)
                .build(),
        )
        .build();
    let policy = Service::builder()
        .name("PolicyService")
        .package("io.angzarr.v1")
        .method(
            Method::builder()
                .name("handle")
                .route_name("Handle")
                .input_type("::angzarr_router::pb::EventBook")
                .output_type("::angzarr_router::pb::PolicyResponse")
                .codec_path(CODEC)
                .build(),
        )
        .method(
            Method::builder()
                .name("on_refused")
                .route_name("OnRefused")
                .input_type("::angzarr_router::pb::Refusal")
                .output_type("::angzarr_router::pb::OnRefusedResponse")
                .codec_path(CODEC)
                .build(),
        )
        .build();
    let projector = Service::builder()
        .name("ProjectorService")
        .package("io.angzarr.v1")
        .method(
            Method::builder()
                .name("handle")
                .route_name("Handle")
                .input_type("::angzarr_router::pb::EventBook")
                .output_type("::angzarr_router::pb::Projection")
                .codec_path(CODEC)
                .build(),
        )
        .build();
    Builder::new()
        .build_client(true)
        .build_server(true)
        .build_transport(true)
        .compile(&[command_handler, component, policy, projector]);
}
