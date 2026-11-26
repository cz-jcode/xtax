//! Shared OpenDAL test operators and blob store factories.
#![allow(dead_code)]

use std::sync::Arc;

use futures_util::TryStreamExt;
use opendal::{
    Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter, Operator,
    services::{Fs, Memory, S3},
};
use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use xtax_blob_storage::{BlobStore, BlobStoreBuilder};

pub fn memory_operator() -> Operator {
    Operator::new(Memory::default()).unwrap()
}

pub fn filesystem_operator() -> Operator {
    let root = tempfile::tempdir().unwrap().keep();
    Operator::new(Fs::default().root(root.to_str().unwrap())).unwrap()
}

pub fn s3_operator() -> Operator {
    const ACCESS_KEY: &str = "test-access-key";
    const SECRET_KEY: &str = "test-secret-key";
    const BUCKET: &str = "opendal-test";

    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join(BUCKET)).unwrap();
    let filesystem = s3s_fs::FileSystem::new(root.path()).unwrap();
    let mut service = S3ServiceBuilder::new(filesystem);
    service.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));

    let operator = Operator::new(
        S3::default()
            .bucket(BUCKET)
            .region("us-east-1")
            .endpoint("http://s3.example.test")
            .access_key_id(ACCESS_KEY)
            .secret_access_key(SECRET_KEY)
            .disable_config_load(),
    )
    .unwrap();
    let transport = HttpTransporter::new(MockS3Transport {
        service: service.build(),
        _root: root,
    });
    let context = operator.context().with_http_transport(transport);
    operator.with_context(context)
}

pub async fn memory_store() -> Arc<dyn BlobStore> {
    build_store(memory_operator()).await
}

pub async fn filesystem_store() -> Arc<dyn BlobStore> {
    build_store(filesystem_operator()).await
}

pub async fn s3_store() -> Arc<dyn BlobStore> {
    build_store(s3_operator()).await
}

async fn build_store(operator: Operator) -> Arc<dyn BlobStore> {
    BlobStoreBuilder::new()
        .with_opendal(operator)
        .build()
        .await
        .unwrap()
}

// Exercise the actual S3 request/response path without a socket or live credentials.
struct MockS3Transport {
    service: S3Service,
    _root: tempfile::TempDir,
}

impl HttpTransport for MockS3Transport {
    async fn fetch(
        &self,
        mut request: s3s::HttpRequest<Buffer>,
    ) -> opendal::Result<s3s::HttpResponse<HttpBody>> {
        let host = request.uri().authority().unwrap().as_str().parse().unwrap();
        request.headers_mut().entry("host").or_insert(host);
        let is_head = request.method() == "HEAD";
        let response = self
            .service
            .call(request.map(|body| body.to_bytes().into()))
            .await
            .map_err(|error| {
                Error::new(ErrorKind::Unexpected, "mock S3 request failed").set_source(error)
            })?;
        if is_head {
            return Ok(response.map(|_| HttpBody::new(futures_util::stream::empty(), Some(0))));
        }
        let size = response
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        Ok(response.map(|body| {
            HttpBody::new(
                body.map_ok(Buffer::from).map_err(|error| {
                    Error::new(ErrorKind::Unexpected, "mock S3 response failed")
                        .set_source(std::io::Error::other(error))
                }),
                size,
            )
        }))
    }
}
