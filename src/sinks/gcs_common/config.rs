use futures::FutureExt;
use http::{StatusCode, Uri};
use hyper::Body;
use snafu::Snafu;
use vector_lib::configurable::configurable_component;
use tokio::time::{interval, Duration};

use crate::{
    gcp::{GcpAuthenticator, GcpError},
    http::{HttpClient, HttpError},
    sinks::{
        gcs_common::service::GcsResponse,
        util::retries::{RetryAction, RetryLogic},
        Healthcheck, HealthcheckError,
    },
};

pub const BASE_URL: &str = "https://storage.googleapis.com/";

/// GCS Predefined ACLs.
///
/// For more information, see [Predefined ACLs][predefined_acls].
///
/// [predefined_acls]: https://cloud.google.com/storage/docs/access-control/lists#predefined-acl
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative)]
#[derivative(Default)]
#[serde(rename_all = "kebab-case")]
pub enum GcsPredefinedAcl {
    /// Bucket/object can be read by authenticated users.
    ///
    /// The bucket/object owner is granted the `OWNER` permission, and anyone authenticated Google
    /// account holder is granted the `READER` permission.
    AuthenticatedRead,

    /// Object is semi-private.
    ///
    /// Both the object owner and bucket owner are granted the `OWNER` permission.
    ///
    /// Only relevant when specified for an object: this predefined ACL is otherwise ignored when
    /// specified for a bucket.
    BucketOwnerFullControl,

    /// Object is private, except to the bucket owner.
    ///
    /// The object owner is granted the `OWNER` permission, and the bucket owner is granted the
    /// `READER` permission.
    ///
    /// Only relevant when specified for an object: this predefined ACL is otherwise ignored when
    /// specified for a bucket.
    BucketOwnerRead,

    /// Bucket/object are private.
    ///
    /// The bucket/object owner is granted the `OWNER` permission, and no one else has
    /// access.
    Private,

    /// Bucket/object are private within the project.
    ///
    /// Project owners and project editors are granted the `OWNER` permission, and anyone who is
    /// part of the project team is granted the `READER` permission.
    ///
    /// This is the default.
    #[derivative(Default)]
    ProjectPrivate,

    /// Bucket/object can be read publically.
    ///
    /// The bucket/object owner is granted the `OWNER` permission, and all other users, whether
    /// authenticated or anonymous, are granted the `READER` permission.
    PublicRead,
}

/// GCS storage classes.
///
/// For more information, see [Storage classes][storage_classes].
///
/// [storage_classes]: https://cloud.google.com/storage/docs/storage-classes
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative, PartialEq, Eq)]
#[derivative(Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GcsStorageClass {
    /// Standard storage.
    ///
    /// This is the default.
    #[derivative(Default)]
    Standard,

    /// Nearline storage.
    Nearline,

    /// Coldline storage.
    Coldline,

    /// Archive storage.
    Archive,
}

#[derive(Debug, Snafu)]
pub enum GcsError {
    #[snafu(display("Bucket {:?} not found", bucket))]
    BucketNotFound { bucket: String },
}

pub fn build_healthcheck(
    bucket: String,
    client: HttpClient,
    base_url: String,
    auth: GcpAuthenticator,
) -> crate::Result<Healthcheck> {
    let healthcheck = async move {
        let uri = base_url.parse::<Uri>()?;
        let mut num_retries = 0;
        let max_retries = 3;
        // repeat healthcheck every 5 sec
        let mut interval = interval(Duration::from_secs(5));
        let mut num_failures = 0;

        let not_found_error = GcsError::BucketNotFound { bucket }.into();

        loop {
            interval.tick().await;
            let mut request = http::Request::head(uri.clone()).body(Body::empty())?;

            auth.apply(&mut request);

            let response = client.send(request).await?;
            num_retries += 1;
            if response.status().is_success() {
                // the healthcheck passes on the first success
                return healthcheck_response(response, not_found_error);
            } else {
                // debug the healthcheck response
                warn!("healthcheck response was not successful! {:#?}", response);
                num_failures += 1;
            }

            if num_retries >= max_retries {
                info!("non-success healthcheck responses = {}", num_failures);
                info!("total healthcheck attempts = {}", num_retries);
                return healthcheck_response(response, not_found_error);
            }
        }
    };

    Ok(healthcheck.boxed())
}

pub fn healthcheck_response(
    response: http::Response<hyper::Body>,
    not_found_error: crate::Error,
) -> crate::Result<()> {
    match response.status() {
        StatusCode::OK => Ok(()),
        StatusCode::FORBIDDEN => Err(GcpError::HealthcheckForbidden.into()),
        StatusCode::NOT_FOUND => Err(not_found_error),
        status => Err(HealthcheckError::UnexpectedStatus { status }.into()),
    }
}

#[derive(Clone)]
pub struct GcsRetryLogic;

// This is a clone of HttpRetryLogic for the Body type, should get merged
impl RetryLogic for GcsRetryLogic {
    type Error = HttpError;
    type Response = GcsResponse;

    fn is_retriable_error(&self, _error: &Self::Error) -> bool {
        true
    }

    fn should_retry_response(&self, response: &Self::Response) -> RetryAction {
        let status = response.inner.status();

        match status {
            StatusCode::UNAUTHORIZED => RetryAction::Retry("unauthorized".into()),
            StatusCode::TOO_MANY_REQUESTS => RetryAction::Retry("too many requests".into()),
            StatusCode::NOT_IMPLEMENTED => {
                RetryAction::DontRetry("endpoint not implemented".into())
            }
            _ if status.is_server_error() => RetryAction::Retry(status.to_string().into()),
            _ if status.is_success() => RetryAction::Successful,
            _ => RetryAction::Retry(format!("catchall retry with response status: {}", status).into()),
        }
    }
}
