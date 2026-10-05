//! `tonic::Status` → `AutumnError`, with a fixed code map (ADR 0009).

use autumn_web::AutumnError;
use http::StatusCode;
use tonic::{Code, Status};

/// The HTTP status for a gRPC code.
///
/// | gRPC code | HTTP |
/// |---|---|
/// | `InvalidArgument`, `OutOfRange`, `FailedPrecondition` | 400 |
/// | `Unauthenticated` | 401 |
/// | `PermissionDenied` | 403 |
/// | `NotFound` | 404 |
/// | `AlreadyExists`, `Aborted` | 409 |
/// | `ResourceExhausted` | 429 |
/// | `Unavailable` | 503 |
/// | `DeadlineExceeded` | 504 |
/// | all others | 502 |
#[must_use]
pub const fn http_status(code: Code) -> StatusCode {
    match code {
        Code::InvalidArgument | Code::OutOfRange | Code::FailedPrecondition => {
            StatusCode::BAD_REQUEST
        }
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::AlreadyExists | Code::Aborted => StatusCode::CONFLICT,
        Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        Code::Ok
        | Code::Cancelled
        | Code::Unknown
        | Code::Unimplemented
        | Code::Internal
        | Code::DataLoss => StatusCode::BAD_GATEWAY,
    }
}

/// The fixed text for a 4xx status. The downstream message does not go to
/// the HTTP client.
const fn client_text(code: Code) -> &'static str {
    match code {
        Code::InvalidArgument => "invalid argument",
        Code::OutOfRange => "out of range",
        Code::FailedPrecondition => "failed precondition",
        Code::Unauthenticated => "unauthenticated",
        Code::PermissionDenied => "permission denied",
        Code::NotFound => "not found",
        Code::AlreadyExists => "already exists",
        Code::Aborted => "aborted",
        Code::ResourceExhausted => "resource exhausted",
        _ => "downstream gRPC call failed",
    }
}

/// The error text of a failed call.
#[derive(Debug)]
struct CallFailed(String);

impl std::fmt::Display for CallFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CallFailed {}

/// Convert a failed call to an [`AutumnError`] with the status from
/// [`http_status`].
///
/// - 4xx: a fixed text for the code. The downstream message stays out of
///   the response in all profiles.
/// - 5xx: the code and the downstream message. Autumn shows 5xx details
///   only in `dev`.
///
/// The log gets the full status in all cases.
#[must_use]
pub fn status_to_error(status: &Status) -> AutumnError {
    let code = status.code();
    let http = http_status(code);
    let name = crate::metrics::code_name(code as i32);
    let text = if http.is_server_error() {
        tracing::warn!(
            grpc_code = name,
            message = status.message(),
            "downstream gRPC call failed"
        );
        format!("downstream gRPC call failed: {name}: {}", status.message())
    } else {
        tracing::debug!(
            grpc_code = name,
            message = status.message(),
            "downstream gRPC call failed"
        );
        client_text(code).to_owned()
    };
    AutumnError::from(CallFailed(text)).with_status(http)
}

/// `.or_http()` on the result of a client call.
///
/// A bare `?` on `tonic::Status` gives a 500: Autumn converts every error
/// type to 500, and Rust does not let this crate add `From<Status>`. Use
/// `.or_http()?` to get the code map.
///
/// ```rust,ignore
/// let reply = billing.get_invoice(request).await.or_http()?;
/// ```
pub trait GrpcResultExt<T> {
    /// Map an error with [`status_to_error`].
    ///
    /// # Errors
    ///
    /// The mapped [`AutumnError`] when the call failed.
    fn or_http(self) -> Result<T, AutumnError>;
}

impl<T> GrpcResultExt<T> for Result<T, Status> {
    fn or_http(self) -> Result<T, AutumnError> {
        self.map_err(|status| status_to_error(&status))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_a_fixed_status() {
        let expected = [
            (Code::Ok, 502),
            (Code::Cancelled, 502),
            (Code::Unknown, 502),
            (Code::InvalidArgument, 400),
            (Code::DeadlineExceeded, 504),
            (Code::NotFound, 404),
            (Code::AlreadyExists, 409),
            (Code::PermissionDenied, 403),
            (Code::ResourceExhausted, 429),
            (Code::FailedPrecondition, 400),
            (Code::Aborted, 409),
            (Code::OutOfRange, 400),
            (Code::Unimplemented, 502),
            (Code::Internal, 502),
            (Code::Unavailable, 503),
            (Code::DataLoss, 502),
            (Code::Unauthenticated, 401),
        ];
        assert_eq!(expected.len(), 17, "all codes");
        for (code, status) in expected {
            assert_eq!(http_status(code).as_u16(), status, "{code:?}");
            let error = status_to_error(&Status::new(code, "secret"));
            assert_eq!(error.status().as_u16(), status, "{code:?}");
            let message = error.message();
            if status < 500 {
                assert!(!message.contains("secret"), "{code:?}: {message}");
            } else {
                assert!(message.contains("secret"), "{code:?}: {message}");
            }
        }
    }

    #[test]
    fn or_http_passes_ok_values_through() {
        assert_eq!(Ok::<_, Status>(5).or_http().unwrap_or_default(), 5);
        let error = Err::<(), _>(Status::not_found("x")).or_http().unwrap_err();
        assert_eq!(error.status(), StatusCode::NOT_FOUND);
        assert_eq!(error.message(), "not found");
    }
}
