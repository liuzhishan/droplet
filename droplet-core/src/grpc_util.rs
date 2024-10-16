use prost::{Message, Name};
use tonic::{transport::Server, Code, Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};

/// Send error message of bad request for grpc request.
pub fn send_bad_request_error<T>(
    field: impl Into<String>,
    description: impl Into<String>,
) -> Result<Response<T>, Status> {
    let mut err_details = ErrorDetails::new();
    err_details.add_bad_request_violation(field, description);

    let status = Status::with_error_details(
        Code::InvalidArgument,
        "request cotains invalid argumetns",
        err_details,
    );

    return Err(status);
}

/// Send error message of internal error for grpc request.
pub fn send_error_message<T>(s: impl Into<String>) -> Result<Response<T>, Status> {
    let s1: String = s.into();

    let mut err_details = ErrorDetails::new();

    let metadata: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    err_details.set_error_info("error", s1.clone(), metadata);

    let status = Status::with_error_details(Code::Internal, s1.clone(), err_details);
    return Err(status);
}
