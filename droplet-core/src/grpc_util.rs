use mysql::PooledConn;
use tonic::{Code, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};

use crate::db::db::DB;

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

/// Get error status from string message.
pub fn get_error_status(s: impl Into<String>) -> Status {
    let s1: String = s.into();

    let mut err_details = ErrorDetails::new();
    let metadata: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    err_details.set_error_info("error", s1.clone(), metadata);

    let status = Status::with_error_details(Code::Internal, s1.clone(), err_details);

    status
}

/// Send error message of internal error for grpc request.
pub fn send_error_message<T>(s: impl Into<String>) -> Result<Response<T>, Status> {
    Err(get_error_status(s))
}

pub fn get_db_conn(db: &DB) -> Result<PooledConn, Status> {
    match db.get_conn() {
        Ok(conn) => Ok(conn),
        Err(e) => Err(get_error_status(format!(
            "Failed to get db connection: {}",
            e
        ))),
    }
}

/// Print error message and send error status.
///
/// For better logging.
#[macro_export]
macro_rules! print_and_send_error_status {
    ($msg:literal $(,)?) => {
        error!("{}", $msg);
        return get_error_status($msg)
    };
    ($err:expr $(,)?) => {
        error!("{}", $err);
        return get_error_status($err)
    };
    ($fmt:expr, $($arg:tt)*) => {
        error!($fmt, $($arg)*);
        return get_error_status(format!($fmt, $($arg)*))
    };
}
