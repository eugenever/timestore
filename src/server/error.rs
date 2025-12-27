use axum::http::StatusCode;
use axum::response::{ IntoResponse, Response };

use super::query_filter::mongo::ToSqlError;

#[derive(Debug)]
pub enum ServerError {
    AnyError(anyhow::Error),
    IOError(std::io::Error),
    ToSqlError(ToSqlError),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let body = match self {
            ServerError::AnyError(err) => err.to_string(),
            ServerError::IOError(err) => err.to_string(),
            ServerError::ToSqlError(err) => err.to_string(),
        };
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}
