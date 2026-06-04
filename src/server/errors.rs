use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub ok: bool,
    pub message: String,
}

pub struct ApiError {
    status: StatusCode,
    error: anyhow::Error,
}

impl ApiError {
    pub fn new(status: StatusCode, error: impl Into<anyhow::Error>) -> Self {
        Self {
            status,
            error: error.into(),
        }
    }

    pub fn not_found(error: impl Into<anyhow::Error>) -> Self {
        Self::new(StatusCode::NOT_FOUND, error)
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, err)
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(ApiResponse {
            ok: false,
            message: format!("{:#}", self.error),
        });
        (self.status, body).into_response()
    }
}

pub fn ok(message: &str) -> ApiResponse {
    ApiResponse {
        ok: true,
        message: message.to_string(),
    }
}
