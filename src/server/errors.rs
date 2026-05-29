use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub ok: bool,
    pub message: String,
}

pub struct ApiError(pub anyhow::Error);

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(ApiResponse {
            ok: false,
            message: format!("{:#}", self.0),
        });
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}

pub fn ok(message: &str) -> ApiResponse {
    ApiResponse {
        ok: true,
        message: message.to_string(),
    }
}
