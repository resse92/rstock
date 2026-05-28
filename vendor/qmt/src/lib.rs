pub mod common {
    tonic::include_proto!("qmt.common");
}

pub mod data {
    tonic::include_proto!("qmt.data");
}

pub mod health {
    tonic::include_proto!("qmt.health");
}

pub mod trading {
    tonic::include_proto!("qmt.trading");
}

use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint, Error as TransportError};
use tonic::{Request, Status};

#[derive(Debug)]
pub enum QmtClientError {
    Transport(TransportError),
    InvalidAuthorization(tonic::metadata::errors::InvalidMetadataValue),
}

impl std::fmt::Display for QmtClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(err) => write!(f, "transport error: {err}"),
            Self::InvalidAuthorization(err) => write!(f, "invalid authorization metadata: {err}"),
        }
    }
}

impl std::error::Error for QmtClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(err) => Some(err),
            Self::InvalidAuthorization(err) => Some(err),
        }
    }
}

impl From<TransportError> for QmtClientError {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}

impl From<tonic::metadata::errors::InvalidMetadataValue> for QmtClientError {
    fn from(value: tonic::metadata::errors::InvalidMetadataValue) -> Self {
        Self::InvalidAuthorization(value)
    }
}

#[derive(Debug, Clone)]
pub struct AuthInterceptor {
    authorization: Option<MetadataValue<Ascii>>,
}

impl AuthInterceptor {
    pub fn new(authorization: Option<impl Into<String>>) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        let authorization = authorization
            .map(|value| normalize_authorization(value.into()))
            .transpose()?;
        Ok(Self { authorization })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(authorization) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
        }
        Ok(request)
    }
}

pub type InterceptedChannel = InterceptedService<Channel, AuthInterceptor>;
pub type HealthGrpcClient = health::health_client::HealthClient<InterceptedChannel>;
pub type DataGrpcClient = data::data_service_client::DataServiceClient<InterceptedChannel>;
pub type TradingGrpcClient = trading::trading_service_client::TradingServiceClient<InterceptedChannel>;

#[derive(Debug, Clone)]
pub struct QmtClient {
    channel: Channel,
    interceptor: AuthInterceptor,
}

impl QmtClient {
    pub async fn connect(dst: impl AsRef<str>) -> Result<Self, QmtClientError> {
        Self::connect_with_authorization(dst, Option::<String>::None).await
    }

    pub async fn connect_with_authorization(
        dst: impl AsRef<str>,
        authorization: Option<impl Into<String>>,
    ) -> Result<Self, QmtClientError> {
        let endpoint = Endpoint::from_shared(dst.as_ref().to_string())?;
        Self::connect_endpoint_with_authorization(endpoint, authorization).await
    }

    pub fn connect_lazy(dst: impl AsRef<str>) -> Result<Self, QmtClientError> {
        Self::connect_lazy_with_authorization(dst, Option::<String>::None)
    }

    pub fn connect_lazy_with_authorization(
        dst: impl AsRef<str>,
        authorization: Option<impl Into<String>>,
    ) -> Result<Self, QmtClientError> {
        let endpoint = Endpoint::from_shared(dst.as_ref().to_string())?;
        Self::from_endpoint_with_authorization(endpoint, authorization)
    }

    pub async fn connect_endpoint(endpoint: Endpoint) -> Result<Self, QmtClientError> {
        Self::connect_endpoint_with_authorization(endpoint, Option::<String>::None).await
    }

    pub async fn connect_endpoint_with_authorization(
        endpoint: Endpoint,
        authorization: Option<impl Into<String>>,
    ) -> Result<Self, QmtClientError> {
        let channel = endpoint.connect().await?;
        Self::from_channel_with_authorization(channel, authorization)
    }

    pub fn from_endpoint(endpoint: Endpoint) -> Result<Self, QmtClientError> {
        Self::from_endpoint_with_authorization(endpoint, Option::<String>::None)
    }

    pub fn from_endpoint_with_authorization(
        endpoint: Endpoint,
        authorization: Option<impl Into<String>>,
    ) -> Result<Self, QmtClientError> {
        let channel = endpoint.connect_lazy();
        Self::from_channel_with_authorization(channel, authorization)
    }

    pub fn from_channel(channel: Channel) -> Result<Self, QmtClientError> {
        Self::from_channel_with_authorization(channel, Option::<String>::None)
    }

    pub fn from_channel_with_authorization(
        channel: Channel,
        authorization: Option<impl Into<String>>,
    ) -> Result<Self, QmtClientError> {
        let interceptor = AuthInterceptor::new(authorization)?;
        Ok(Self {
            channel,
            interceptor,
        })
    }

    pub fn health(&self) -> HealthGrpcClient {
        health::health_client::HealthClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn data(&self) -> DataGrpcClient {
        data::data_service_client::DataServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn trading(&self) -> TradingGrpcClient {
        trading::trading_service_client::TradingServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }
}

fn normalize_authorization(
    value: String,
) -> Result<MetadataValue<Ascii>, tonic::metadata::errors::InvalidMetadataValue> {
    let value = value.trim();
    let value = if value.to_ascii_lowercase().starts_with("bearer ") {
        value.to_string()
    } else {
        format!("Bearer {value}")
    };
    MetadataValue::try_from(value)
}
