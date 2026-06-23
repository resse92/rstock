use chrono::Utc;
use serde_json::{Map, Number, Value};
use std::fmt;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, Writer};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

const SERVICE_NAME: &str = "rstock";
const DEFAULT_LOG_FILTER: &str = "info,rstock=info,rstock_jobs=info";

pub fn init_logging() {
    tracing_subscriber::fmt()
        .event_format(VictoriaLogsJsonFormatter)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER)),
        )
        .init();
}

struct VictoriaLogsJsonFormatter;

impl<S, N> FormatEvent<S, N> for VictoriaLogsJsonFormatter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut visitor = JsonFieldVisitor::default();
        event.record(&mut visitor);

        let mut log = Map::new();
        log.insert("_time".to_string(), Value::String(Utc::now().to_rfc3339()));
        log.insert(
            "_msg".to_string(),
            Value::String(visitor.message.unwrap_or_else(|| meta.name().to_string())),
        );
        log.insert("level".to_string(), Value::String(meta.level().to_string()));
        log.insert(
            "target".to_string(),
            Value::String(meta.target().to_string()),
        );
        log.insert(
            "service".to_string(),
            Value::String(SERVICE_NAME.to_string()),
        );
        log.extend(visitor.fields);

        let line = serde_json::to_string(&Value::Object(log)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

#[derive(Default)]
struct JsonFieldVisitor {
    message: Option<String>,
    fields: Map<String, Value>,
}

impl JsonFieldVisitor {
    fn record_value(&mut self, field: &Field, value: Value) {
        if field.name() == "message" {
            self.message = Some(match value {
                Value::String(value) => value,
                other => other.to_string(),
            });
            return;
        }
        self.fields.insert(field.name().to_string(), value);
    }

    fn debug_string(value: &dyn fmt::Debug) -> String {
        let value = format!("{value:?}");
        serde_json::from_str::<String>(&value).unwrap_or(value)
    }
}

impl Visit for JsonFieldVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, Value::Number(value.into()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, Value::Bool(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, Value::String(value.to_string()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let value = Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null);
        self.record_value(field, value);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record_value(field, Value::String(value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(field, Value::String(Self::debug_string(value)));
    }
}
