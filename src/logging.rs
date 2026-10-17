//! Structured logging. This is the only place that formats routing identity for logs.
//!
//! Logged: request ids, regions, nodes, models, placement reasons, gate results, sanitized error
//! codes, timings, and the first 12 hex characters of hashed session/account ids. Never logged:
//! headers (so never `Authorization` or the mesh key), bodies, prompt cache keys, or account ids.
use crate::store::{AttemptRecord, Event, RelayRecord};
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum LogFormat {
    Auto,
    Json,
    Pretty,
}

pub fn init(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let use_json = match format {
        LogFormat::Json => true,
        LogFormat::Pretty => false,
        LogFormat::Auto => !std::io::IsTerminal::is_terminal(&std::io::stdout()),
    };
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_target(false);
    if use_json {
        builder.json().init();
    } else {
        builder.pretty().init();
    }
}

/// First 12 characters of a hash, for correlation without the full value.
pub fn short(hash: Option<&str>) -> &str {
    hash.map(|h| &h[..h.len().min(12)]).unwrap_or("-")
}

/// @cc [owner:ghuntley,label:logging] attempt-log-fields
/// `log_attempt` MUST log only the fields of `AttemptRecord` that carry no secret: identifiers,
/// regions, node, model, reason, gate result, outcome, sanitized code, timings, token counts, and
/// at most 12 characters of the hashed session and account. Retried and failed attempts MUST log
/// at warn.
pub fn log_attempt(r: &AttemptRecord) {
    macro_rules! emit {
        ($level:ident) => {
            tracing::$level!(
                request_id = %r.request_id,
                attempt = r.attempt_no,
                model = %r.model,
                session = %short(r.session.as_deref()),
                account = %short(r.account.as_deref()),
                ingress = %r.ingress_region,
                egress = %r.egress_region,
                egress_node = %r.egress_node,
                domain = %r.failure_domain,
                colo = %r.cf_colo.as_deref().unwrap_or("-"),
                reason = %r.placement_reason,
                status = r.status,
                gate = %r.gate_result,
                outcome = %r.stream_outcome.as_deref().unwrap_or("-"),
                error_code = %r.error_code.as_deref().unwrap_or("-"),
                ttft_ms = r.ttft_ms,
                held_ms = r.held_ms,
                duration_ms = r.duration_ms,
                input_tokens = r.input_tokens,
                cached_input_tokens = r.cached_input_tokens,
                output_tokens = r.output_tokens,
                "attempt.finished"
            )
        };
    }
    if matches!(r.gate_result.as_str(), "retried" | "exhausted" | "abandoned")
        || matches!(r.stream_outcome.as_deref(), Some("failed" | "truncated" | "upstream_error"))
    {
        emit!(warn);
    } else {
        emit!(info);
    }
}

pub fn log_relay(r: &RelayRecord) {
    tracing::info!(
        request_id = %r.request_id,
        from_node = %r.from_node,
        from_region = %r.from_region,
        status = r.status,
        duration_ms = r.duration_ms,
        bytes = r.bytes,
        headers_ms = r.headers_ms,
        colo = %r.cf_colo.as_deref().unwrap_or("-"),
        error = %r.error.as_deref().unwrap_or("-"),
        "relay.finished"
    );
}

pub fn log_event(e: &Event) {
    tracing::info!(
        kind = %e.kind,
        region = %e.region.as_deref().unwrap_or("-"),
        model = %e.model.as_deref().unwrap_or("-"),
        domain = %e.domain.as_deref().unwrap_or("-"),
        from = %e.from_region.as_deref().unwrap_or("-"),
        to = %e.to_region.as_deref().unwrap_or("-"),
        request_id = %e.request_id.as_deref().unwrap_or("-"),
        detail = %e.detail.as_deref().unwrap_or("-"),
        "mesh.event"
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn short_truncates() {
        assert_eq!(super::short(Some("0123456789abcdef")), "0123456789ab");
        assert_eq!(super::short(Some("ab")), "ab");
        assert_eq!(super::short(None), "-");
    }
}
