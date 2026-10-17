//! Server-sent-event observation shared by the gate and statistics: token usage, how a body
//! ended, the first error code, and whether model output appeared. Adapted from underclass
//! `usage.rs` (ADR 0021 there) with cached-input accounting added.
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenCounts {
    pub input_tokens: i64,
    pub output_tokens: i64,
    /// Input tokens served from the upstream prompt cache (0 when not reported).
    #[serde(default)]
    pub cached_input_tokens: i64,
}

fn pair(value: &Value, input: &str, output: &str, details: &str) -> Option<TokenCounts> {
    let input_tokens = value.get(input)?.as_i64()?;
    let output_tokens = value.get(output)?.as_i64()?;
    let cached_input_tokens = value
        .get(details)
        .and_then(|d| d.get("cached_tokens"))
        .and_then(Value::as_i64)
        .filter(|c| (0..=input_tokens).contains(c))
        .unwrap_or(0);
    (input_tokens >= 0 && output_tokens >= 0).then_some(TokenCounts {
        input_tokens,
        output_tokens,
        cached_input_tokens,
    })
}

pub fn usage_from_json(value: &Value, chat: bool) -> Option<TokenCounts> {
    let usage = value.get("usage")?;
    if chat {
        pair(usage, "prompt_tokens", "completion_tokens", "prompt_tokens_details")
    } else {
        pair(usage, "input_tokens", "output_tokens", "input_tokens_details")
    }
}

/// How an upstream response body ended, as observed by the proxy while passing it through.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamOutcome {
    /// `response.completed`, chat `[DONE]`, or a successful non-stream body.
    Completed,
    /// `response.failed`, a stream `error` event, or an error object in the body.
    Failed,
    /// `response.incomplete`.
    Incomplete,
    /// Upstream closed the stream without a terminal event.
    Truncated,
    /// The upstream body errored mid-transfer.
    UpstreamError,
    /// The client went away before the stream ended.
    ClientClosed,
}

impl StreamOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Incomplete => "incomplete",
            Self::Truncated => "truncated",
            Self::UpstreamError => "upstream_error",
            Self::ClientClosed => "client_closed",
        }
    }
}

/// Why the proxy stopped reading an upstream body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEnd {
    Eof,
    UpstreamError,
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamReport {
    pub outcome: StreamOutcome,
    pub error_code: Option<String>,
    /// Whether model output arrived before the first terminal event.
    pub saw_output: bool,
}

const MAX_CODE_LEN: usize = 64;

/// @cc [owner:ghuntley,label:security] error-code-sanitized
/// `sanitize_code` MUST return only ASCII alphanumerics, `_`, `-`, and `.`, at most 64 bytes, and
/// `None` when nothing remains; upstream error codes pass through it before they are logged or
/// stored, so free-form upstream `message` text can never reach logs or the database.
pub fn sanitize_code(raw: &str) -> Option<String> {
    let code: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        .take(MAX_CODE_LEN)
        .collect();
    (!code.is_empty()).then_some(code)
}

/// Extracts `code`, falling back to `type`, from an upstream error object.
pub fn error_code(error: &Value) -> Option<String> {
    error
        .get("code")
        .and_then(Value::as_str)
        .or_else(|| error.get("type").and_then(Value::as_str))
        .and_then(sanitize_code)
}

/// Whether an upstream error code means the model is out of serving capacity.
pub fn is_overload_code(code: &str) -> bool {
    let code = code.to_ascii_lowercase();
    code == "slow_down" || code.contains("overload") || code.contains("capacity")
}

/// Whether an error code is a capacity failure that another region may not share. This is the
/// only class of upstream failure GeoClass retries elsewhere.
pub fn is_retryable_code(code: Option<&str>) -> bool {
    code.is_some_and(is_overload_code)
}

/// @cc [owner:ghuntley,label:accounting] upstream-event-kind
/// `event_kind` MUST map an upstream error code to exactly one stats kind: `overloaded` for
/// capacity codes, `policy_blocked` for policy/safety refusals (e.g. `cyber_policy`),
/// `quota_exhausted` for usage-limit codes, and `upstream_failed` otherwise.
pub fn event_kind(code: Option<&str>) -> &'static str {
    let Some(code) = code else { return "upstream_failed" };
    let lower = code.to_ascii_lowercase();
    if is_overload_code(&lower) {
        "overloaded"
    } else if lower.contains("policy") || lower.contains("content_filter") || lower.contains("safety") {
        "policy_blocked"
    } else if lower.contains("usage_limit") || lower.contains("quota") || lower.contains("usage_not_included") {
        "quota_exhausted"
    } else {
        "upstream_failed"
    }
}

/// @cc [owner:ghuntley,label:accounting] usage-tap-bounded
/// The tap MUST retain at most one megabyte of upstream data while scanning and MUST count only
/// non-negative upstream-reported token pairs; absent, malformed, or incomplete usage is unknown.
pub struct UsageTap {
    stream: bool,
    chat: bool,
    buffer: Vec<u8>,
    event_data: Vec<u8>,
    event_name: Option<String>,
    overflow: bool,
    counts: Option<TokenCounts>,
    terminal: Option<(StreamOutcome, Option<String>)>,
    saw_output: bool,
}

impl UsageTap {
    pub fn new(stream: bool, chat: bool) -> Self {
        Self {
            stream,
            chat,
            buffer: Vec::new(),
            event_data: Vec::new(),
            event_name: None,
            overflow: false,
            counts: None,
            terminal: None,
            saw_output: false,
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        if !self.stream {
            if self.overflow {
                return;
            }
            if self.buffer.len().saturating_add(chunk.len()) <= MAX_CAPTURE_BYTES {
                self.buffer.extend_from_slice(chunk);
            } else {
                self.overflow = true;
                self.buffer.clear();
            }
            return;
        }
        for &byte in chunk {
            if byte == b'\n' {
                let line = std::mem::take(&mut self.buffer);
                let line = line.strip_suffix(b"\r").unwrap_or(&line);
                if line.is_empty() {
                    self.finish_event();
                } else if !self.overflow && line.starts_with(b"event:") {
                    let name = line[6..].strip_prefix(b" ").unwrap_or(&line[6..]);
                    self.event_name = std::str::from_utf8(name)
                        .ok()
                        .filter(|name| name.len() <= MAX_CODE_LEN)
                        .map(str::to_string);
                } else if !self.overflow && line.starts_with(b"data:") {
                    let data = line[5..].strip_prefix(b" ").unwrap_or(&line[5..]);
                    let separator = usize::from(!self.event_data.is_empty());
                    if self
                        .event_data
                        .len()
                        .saturating_add(separator)
                        .saturating_add(data.len())
                        <= MAX_CAPTURE_BYTES
                    {
                        if separator != 0 {
                            self.event_data.push(b'\n');
                        }
                        self.event_data.extend_from_slice(data);
                    } else {
                        self.overflow = true;
                        self.event_data.clear();
                    }
                }
            } else if !self.overflow {
                if self.buffer.len().saturating_add(self.event_data.len()) < MAX_CAPTURE_BYTES {
                    self.buffer.push(byte);
                } else {
                    self.overflow = true;
                    self.buffer.clear();
                    self.buffer.push(b'!');
                }
            } else if self.buffer.is_empty() {
                self.buffer.push(b'!');
            }
        }
    }

    fn finish_event(&mut self) {
        let parsed = if self.overflow {
            None
        } else {
            serde_json::from_slice::<Value>(&self.event_data).ok()
        };
        if let Some(value) = &parsed {
            let counts = if self.chat {
                usage_from_json(value, true)
            } else if value.get("type").and_then(Value::as_str) == Some("response.completed") {
                value
                    .get("response")
                    .and_then(|response| usage_from_json(response, false))
            } else {
                None
            };
            if counts.is_some() {
                self.counts = counts;
            }
        }
        if self.chat {
            if !self.overflow && self.event_data.trim_ascii() == b"[DONE]" {
                self.set_terminal(StreamOutcome::Completed, None);
            } else if let Some(value) = &parsed {
                if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
                    self.set_terminal(StreamOutcome::Failed, error_code(error));
                } else if self.terminal.is_none()
                    && value
                        .get("choices")
                        .and_then(Value::as_array)
                        .is_some_and(|choices| !choices.is_empty())
                {
                    self.saw_output = true;
                }
            }
        } else {
            // The SSE `event:` name survives even when an oversized data payload was dropped.
            let kind = parsed
                .as_ref()
                .and_then(|v| v.get("type"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| self.event_name.clone());
            let response = parsed.as_ref().and_then(|v| v.get("response"));
            match kind.as_deref() {
                Some("response.completed") => self.set_terminal(StreamOutcome::Completed, None),
                Some("response.failed") => {
                    let code = response.and_then(|r| r.get("error")).and_then(error_code);
                    self.set_terminal(StreamOutcome::Failed, code);
                }
                Some("response.incomplete") => {
                    let reason = response
                        .and_then(|r| r.get("incomplete_details"))
                        .and_then(|d| d.get("reason"))
                        .and_then(Value::as_str)
                        .and_then(sanitize_code);
                    self.set_terminal(StreamOutcome::Incomplete, reason);
                }
                Some("error") => {
                    let code = parsed.as_ref().and_then(|v| {
                        error_code(v).or_else(|| v.get("error").and_then(error_code))
                    });
                    self.set_terminal(StreamOutcome::Failed, code);
                }
                Some(kind) if (kind == "response.output_item.added" || kind.ends_with(".delta")) && self.terminal.is_none() => {
                    self.saw_output = true;
                }
                _ => {}
            }
        }
        self.event_data.clear();
        self.event_name = None;
        self.overflow = false;
    }

    fn set_terminal(&mut self, outcome: StreamOutcome, code: Option<String>) {
        if self.terminal.is_none() {
            self.terminal = Some((outcome, code));
        }
    }

    /// @cc [owner:ghuntley,label:accounting] stream-outcome-classified
    /// `report` MUST prefer the first terminal event seen in the body (`response.completed`,
    /// `response.failed`, `response.incomplete`, stream `error`, chat `[DONE]`) over how the
    /// transfer ended; without one, a streamed body MUST be `truncated` on EOF, `upstream_error` on a
    /// transport error, and `client_closed` when dropped. The result MUST NOT depend on how the body
    /// was split into chunks.
    pub fn report(&self, end: StreamEnd) -> StreamReport {
        let (outcome, error_code) = if let Some((outcome, code)) = &self.terminal {
            (*outcome, code.clone())
        } else {
            match end {
                StreamEnd::Eof if !self.stream => {
                    let error = (!self.overflow)
                        .then(|| serde_json::from_slice::<Value>(&self.buffer).ok())
                        .flatten()
                        .and_then(|v| v.get("error").filter(|e| !e.is_null()).cloned());
                    match error {
                        Some(error) => (StreamOutcome::Failed, error_code(&error)),
                        None => (StreamOutcome::Completed, None),
                    }
                }
                StreamEnd::Eof => (StreamOutcome::Truncated, None),
                StreamEnd::UpstreamError => (StreamOutcome::UpstreamError, None),
                StreamEnd::Dropped => (StreamOutcome::ClientClosed, None),
            }
        };
        StreamReport { outcome, error_code, saw_output: self.saw_output }
    }

    /// Whether model output (a delta, an added output item, or chat choices) arrived before any
    /// terminal event.
    pub fn saw_output(&self) -> bool {
        self.saw_output
    }

    /// The first terminal event seen so far, if any.
    pub fn terminal(&self) -> Option<(StreamOutcome, Option<&str>)> {
        self.terminal.as_ref().map(|(o, c)| (*o, c.as_deref()))
    }

    pub fn is_stream(&self) -> bool {
        self.stream
    }

    pub fn counts(&self) -> Option<TokenCounts> {
        if self.stream {
            self.counts
        } else if self.overflow {
            None
        } else {
            serde_json::from_slice::<Value>(&self.buffer)
                .ok()
                .and_then(|v| usage_from_json(&v, self.chat))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_stream_across_chunks() {
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"event: response.completed\r\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":12,");
        tap.feed(b"\"output_tokens\":4}}}\r\n\r\n");
        assert_eq!(
            tap.counts(),
            Some(TokenCounts {
                input_tokens: 12,
                output_tokens: 4,
                cached_input_tokens: 0,
            })
        );
    }

    #[test]
    fn cached_input_tokens_are_read_and_bounded() {
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":100,\"output_tokens\":4,\"input_tokens_details\":{\"cached_tokens\":80}}}}\n\n");
        assert_eq!(tap.counts().unwrap().cached_input_tokens, 80);
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":4,\"input_tokens_details\":{\"cached_tokens\":80}}}}\n\n");
        assert_eq!(tap.counts().unwrap().cached_input_tokens, 0);
        assert!(is_retryable_code(Some("server_is_overloaded")));
        assert!(!is_retryable_code(Some("usage_limit_reached")));
        assert!(!is_retryable_code(None));
    }

    #[test]
    fn chat_ignores_null_and_negative_usage() {
        let mut tap = UsageTap::new(true, true);
        tap.feed(b"data: {\"usage\":null}\n\ndata: {\"usage\":{\"prompt_tokens\":-1,\"completion_tokens\":2}}\n\n");
        assert_eq!(tap.counts(), None);
        tap.feed(b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n");
        assert_eq!(
            tap.counts(),
            Some(TokenCounts {
                input_tokens: 5,
                output_tokens: 2,
                cached_input_tokens: 0,
            })
        );
    }

    #[test]
    fn incomplete_stream_is_unknown() {
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n");
        assert_eq!(tap.counts(), None);
    }

    #[test]
    fn overloaded_failure_before_output() {
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n");
        tap.feed(b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity.\"}}}\n\n");
        assert_eq!(
            tap.report(StreamEnd::Eof),
            StreamReport {
                outcome: StreamOutcome::Failed,
                error_code: Some("server_is_overloaded".into()),
                saw_output: false
            }
        );
    }

    #[test]
    fn failure_after_output_records_saw_output() {
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n");
        tap.feed(b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\"}}}\n\n");
        let report = tap.report(StreamEnd::Dropped);
        assert_eq!(report.outcome, StreamOutcome::Failed);
        assert_eq!(report.error_code.as_deref(), Some("server_error"));
        assert!(report.saw_output);
    }

    #[test]
    fn incomplete_reports_reason() {
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n");
        let report = tap.report(StreamEnd::Eof);
        assert_eq!(report.outcome, StreamOutcome::Incomplete);
        assert_eq!(report.error_code.as_deref(), Some("max_output_tokens"));
    }

    #[test]
    fn missing_terminal_event_depends_on_how_stream_ended() {
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n");
        assert_eq!(tap.report(StreamEnd::Eof).outcome, StreamOutcome::Truncated);
        assert_eq!(tap.report(StreamEnd::UpstreamError).outcome, StreamOutcome::UpstreamError);
        assert_eq!(tap.report(StreamEnd::Dropped).outcome, StreamOutcome::ClientClosed);
    }

    #[test]
    fn chat_done_and_chat_error() {
        let mut tap = UsageTap::new(true, true);
        tap.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n");
        let report = tap.report(StreamEnd::Eof);
        assert_eq!((report.outcome, report.saw_output), (StreamOutcome::Completed, true));
        let mut tap = UsageTap::new(true, true);
        tap.feed(b"data: {\"error\":{\"code\":\"model_overloaded\",\"message\":\"busy\"}}\n\n");
        assert_eq!(tap.report(StreamEnd::Eof).error_code.as_deref(), Some("model_overloaded"));
    }

    #[test]
    fn oversized_terminal_event_still_classified_by_event_name() {
        let mut tap = UsageTap::new(true, false);
        tap.feed(b"event: response.completed\ndata: ");
        tap.feed(&vec![b'x'; MAX_CAPTURE_BYTES + 10]);
        tap.feed(b"\n\n");
        assert_eq!(tap.report(StreamEnd::Eof).outcome, StreamOutcome::Completed);
    }

    #[test]
    fn non_stream_bodies() {
        let mut tap = UsageTap::new(false, false);
        tap.feed(b"{\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}");
        assert_eq!(tap.report(StreamEnd::Eof).outcome, StreamOutcome::Completed);
        let mut tap = UsageTap::new(false, false);
        tap.feed(b"{\"error\":{\"code\":\"server_is_overloaded\"}}");
        assert_eq!(tap.report(StreamEnd::Eof).error_code.as_deref(), Some("server_is_overloaded"));
    }

    #[test]
    fn codes_are_sanitized() {
        assert_eq!(sanitize_code("Selected model is at capacity."), Some("Selectedmodelisatcapacity.".into()));
        assert_eq!(sanitize_code("  "), None);
        assert_eq!(sanitize_code(&"a".repeat(100)).unwrap().len(), 64);
        assert!(is_overload_code("server_is_overloaded"));
        assert!(is_overload_code("server_overloaded"));
        assert!(is_overload_code("slow_down"));
        assert!(!is_overload_code("rate_limit_exceeded"));
        assert_eq!(event_kind(Some("server_is_overloaded")), "overloaded");
        assert_eq!(event_kind(Some("cyber_policy")), "policy_blocked");
        assert_eq!(event_kind(Some("usage_limit_reached")), "quota_exhausted");
        assert_eq!(event_kind(Some("server_error")), "upstream_failed");
        assert_eq!(event_kind(None), "upstream_failed");
    }

    #[test]
    fn oversized_json_does_not_parse_trailing_bytes_as_usage() {
        let mut tap = UsageTap::new(false, false);
        tap.feed(&vec![b'x'; MAX_CAPTURE_BYTES + 1]);
        tap.feed(b"{\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}");
        assert_eq!(tap.counts(), None);
    }
}
