//! The retry-before-output gate.
//!
//! Upstream capacity failures arrive inside an HTTP 200 stream (`response.created`, then
//! `response.failed` with `server_is_overloaded`). The gate holds every byte of an attempt until
//! the stream proves itself — first model output, a terminal event, or too many buffered bytes —
//! and only then releases the buffer to the client. A capacity failure seen while holding becomes
//! a retry in another region; the client never sees the failed attempt.
//!
//! Pure and synchronous: no I/O, no clock.
use crate::sse::{StreamEnd, StreamOutcome, TokenCounts, UsageTap, is_retryable_code};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitReason {
    /// Model output (a delta, an added output item, chat choices) arrived.
    Output,
    /// A terminal event that is not a capacity failure arrived (completed, incomplete, a
    /// non-capacity failure); it belongs to the client.
    Terminal,
    /// The held bytes exceeded the cap before output arrived.
    Overflow,
    /// The body ended cleanly while held.
    Eof,
}

impl CommitReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Output => "output",
            Self::Terminal => "terminal",
            Self::Overflow => "overflow",
            Self::Eof => "eof",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryKind {
    /// A capacity error code before any output.
    Capacity,
    /// The upstream transfer broke before any output.
    Transport,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Keep holding; nothing goes to the client yet.
    Hold,
    /// Release the held buffer; from now on every byte passes straight through.
    Commit(CommitReason),
    /// Discard this attempt and try another region.
    Retry { kind: RetryKind, code: Option<String> },
    /// Already committed: this chunk passes straight through.
    Pass,
}

pub struct Gate {
    tap: UsageTap,
    held: Vec<u8>,
    max_held: usize,
    committed: bool,
    retried: bool,
}

impl Gate {
    /// `stream` is whether the body is SSE; `chat` selects Chat Completions event shapes.
    pub fn new(stream: bool, chat: bool, max_held: usize) -> Self {
        Self { tap: UsageTap::new(stream, chat), held: Vec::new(), max_held, committed: false, retried: false }
    }

    /// @cc [owner:ghuntley,label:proxy] gate-retry-only-before-output
    /// `feed` and `finish` MUST NOT return `Retry` once the gate has committed, and MUST return
    /// `Retry` only for a capacity error code (`sse::is_retryable_code`) or a transport failure
    /// seen before any model output. Model output MUST commit even if a capacity failure follows
    /// it in the same chunk.
    pub fn feed(&mut self, chunk: &[u8]) -> Verdict {
        self.tap.feed(chunk);
        if self.committed {
            return Verdict::Pass;
        }
        if self.retried {
            return Verdict::Hold;
        }
        self.held.extend_from_slice(chunk);
        let verdict = if self.tap.saw_output() {
            Verdict::Commit(CommitReason::Output)
        } else {
            match self.tap.terminal() {
                Some((StreamOutcome::Failed, code)) if is_retryable_code(code) => {
                    Verdict::Retry { kind: RetryKind::Capacity, code: code.map(str::to_string) }
                }
                Some(_) => Verdict::Commit(CommitReason::Terminal),
                None if self.held.len() > self.max_held => Verdict::Commit(CommitReason::Overflow),
                None => Verdict::Hold,
            }
        };
        self.settle(verdict)
    }

    /// Called when the upstream body ends (cleanly or not) while the gate may still be holding.
    pub fn finish(&mut self, end: StreamEnd) -> Verdict {
        if self.committed {
            return Verdict::Pass;
        }
        if self.retried {
            return Verdict::Hold;
        }
        let verdict = match end {
            StreamEnd::UpstreamError | StreamEnd::Dropped => Verdict::Retry { kind: RetryKind::Transport, code: None },
            StreamEnd::Eof => {
                let report = self.tap.report(StreamEnd::Eof);
                if report.outcome == StreamOutcome::Failed && !report.saw_output && is_retryable_code(report.error_code.as_deref()) {
                    Verdict::Retry { kind: RetryKind::Capacity, code: report.error_code }
                } else {
                    Verdict::Commit(CommitReason::Eof)
                }
            }
        };
        self.settle(verdict)
    }

    fn settle(&mut self, verdict: Verdict) -> Verdict {
        match &verdict {
            Verdict::Commit(_) => self.committed = true,
            Verdict::Retry { .. } => self.retried = true,
            _ => {}
        }
        verdict
    }

    /// The held bytes, released exactly once at commit (or kept as the failure to pass through
    /// when every region fails).
    pub fn take_held(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.held)
    }

    pub fn committed(&self) -> bool {
        self.committed
    }

    pub fn tap(&self) -> &UsageTap {
        &self.tap
    }

    pub fn counts(&self) -> Option<TokenCounts> {
        self.tap.counts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREATED: &[u8] = b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n";
    const OVERLOADED: &[u8] = b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n";
    const DELTA: &[u8] = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n";
    const COMPLETED: &[u8] = b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n";

    #[test]
    fn overload_before_output_retries() {
        let mut gate = Gate::new(true, false, 1 << 20);
        assert_eq!(gate.feed(CREATED), Verdict::Hold);
        assert_eq!(
            gate.feed(OVERLOADED),
            Verdict::Retry { kind: RetryKind::Capacity, code: Some("server_is_overloaded".into()) }
        );
        assert_eq!(gate.feed(DELTA), Verdict::Hold, "a retried gate never commits");
    }

    #[test]
    fn output_commits_and_releases_the_held_prefix() {
        let mut gate = Gate::new(true, false, 1 << 20);
        assert_eq!(gate.feed(CREATED), Verdict::Hold);
        assert_eq!(gate.feed(DELTA), Verdict::Commit(CommitReason::Output));
        assert_eq!(gate.take_held(), [CREATED, DELTA].concat());
        assert_eq!(gate.feed(OVERLOADED), Verdict::Pass);
        assert_eq!(gate.finish(StreamEnd::UpstreamError), Verdict::Pass);
    }

    #[test]
    fn order_within_a_chunk_decides() {
        let mut gate = Gate::new(true, false, 1 << 20);
        assert_eq!(gate.feed(&[CREATED, DELTA, OVERLOADED].concat()), Verdict::Commit(CommitReason::Output));
        let mut gate = Gate::new(true, false, 1 << 20);
        assert!(matches!(gate.feed(&[CREATED, OVERLOADED, DELTA].concat()), Verdict::Retry { .. }));
    }

    #[test]
    fn non_capacity_failures_and_quota_belong_to_the_client() {
        let mut gate = Gate::new(true, false, 1 << 20);
        let quota = b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"usage_limit_reached\"}}}\n\n";
        assert_eq!(gate.feed(quota), Verdict::Commit(CommitReason::Terminal));
        let mut gate = Gate::new(true, false, 1 << 20);
        assert_eq!(gate.feed(COMPLETED), Verdict::Commit(CommitReason::Terminal));
    }

    #[test]
    fn overflow_and_eof_commit_and_transport_errors_retry() {
        let mut gate = Gate::new(true, false, 10);
        assert_eq!(gate.feed(CREATED), Verdict::Commit(CommitReason::Overflow));
        let mut gate = Gate::new(true, false, 1 << 20);
        gate.feed(CREATED);
        assert_eq!(gate.finish(StreamEnd::Eof), Verdict::Commit(CommitReason::Eof));
        let mut gate = Gate::new(true, false, 1 << 20);
        gate.feed(CREATED);
        assert_eq!(gate.finish(StreamEnd::UpstreamError), Verdict::Retry { kind: RetryKind::Transport, code: None });
    }

    #[test]
    fn non_stream_bodies_decide_at_eof() {
        let mut gate = Gate::new(false, false, 1 << 20);
        assert_eq!(gate.feed(b"{\"error\":{\"code\":\"server_is_overloaded\"}}"), Verdict::Hold);
        assert_eq!(gate.finish(StreamEnd::Eof), Verdict::Retry { kind: RetryKind::Capacity, code: Some("server_is_overloaded".into()) });
        let mut gate = Gate::new(false, false, 1 << 20);
        gate.feed(b"{\"output_text\":\"hi\"}");
        assert_eq!(gate.finish(StreamEnd::Eof), Verdict::Commit(CommitReason::Eof));
    }
}
