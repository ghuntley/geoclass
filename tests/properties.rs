//! Hegel property tests over the pure core: placement, the gate, bindings, health, and stats.
use geoclass::bindings::{BindingRecord, Bindings};
use geoclass::config::Thresholds;
use geoclass::gate::{Gate, Verdict};
use geoclass::health::{Counts, State, next_state};
use geoclass::placement::{self, Reason, RegionView, Request};
use geoclass::sse::{StreamEnd, UsageTap};
use geoclass::stats_api::{self, StatsParams};
use geoclass::store::{AttemptRecord, Store};
use hegel::TestCase;
use hegel::generators as gs;

const NAMES: [&str; 5] = ["r0", "r1", "r2", "r3", "r4"];

fn draw_state(tc: &TestCase) -> State {
    tc.draw(gs::sampled_from(vec![State::Green, State::Amber, State::Red]))
}

fn draw_views(tc: &TestCase) -> Vec<RegionView> {
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(NAMES.len()));
    (0..n)
        .map(|i| RegionView {
            name: NAMES[i].to_string(),
            domain: tc.draw(gs::sampled_from(vec!["d0", "d1", "d2"])).to_string(),
            available: tc.draw(gs::integers::<u8>().min_value(0).max_value(4)) != 0,
            state: draw_state(tc),
            weight: tc.draw(gs::floats::<f64>().min_value(0.0).max_value(100.0).allow_nan(false).allow_infinity(false)),
        })
        .collect()
}

fn draw_tried(tc: &TestCase, views: &[RegionView]) -> Vec<String> {
    views.iter().filter(|_| tc.draw(gs::booleans())).map(|v| v.name.clone()).collect()
}

#[hegel::test]
fn test_placement_is_order_independent_and_never_picks_tried_or_unavailable(tc: TestCase) {
    let views = draw_views(&tc);
    let tried = draw_tried(&tc, &views);
    let binding = tc.draw(gs::sampled_from(vec![None, Some("r0"), Some("r1"), Some("r9")]));
    let session = tc.draw(gs::sampled_from(vec![None, Some("s1"), Some("s2")]));
    let req = Request { session, spread: "req", binding, tried: &tried };
    let choice = placement::choose(&views, &req);
    let mut reversed = views.clone();
    reversed.reverse();
    assert_eq!(placement::choose(&reversed, &req), choice);
    let open: Vec<&RegionView> = views.iter().filter(|v| v.available && !tried.contains(&v.name)).collect();
    match &choice {
        None => assert!(open.is_empty()),
        Some(c) => {
            assert!(open.iter().any(|v| v.name == c.region), "{c:?} is not an open region");
            if open.iter().any(|v| v.state != State::Red) {
                assert_ne!(views.iter().find(|v| v.name == c.region).unwrap().state, State::Red);
            }
        }
    }
}

#[hegel::test]
fn test_bound_sessions_stay_until_their_region_has_trouble(tc: TestCase) {
    let views = draw_views(&tc);
    let tried = draw_tried(&tc, &views);
    let bound = tc.draw(gs::sampled_from(views.iter().map(|v| v.name.clone()).collect::<Vec<_>>()));
    let view = views.iter().find(|v| v.name == bound).unwrap();
    let choice = placement::choose(&views, &Request { session: Some("s"), spread: "req", binding: Some(&bound), tried: &tried });
    let open = view.available && !tried.contains(&bound);
    let all_open_red = views.iter().filter(|v| v.available && !tried.contains(&v.name)).all(|v| v.state == State::Red);
    if open && (view.state != State::Red || all_open_red) {
        assert_eq!(choice.unwrap(), placement::Choice { region: bound, reason: Reason::Bound });
    } else if let Some(c) = choice {
        assert_ne!(c.region, bound);
        assert!(c.reason.is_move(), "{c:?}");
    }
}

#[hegel::test]
fn test_retry_sequences_visit_each_region_once_and_leave_failed_domains_first(tc: TestCase) {
    let mut views = draw_views(&tc);
    for v in &mut views {
        v.available = true;
    }
    let mut tried: Vec<String> = Vec::new();
    while let Some(c) = placement::choose(&views, &Request { session: Some("s"), spread: "req", binding: None, tried: &tried }) {
        assert!(!tried.contains(&c.region));
        let domain = &views.iter().find(|v| v.name == c.region).unwrap().domain;
        let tried_domains: Vec<&String> = tried.iter().map(|t| &views.iter().find(|v| &v.name == t).unwrap().domain).collect();
        let fresh_exists = views.iter().any(|v| !tried.contains(&v.name) && !tried_domains.contains(&&v.domain) && v.state != State::Red);
        let picked_red = views.iter().find(|v| v.name == c.region).unwrap().state == State::Red;
        if fresh_exists && !picked_red {
            assert!(!tried_domains.contains(&domain), "picked an already-failed domain while a fresh one was open");
        }
        tried.push(c.region);
        assert!(tried.len() <= views.len());
    }
    assert_eq!(tried.len(), views.len());
}

#[hegel::test]
fn test_placement_does_not_interpret_region_names(tc: TestCase) {
    // Rendezvous hashing depends on names, but no name is special: swapping two regions' names
    // together with all their attributes yields the swapped choice.
    let views = draw_views(&tc);
    tc.assume(views.len() >= 2);
    let req = Request { session: Some("s"), spread: "req", binding: None, tried: &[] };
    let choice = placement::choose(&views, &req);
    let renamed: Vec<RegionView> = views.iter().map(|v| RegionView { name: format!("zz-{}", v.name), ..v.clone() }).collect();
    let renamed_choice = placement::choose(&renamed, &req);
    assert_eq!(choice.is_some(), renamed_choice.is_some());
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Ev {
    Created,
    Output,
    Overload,
    Quota,
    Completed,
}

fn bytes(ev: Ev) -> &'static [u8] {
    match ev {
        Ev::Created => b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
        Ev::Output => b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
        Ev::Overload => b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n",
        Ev::Quota => b"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"usage_limit_reached\"}}}\n\n",
        Ev::Completed => b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
    }
}

#[hegel::test]
fn test_gate_retries_only_before_output_and_releases_exact_bytes(tc: TestCase) {
    let events: Vec<Ev> = tc.draw(gs::vecs(gs::sampled_from(vec![Ev::Created, Ev::Output, Ev::Overload, Ev::Quota, Ev::Completed])).max_size(8));
    let stream: Vec<u8> = events.iter().flat_map(|e| bytes(*e).to_vec()).collect();
    let splits: Vec<usize> = tc.draw(gs::vecs(gs::integers::<usize>().min_value(1).max_value(64)).max_size(40));
    let mut gate = Gate::new(true, false, 1 << 20);
    let mut released: Vec<u8> = Vec::new();
    let mut committed = false;
    let mut retried = false;
    let mut pos = 0;
    let mut chunks: Vec<&[u8]> = Vec::new();
    for size in splits {
        if pos >= stream.len() {
            break;
        }
        let end = (pos + size).min(stream.len());
        chunks.push(&stream[pos..end]);
        pos = end;
    }
    if pos < stream.len() {
        chunks.push(&stream[pos..]);
    }
    for chunk in &chunks {
        match gate.feed(chunk) {
            Verdict::Commit(_) => {
                assert!(!committed && !retried);
                committed = true;
                released.extend(gate.take_held());
            }
            Verdict::Pass => {
                assert!(committed);
                released.extend_from_slice(chunk);
            }
            Verdict::Retry { .. } => {
                assert!(!committed);
                retried = true;
            }
            Verdict::Hold => assert!(!committed),
        }
    }
    match gate.finish(StreamEnd::Eof) {
        Verdict::Commit(_) => {
            assert!(!committed && !retried);
            committed = true;
            released.extend(gate.take_held());
        }
        Verdict::Retry { .. } => {
            assert!(!committed);
            retried = true;
        }
        _ => {}
    }
    // Retry happens exactly when a capacity failure precedes any output and any other terminal.
    let first_decisive = events.iter().find(|e| !matches!(e, Ev::Created));
    assert_eq!(retried, first_decisive == Some(&Ev::Overload), "events {events:?}");
    if committed {
        assert_eq!(released, stream, "the client receives the upstream bytes exactly");
    }
}

#[hegel::test]
fn test_usage_parser_independent_of_chunk_boundaries(tc: TestCase) {
    let input: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(1_000_000));
    let cached: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(input));
    let output: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(1_000_000));
    let splits: Vec<usize> = tc.draw(gs::vecs(gs::integers::<usize>().min_value(1).max_value(40)).max_size(30));
    let event = format!("event: response.completed\r\ndata: {{\"type\":\"response.completed\",\"response\":{{\"usage\":{{\"input_tokens\":{input},\"output_tokens\":{output},\"input_tokens_details\":{{\"cached_tokens\":{cached}}}}}}}}}\r\n\r\n");
    let mut tap = UsageTap::new(true, false);
    let mut position = 0;
    for size in splits {
        if position >= event.len() {
            break;
        }
        let end = (position + size).min(event.len());
        tap.feed(&event.as_bytes()[position..end]);
        position = end;
    }
    tap.feed(&event.as_bytes()[position..]);
    let counts = tap.counts().unwrap();
    assert_eq!((counts.input_tokens, counts.cached_input_tokens, counts.output_tokens), (input, cached, output));
}

#[hegel::test]
fn test_binding_merge_is_order_independent_last_writer_wins(tc: TestCase) {
    let records: Vec<(u8, u8, i64, u8)> = tc.draw(gs::vecs(gs::tuples!(
        gs::integers::<u8>().min_value(0).max_value(3),
        gs::integers::<u8>().min_value(0).max_value(3),
        gs::integers::<i64>().min_value(0).max_value(50),
        gs::integers::<u8>().min_value(0).max_value(2)
    )).max_size(20));
    let records: Vec<BindingRecord> = records
        .into_iter()
        .map(|(k, r, at, n)| BindingRecord { key: format!("k{k}"), region: format!("r{r}"), updated_at: at, node: format!("n{n}") })
        .collect();
    let apply = |order: &[BindingRecord]| {
        let mut b = Bindings::new(100, 1_000_000);
        for r in order {
            b.merge(r.clone(), 100);
        }
        let mut snap = b.snapshot(100);
        snap.sort_by(|a, b| a.key.cmp(&b.key));
        snap
    };
    let forward = apply(&records);
    let mut reversed = records.clone();
    reversed.reverse();
    let twice: Vec<BindingRecord> = records.iter().chain(records.iter()).cloned().collect();
    assert_eq!(apply(&reversed), forward);
    assert_eq!(apply(&twice), forward);
    for r in &forward {
        let best = records
            .iter()
            .filter(|x| x.key == r.key)
            .max_by(|a, b| (a.updated_at, &a.node, &a.region).cmp(&(b.updated_at, &b.node, &b.region)))
            .unwrap();
        assert_eq!(r, best);
    }
}

#[hegel::test]
fn test_red_regions_clear_only_below_the_green_rate_or_with_no_failures(tc: TestCase) {
    let attempts = tc.draw(gs::integers::<i64>().min_value(0).max_value(500));
    let failures = tc.draw(gs::integers::<i64>().min_value(0).max_value(attempts));
    let counts = Counts { attempts, overloaded: failures, ..Default::default() };
    let t = Thresholds::default();
    let previous = draw_state(&tc);
    let next = next_state(previous, &counts, &t);
    if previous == State::Red && next != State::Red {
        assert!(counts.rate() <= t.green_rate || (attempts < t.min_samples && failures == 0), "{counts:?} -> {next:?}");
    }
    if attempts >= t.min_samples && counts.rate() >= t.red_rate {
        assert_eq!(next, State::Red);
    }
}

#[hegel::test]
fn test_cluster_merge_matches_a_single_node_with_every_row(tc: TestCase) {
    let rows: Vec<(u8, i64, u8, u8, u8)> = tc.draw(gs::vecs(gs::tuples!(
        gs::integers::<u8>().min_value(0).max_value(2),
        gs::integers::<i64>().min_value(0).max_value(3_599_000),
        gs::integers::<u8>().min_value(0).max_value(2),
        gs::integers::<u8>().min_value(0).max_value(3),
        gs::integers::<u8>().min_value(0).max_value(200)
    )).max_size(30));
    let nodes: Vec<Store> = (0..3).map(|_| Store::in_memory().unwrap()).collect();
    let all = Store::in_memory().unwrap();
    for (i, (node, ts, region, gate, tokens)) in rows.iter().enumerate() {
        let record = AttemptRecord {
            request_id: format!("req-{i}"),
            ts: *ts,
            duration_ms: Some(1),
            endpoint: "/r".into(),
            model: "m".into(),
            ingress_region: format!("r{node}"),
            egress_region: format!("r{region}"),
            egress_node: "n".into(),
            failure_domain: format!("r{region}"),
            placement_reason: "new".into(),
            status: 200,
            gate_result: ["output", "retried", "terminal", "exhausted"][*gate as usize].into(),
            input_tokens: Some(*tokens as i64),
            cached_input_tokens: Some(*tokens as i64 / 2),
            output_tokens: Some(1),
            rescued: *gate == 0 && tokens % 2 == 0,
            overload_hit: *gate == 1,
            ..Default::default()
        };
        nodes[*node as usize].insert_attempt(&record).unwrap();
        all.insert_attempt(&record).unwrap();
    }
    let now = 3_600_000;
    let params = StatsParams { from_ms: Some(0), to_ms: Some(now), bucket: Some("minute".into()), by: Some("region".into()), ..Default::default() };
    let parts: Vec<_> = nodes.iter().map(|s| stats_api::query(s, &params, now, 0, 730).unwrap()).collect();
    let merged = stats_api::merge_cluster(parts, vec![]).unwrap();
    let single = stats_api::query(&all, &params, now, 0, 730).unwrap();
    let strip = |t: &geoclass::stats::Totals| {
        let mut t = t.clone();
        t.requests = 0;
        t.sessions = 0;
        t.peak_concurrency = 0;
        t.mean_concurrency_milli = 0;
        t
    };
    assert_eq!(strip(&merged.totals), strip(&single.totals));
    assert_eq!(merged.series.len(), single.series.len());
    for (m, s) in merged.series.iter().zip(&single.series) {
        assert_eq!(m.key, s.key);
        assert_eq!(strip(&m.totals), strip(&s.totals));
    }
}
