//! Spike S1.1: durable inference ledger + Bedrock Converse online.
//!
//! Subcommands:
//!   run [--provider mock|bedrock] [--model ID] [--region R] [--db PATH]
//!       Process the sample tickets; rerunning reuses recorded results.
//!   bench [--items N] [--db PATH]
//!       Measure ledger overhead per work item with the mock provider.
//!
//! Real Bedrock runs need AWS credentials and (per project policy) the
//! eu-central-1 region; everything else runs offline for free.

mod ledger;
mod provider;
mod runner;
#[cfg(test)]
mod stub;
mod workkey;

use anyhow::Result;
use ledger::Ledger;
use provider::{BedrockProvider, MockProvider, OpenAiCompatProvider, Provider};
use std::path::PathBuf;
use std::time::Instant;

const SAMPLE_TICKETS: &[(&str, &str)] = &[
    ("t-1", "the printer in building 7 is on fire"),
    ("t-2", "duplicate charge on my last invoice"),
    ("t-3", "how do I export my data to csv"),
    ("t-4", "app crash when opening settings on android"),
    ("t-5", "question about billing cycle dates"),
];

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| default.to_owned())
}

fn main() -> Result<()> {
    let command = std::env::args().nth(1).unwrap_or_else(|| "run".into());
    let db = PathBuf::from(arg("--db", "s1-1-ledger.sqlite3"));

    match command.as_str() {
        "run" => {
            let provider_name = arg("--provider", "mock");
            let model = arg("--model", "mock-1");
            let ledger = Ledger::open(&db)?;
            let specs = runner::ticket_specs(&provider_name, &model, SAMPLE_TICKETS);

            let provider: Box<dyn Provider> = match provider_name.as_str() {
                "bedrock" => Box::new(BedrockProvider::new(&arg("--region", "eu-central-1"))?),
                // Fully local inference: e.g. Ollama at
                // http://localhost:11434/v1 or vLLM at http://host:8000/v1.
                "openai" => Box::new(OpenAiCompatProvider::new(
                    &arg("--endpoint", "http://localhost:11434/v1"),
                    std::env::var("OPENAI_API_KEY").ok(),
                )?),
                _ => Box::new(MockProvider::default()),
            };

            let started = Instant::now();
            let stats = runner::process(&ledger, provider.as_ref(), &specs)?;
            let (pending, submitted, completed, failed) = ledger.counts()?;
            println!(
                "run finished in {:?}\n  reused={} dispatched={} reconciled={} invalid={} failed={}\n  tokens in/out = {}/{}\n  ledger: pending={pending} submitted={submitted} completed={completed} failed={failed}",
                started.elapsed(),
                stats.reused,
                stats.dispatched,
                stats.reconciled,
                stats.invalid,
                stats.failed,
                stats.input_tokens,
                stats.output_tokens,
            );
        }
        "bench" => {
            let items: usize = arg("--items", "10000").parse()?;
            let _ = std::fs::remove_file(&db);
            let ledger = Ledger::open(&db)?;
            let tickets: Vec<(String, String)> = (0..items)
                .map(|i| (format!("t-{i}"), format!("synthetic ticket number {i}")))
                .collect();
            let borrowed: Vec<(&str, &str)> = tickets
                .iter()
                .map(|(a, b)| (a.as_str(), b.as_str()))
                .collect();
            let specs = runner::ticket_specs("mock", "mock-1", &borrowed);
            let provider = MockProvider::default();

            let started = Instant::now();
            let stats = runner::process(&ledger, &provider, &specs)?;
            let cold = started.elapsed();

            let started = Instant::now();
            let stats2 = runner::process(&ledger, &provider, &specs)?;
            let warm = started.elapsed();

            println!(
                "bench items={items}\n  cold (dispatch+record): {cold:?} ({:.1} µs/item)\n  warm (pure reuse):      {warm:?} ({:.1} µs/item)\n  dispatched cold={} reused warm={}",
                cold.as_micros() as f64 / items as f64,
                warm.as_micros() as f64 / items as f64,
                stats.dispatched,
                stats2.reused,
            );
        }
        other => {
            eprintln!("unknown command `{other}` (use: run | bench)");
            std::process::exit(1);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use provider::MockProvider;
    use std::sync::atomic::Ordering;

    fn temp_db(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("pramen-s1-1-tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{name}-{}.sqlite3", uuid::Uuid::new_v4()));
        path
    }

    #[test]
    fn completed_results_are_reused_across_restart() {
        let db = temp_db("reuse");
        let specs = runner::ticket_specs("mock", "mock-1", SAMPLE_TICKETS);

        // First run: everything dispatched.
        let provider = MockProvider::default();
        {
            let ledger = Ledger::open(&db).unwrap();
            let stats = runner::process(&ledger, &provider, &specs).unwrap();
            assert_eq!(stats.dispatched, SAMPLE_TICKETS.len() as u64);
            assert_eq!(stats.reused, 0);
        } // ledger dropped = process "restart"

        // Second run against a fresh connection: zero provider calls.
        let provider2 = MockProvider::default();
        let ledger = Ledger::open(&db).unwrap();
        let stats = runner::process(&ledger, &provider2, &specs).unwrap();
        assert_eq!(stats.reused, SAMPLE_TICKETS.len() as u64);
        assert_eq!(stats.dispatched, 0);
        assert_eq!(provider2.calls.load(Ordering::SeqCst), 0, "re-billed work");
    }

    #[test]
    fn changed_prompt_revision_is_new_work() {
        let db = temp_db("revision");
        let provider = MockProvider::default();
        let ledger = Ledger::open(&db).unwrap();

        let v1 = runner::ticket_specs("mock", "mock-1", SAMPLE_TICKETS);
        runner::process(&ledger, &provider, &v1).unwrap();

        let mut v2 = runner::ticket_specs("mock", "mock-1", SAMPLE_TICKETS);
        for spec in &mut v2 {
            spec.prompt_revision = "tickets-v2".into();
        }
        let stats = runner::process(&ledger, &provider, &v2).unwrap();
        assert_eq!(stats.dispatched, SAMPLE_TICKETS.len() as u64);
        assert_eq!(stats.reused, 0);
    }

    #[test]
    fn crash_after_submit_is_surfaced_and_recovered() {
        let db = temp_db("crash");
        let specs = runner::ticket_specs("mock", "mock-1", &SAMPLE_TICKETS[..1]);
        let key = specs[0].work_key();

        // Simulate a crash between mark_submitted and complete.
        {
            let ledger = Ledger::open(&db).unwrap();
            ledger
                .upsert_pending(&key, &serde_json::to_string(&specs[0]).unwrap())
                .unwrap();
            ledger.mark_submitted(&key, "req-lost-in-crash").unwrap();
        }

        let ledger = Ledger::open(&db).unwrap();
        assert_eq!(ledger.submitted_items().unwrap().len(), 1);

        let provider = MockProvider::default();
        let stats = runner::process(&ledger, &provider, &specs).unwrap();
        assert_eq!(stats.reconciled, 1, "ambiguous submission must be surfaced");
        assert_eq!(stats.dispatched, 1);

        // And the recovery itself is durable.
        let stats = runner::process(&ledger, &provider, &specs).unwrap();
        assert_eq!(stats.reused, 1);
    }

    /// ADR 0005 L1: the *real* Bedrock adapter — request building, signing,
    /// response parsing, schema validation, ledger recording — runs offline
    /// against a localhost protocol stub with static test credentials.
    #[test]
    fn bedrock_adapter_full_path_against_local_stub() {
        let model_output =
            r#"{\"category\": \"incident\", \"priority\": \"high\", \"rationale\": \"fire\"}"#
                .replace("\\\"", "\"");
        let converse_response = serde_json::json!({
            "metrics": {"latencyMs": 42},
            "output": {"message": {"role": "assistant", "content": [{"text": model_output}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 25, "outputTokens": 18, "totalTokens": 43}
        });
        let stub = stub::StubServer::serve_json(converse_response.to_string(), 1);

        let bedrock = provider::BedrockProvider::with_endpoint("eu-central-1", &stub.url).unwrap();
        let db = temp_db("bedrock-stub");
        let ledger = Ledger::open(&db).unwrap();
        let specs = runner::ticket_specs("bedrock", "stub-model-v1", &SAMPLE_TICKETS[..1]);

        let stats = runner::process(&ledger, &bedrock, &specs).unwrap();
        assert_eq!(stats.dispatched, 1);
        assert_eq!(stats.invalid, 0);
        assert_eq!(stats.input_tokens, 25);
        assert_eq!(stats.output_tokens, 18);

        match ledger.state(&specs[0].work_key()).unwrap() {
            Some(ledger::WorkState::Completed(result)) => {
                assert_eq!(result.validation, "valid");
                assert_eq!(result.output["category"], "incident");
                assert_eq!(result.provider, "bedrock");
            }
            other => panic!("expected completed, got {other:?}"),
        }

        let requests = stub.finish();
        assert!(
            requests[0].contains("/model/stub-model-v1/converse"),
            "adapter must call the Converse route: {}",
            requests[0].lines().next().unwrap_or_default()
        );
    }

    /// ADR 0005 L1/L2: the OpenAI-compatible adapter (the same code path
    /// used against Ollama/vLLM locally) runs against a localhost stub.
    #[test]
    fn openai_compat_adapter_against_local_stub() {
        let completion = serde_json::json!({
            "id": "chatcmpl-local-1",
            "choices": [{"message": {"role": "assistant",
                "content": "{\"category\": \"billing\", \"priority\": \"normal\", \"rationale\": \"invoice\"}"}}],
            "usage": {"prompt_tokens": 40, "completion_tokens": 21}
        });
        let stub = stub::StubServer::serve_json(completion.to_string(), 1);

        let provider =
            provider::OpenAiCompatProvider::new(&format!("{}/v1", stub.url), None).unwrap();
        let db = temp_db("openai-stub");
        let ledger = Ledger::open(&db).unwrap();
        let specs = runner::ticket_specs("openai-compat", "local-model", &SAMPLE_TICKETS[1..2]);

        let stats = runner::process(&ledger, &provider, &specs).unwrap();
        assert_eq!(stats.dispatched, 1);
        assert_eq!(stats.invalid, 0);

        match ledger.state(&specs[0].work_key()).unwrap() {
            Some(ledger::WorkState::Completed(result)) => {
                assert_eq!(result.output["category"], "billing");
                assert_eq!(result.request_id, "chatcmpl-local-1");
                assert_eq!(result.input_tokens, 40);
            }
            other => panic!("expected completed, got {other:?}"),
        }

        let requests = stub.finish();
        assert!(requests[0].contains("/v1/chat/completions"));
        assert!(requests[0].contains("response_format"));
    }

    #[test]
    fn invalid_output_is_recorded_not_dropped() {
        struct BadProvider;
        impl Provider for BadProvider {
            fn name(&self) -> &str {
                "bad"
            }
            fn invoke(&self, _spec: &workkey::WorkSpec) -> Result<provider::ProviderResponse> {
                Ok(provider::ProviderResponse {
                    output: serde_json::json!({"category": "not-a-real-category"}),
                    request_id: "bad-1".into(),
                    input_tokens: 1,
                    output_tokens: 1,
                })
            }
        }

        let db = temp_db("invalid");
        let ledger = Ledger::open(&db).unwrap();
        let specs = runner::ticket_specs("bad", "bad-1", &SAMPLE_TICKETS[..1]);
        let stats = runner::process(&ledger, &BadProvider, &specs).unwrap();
        assert_eq!(stats.invalid, 1);

        match ledger.state(&specs[0].work_key()).unwrap() {
            Some(ledger::WorkState::Completed(result)) => {
                assert!(result.validation.starts_with("invalid:"));
            }
            other => panic!("expected completed-with-invalid, got {other:?}"),
        }
    }
}
