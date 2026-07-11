//! Regenerates the committed pipeline JSON Schema artifact:
//!
//! ```sh
//! cargo run -p pramen-core --example generate_schema > docs/schema/pipeline.v1alpha1.schema.json
//! ```
//!
//! A unit test fails when the committed artifact drifts from the code.

fn main() {
    #[allow(clippy::expect_used, reason = "one-shot generator, not library code")]
    let pretty =
        serde_json::to_string_pretty(&pramen_core::spec::json_schema()).expect("schema serializes");
    println!("{pretty}");
}
