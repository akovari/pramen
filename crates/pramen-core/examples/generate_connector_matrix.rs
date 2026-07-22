//! Print the connector support matrix Markdown (E1.4).
//!
//! ```text
//! cargo run -p pramen-core --example generate_connector_matrix --quiet \
//!   > docs/connectors/support-matrix.md
//! ```

fn main() {
    print!(
        "# Connector support matrix\n\n{}",
        pramen_core::connector::matrix_markdown()
    );
}
