//! Guards for L2 tests against real local services (ADR 0005): the test
//! runs when the variable is set and self-skips with a uniform message
//! when it is not, so offline runs stay green without ignoring tests.

/// The PostgreSQL DSN for L2 database tests, e.g.
/// `postgres://postgres:pw@localhost:5432/postgres`. `None` (with a skip
/// note on stderr) when `PRAMEN_TEST_POSTGRES_DSN` is unset.
#[must_use]
pub fn postgres_dsn() -> Option<String> {
    guarded("PRAMEN_TEST_POSTGRES_DSN")
}

/// The object-store URL for L2 S3 tests against MinIO, e.g.
/// `s3://pramen-test/events/` (standard `AWS_*` variables point at the
/// local endpoint). `None` (with a skip note on stderr) when
/// `PRAMEN_TEST_S3_URL` is unset.
#[must_use]
pub fn s3_url() -> Option<String> {
    guarded("PRAMEN_TEST_S3_URL")
}

fn guarded(variable: &str) -> Option<String> {
    match std::env::var(variable) {
        Ok(value) => Some(value),
        Err(_) => {
            eprintln!("skipping: {variable} not set");
            None
        }
    }
}
