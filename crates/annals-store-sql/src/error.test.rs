use super::SqlError;

#[test]
fn an_error_renders_its_english_fallback_with_its_detail() {
    assert_eq!(
        SqlError::Unavailable {
            detail: "disk I/O error".to_string()
        }
        .to_string(),
        "the database failed: disk I/O error"
    );
    assert_eq!(
        SqlError::Undecodable {
            detail: "buffer underflow".to_string()
        }
        .to_string(),
        "a stored value does not decode: buffer underflow"
    );
}
