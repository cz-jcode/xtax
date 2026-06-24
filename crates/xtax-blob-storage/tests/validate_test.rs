use xtax_blob_storage::validate_blob_key;

#[test]
fn empty_key_is_rejected() {
    assert!(validate_blob_key("").is_err());
}

#[test]
fn leading_slash_is_rejected() {
    assert!(validate_blob_key("/etc/passwd").is_err());
}

#[test]
fn dotdot_is_rejected() {
    assert!(validate_blob_key("../../etc/passwd").is_err());
}

#[test]
fn dot_is_rejected() {
    assert!(validate_blob_key("./foo").is_err());
}

#[test]
fn dotdot_inside_is_rejected() {
    assert!(validate_blob_key("a/../../x").is_err());
}

#[test]
fn dot_inside_is_rejected() {
    assert!(validate_blob_key("a/./x").is_err());
}

#[test]
fn normal_single_component_is_ok() {
    assert!(validate_blob_key("hello.txt").is_ok());
}

#[test]
fn nested_key_is_ok() {
    assert!(validate_blob_key("a/b/c.txt").is_ok());
}

#[test]
fn trailing_slash_is_rejected() {
    assert!(validate_blob_key("foo/").is_err());
}

#[test]
fn backslash_is_rejected() {
    assert!(validate_blob_key("a\\b.txt").is_err());
}

#[test]
fn empty_component_is_rejected() {
    assert!(validate_blob_key("a//b.txt").is_err());
}

#[test]
fn key_with_only_dots_is_ok() {
    assert!(validate_blob_key("...").is_ok());
    assert!(validate_blob_key("..a").is_ok());
    assert!(validate_blob_key("a..").is_ok());
}
