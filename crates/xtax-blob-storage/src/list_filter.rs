use crate::types::BlobMeta;

/// Pluggable filter for `list()` operations.
///
/// Backends may optimise known filter types. Users can implement
/// custom filters for backend-specific metadata queries.
///
/// For full documentation see the
/// [Filters guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/filters.md).
pub trait ListFilter: Send + Sync {
    /// Returns `true` if the blob should be included in results.
    fn matches(&self, key: &str, meta: Option<&BlobMeta>) -> bool;

    /// Optional hint that backends can use to optimise the listing.
    ///
    /// When provided, the backend MAY restrict its search to keys that
    /// start with this prefix — reducing the number of keys it needs
    /// to enumerate and then post-filter.
    ///
    /// ## When to use
    ///
    /// - **FS backend**: limits the directory walk to a single subtree,
    ///   avoiding the cost of traversing the entire root.
    /// - **S3 backend**: sets the `prefix` parameter on `ListObjectsV2`,
    ///   reducing the result set size and network cost.
    /// - **Prefix layer**: combines the store's own prefix with the
    ///   caller's filter prefix to minimise the inner scope.
    ///
    /// The default returns `None` — backends must still produce correct
    /// results (just potentially slower).
    fn prefix_hint(&self) -> Option<&str> {
        None
    }
}

// ---------------------------------------------------------------------------
// Built-in filters
// ---------------------------------------------------------------------------

/// Filter blobs whose key ends with a given suffix.
#[derive(Debug, Clone)]
pub struct SuffixFilter {
    pub suffix: String,
}

impl SuffixFilter {
    /// Create a filter matching keys that end with the given `suffix`.
    pub fn new(suffix: impl Into<String>) -> Self {
        Self {
            suffix: suffix.into(),
        }
    }
}

impl ListFilter for SuffixFilter {
    fn matches(&self, key: &str, _meta: Option<&BlobMeta>) -> bool {
        key.ends_with(&self.suffix)
    }
}

/// Filter blobs whose key starts with a given prefix.
#[derive(Debug, Clone)]
pub struct PrefixFilter {
    pub prefix: String,
}

impl PrefixFilter {
    /// Create a filter matching keys that start with the given `prefix`.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl ListFilter for PrefixFilter {
    fn matches(&self, key: &str, _meta: Option<&BlobMeta>) -> bool {
        key.starts_with(&self.prefix)
    }

    fn prefix_hint(&self) -> Option<&str> {
        Some(&self.prefix)
    }
}

/// Inverse of another filter.
///
/// Does **not** delegate `prefix_hint()` — a negated prefix cannot be
/// used as a positive prefix hint, so the default (`None`) is the safe choice.
pub struct NotFilter {
    inner: Box<dyn ListFilter>,
}

impl std::fmt::Debug for NotFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotFilter").finish_non_exhaustive()
    }
}

impl NotFilter {
    /// Create a filter that inverts the given filter.
    pub fn new(inner: Box<dyn ListFilter>) -> Self {
        Self { inner }
    }
}

impl ListFilter for NotFilter {
    fn matches(&self, key: &str, meta: Option<&BlobMeta>) -> bool {
        !self.inner.matches(key, meta)
    }
}

impl dyn ListFilter {
    /// Create a filter that matches keys NOT ending with the given suffix.
    pub fn exclude_suffix(suffix: impl Into<String>) -> NotFilter {
        NotFilter::new(Box::new(SuffixFilter::new(suffix)))
    }
}
