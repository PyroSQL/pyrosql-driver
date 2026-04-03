//! Subscription limits for RMP connections.
//!
//! [`SubscriptionLimits`] defines server-side and client-side limits for
//! subscriptions and mirror memory.

/// Configurable limits for RMP subscriptions and mirrors.
#[derive(Debug, Clone)]
pub struct SubscriptionLimits {
    /// Maximum rows per single subscription (server rejects if exceeded).
    pub max_rows_per_subscription: u64,
    /// Maximum total mirror memory per connection in bytes.
    pub max_mirror_bytes: u64,
}

impl Default for SubscriptionLimits {
    fn default() -> Self {
        Self {
            max_rows_per_subscription: 100_000,
            max_mirror_bytes: 256 * 1024 * 1024, // 256 MB
        }
    }
}
