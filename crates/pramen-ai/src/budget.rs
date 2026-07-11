//! Token budgets, enforced before dispatch.
//!
//! Input budgets use a deliberately conservative byte-based estimate
//! (4 bytes ≈ 1 token holds for English text; most tokenizers do better),
//! so a record rejected here would very likely exceed the real budget too.
//! Output budgets are enforced provider-side via the request's token cap
//! and rechecked against reported usage.

use crate::error::AiError;
use pramen_core::spec::AiBudget;

/// Conservative token estimate for a piece of request text.
#[must_use]
pub fn estimate_tokens(text: &str) -> u32 {
    u32::try_from(text.len().div_ceil(4)).unwrap_or(u32::MAX)
}

/// Enforce the per-record input budget before anything is dispatched.
///
/// # Errors
///
/// Returns [`AiError::BudgetExceeded`] when the estimated input tokens for
/// `request_text` exceed the configured ceiling.
pub fn enforce_input_budget(budget: Option<&AiBudget>, request_text: &str) -> Result<(), AiError> {
    let Some(cap) = budget.and_then(|b| b.max_input_tokens_per_record) else {
        return Ok(());
    };
    let estimate = estimate_tokens(request_text);
    if estimate > cap {
        return Err(AiError::BudgetExceeded(format!(
            "estimated {estimate} input tokens exceed maxInputTokensPerRecord {cap}"
        )));
    }
    Ok(())
}

/// The provider-side output token cap for a request, if configured.
#[must_use]
pub fn output_cap(budget: Option<&AiBudget>) -> Option<u32> {
    budget.and_then(|b| b.max_output_tokens_per_record)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(input: Option<u32>, output: Option<u32>) -> AiBudget {
        AiBudget {
            max_input_tokens_per_record: input,
            max_output_tokens_per_record: output,
        }
    }

    #[test]
    fn under_budget_passes_and_over_budget_fails_before_dispatch() {
        let b = budget(Some(10), None);
        assert!(enforce_input_budget(Some(&b), "short").is_ok());
        let error = enforce_input_budget(Some(&b), &"x".repeat(100)).unwrap_err();
        assert!(matches!(error, AiError::BudgetExceeded(_)));
    }

    #[test]
    fn absent_budget_never_blocks() {
        assert!(enforce_input_budget(None, &"x".repeat(100_000)).is_ok());
        assert!(enforce_input_budget(Some(&budget(None, Some(5))), &"x".repeat(100_000)).is_ok());
        assert_eq!(output_cap(Some(&budget(None, Some(5)))), Some(5));
        assert_eq!(output_cap(None), None);
    }

    #[test]
    fn estimate_is_conservative_ceiling() {
        assert_eq!(estimate_tokens(""), 0);
        assert_eq!(estimate_tokens("abc"), 1);
        assert_eq!(estimate_tokens("abcd"), 1);
        assert_eq!(estimate_tokens("abcde"), 2);
    }
}
