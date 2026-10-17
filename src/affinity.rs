//! Session identity. A session is its prompt cache key — nothing else.
use http::HeaderMap;
use serde_json::Value;

/// @cc [owner:ghuntley,label:routing] session-is-prompt-cache-key
/// The session key MUST be the request body's `prompt_cache_key`, else `promptCacheKey`, when it
/// is a non-empty string; otherwise the request has no session. No header or other field MAY
/// stand in for it. Callers MUST hash the key (`sha256_hex`) before storing, logging, or
/// gossiping it.
pub fn session_key(body: &Value) -> Option<&str> {
    body.get("prompt_cache_key")
        .or_else(|| body.get("promptCacheKey"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// The request's model, when the body names one.
pub fn model(body: &Value) -> Option<&str> {
    body.get("model").and_then(Value::as_str).filter(|s| !s.is_empty())
}

/// SHA-256 of the ChatGPT account id underclass injected, for per-account statistics. The raw id
/// is never kept.
pub fn account_hash(headers: &HeaderMap) -> Option<String> {
    headers
        .get("chatgpt-account-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(crate::sha256_hex)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn key_comes_only_from_the_prompt_cache_key() {
        assert_eq!(session_key(&json!({"prompt_cache_key": "s1"})), Some("s1"));
        assert_eq!(session_key(&json!({"promptCacheKey": "s2"})), Some("s2"));
        assert_eq!(session_key(&json!({"prompt_cache_key": "s1", "promptCacheKey": "s2"})), Some("s1"));
        assert_eq!(session_key(&json!({"prompt_cache_key": ""})), None);
        assert_eq!(session_key(&json!({"session_id": "s3"})), None);
        assert_eq!(session_key(&json!({"prompt_cache_key": 7})), None);
    }

    #[test]
    fn account_ids_are_hashed() {
        let mut headers = HeaderMap::new();
        headers.insert("ChatGPT-Account-Id", "acct-1".parse().unwrap());
        let hash = account_hash(&headers).unwrap();
        assert_eq!(hash.len(), 64);
        assert!(!hash.contains("acct-1"));
    }
}
