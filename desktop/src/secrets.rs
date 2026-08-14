//! OS-level secure storage for mount profile credentials.
//!
//! - Windows: Credential Manager (via `keyring`'s `windows-native` feature)
//! - macOS: Keychain (via `keyring`'s `apple-native` feature)
//! - Other platforms (Linux dev/CI): no system store is compiled in —
//!   `system_store()` returns `None` and callers fall back to keeping the
//!   credentials in `profiles.json` **with an explicit warning**; mounting
//!   must never become unusable because the secure store is missing.
//!
//! One entry is stored per profile under the fixed service name
//! [`SERVICE`], keyed by the profile's `secret_ref` (see `model::Profile`).
//! The JSON payload is a serialized [`Credentials`].

use serde::{Deserialize, Serialize};

/// Service (keychain) / target (Credential Manager) name for all entries.
pub const SERVICE: &str = "ossfs-tray";

/// The S3 credential pair stored per profile.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

/// Failures of a [`SecretStore`] implementation.
#[derive(Debug)]
pub enum StoreError {
    /// No usable secure store exists on this platform.
    Unavailable,
    /// The store exists but rejected the operation (locked, ACL, size, ...).
    Failure(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Unavailable => write!(f, "系统安全存储不可用"),
            StoreError::Failure(msg) => write!(f, "系统安全存储访问失败：{msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub trait SecretStore: Send + Sync {
    fn put(&self, secret_ref: &str, creds: &Credentials) -> Result<(), StoreError>;
    /// `Ok(None)` means the entry does not exist (never set or deleted).
    fn get(&self, secret_ref: &str) -> Result<Option<Credentials>, StoreError>;
    /// Deleting a missing entry is not an error.
    fn delete(&self, secret_ref: &str) -> Result<(), StoreError>;
}

/// The platform secure store, when one is compiled in.
///
/// Only Windows and macOS ship a real backend; everywhere else this returns
/// `None` so `model` degrades to (warned) plaintext storage.
#[cfg(any(windows, target_os = "macos"))]
pub fn system_store() -> Option<&'static dyn SecretStore> {
    Some(&KeyringStore)
}

/// The platform secure store, when one is compiled in (see the cfg'd twin).
#[cfg(not(any(windows, target_os = "macos")))]
pub fn system_store() -> Option<&'static dyn SecretStore> {
    None
}

#[cfg(any(windows, target_os = "macos"))]
static KEYRING_STORE: KeyringStore = KeyringStore;

#[cfg(any(windows, target_os = "macos"))]
struct KeyringStore;

#[cfg(any(windows, target_os = "macos"))]
impl SecretStore for KeyringStore {
    fn put(&self, secret_ref: &str, creds: &Credentials) -> Result<(), StoreError> {
        let payload =
            serde_json::to_string(creds).map_err(|e| StoreError::Failure(e.to_string()))?;
        keyring::Entry::new(SERVICE, secret_ref)
            .and_then(|entry| entry.set_password(&payload))
            .map_err(keyring_err)
    }

    fn get(&self, secret_ref: &str) -> Result<Option<Credentials>, StoreError> {
        let entry = keyring::Entry::new(SERVICE, secret_ref).map_err(keyring_err)?;
        match entry.get_password() {
            Ok(payload) => serde_json::from_str(&payload).map(Some).map_err(|e| {
                // A present-but-corrupt entry must not read as "missing":
                // that would tell the user to re-enter credentials although
                // the (damaged) entry is still there.
                StoreError::Failure(format!("stored credential is not valid JSON: {e}"))
            }),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(keyring_err(e)),
        }
    }

    fn delete(&self, secret_ref: &str) -> Result<(), StoreError> {
        match keyring::Entry::new(SERVICE, secret_ref)
            .map_err(keyring_err)?
            .delete_credential()
        {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(keyring_err(e)),
        }
    }
}

#[cfg(any(windows, target_os = "macos"))]
fn keyring_err(e: keyring::Error) -> StoreError {
    StoreError::Failure(e.to_string())
}

// ---------------------------------------------------------------------------
// In-memory store for unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod memory {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Process-local store used by the `model` unit tests; never touches the
    /// real keychain / Credential Manager, so it is safe in CI.
    #[derive(Default)]
    pub struct MemoryStore {
        map: Mutex<HashMap<String, Credentials>>,
    }

    impl MemoryStore {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn contains(&self, secret_ref: &str) -> bool {
            self.map.lock().unwrap().contains_key(secret_ref)
        }

        pub fn len(&self) -> usize {
            self.map.lock().unwrap().len()
        }

        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }
    }

    impl SecretStore for MemoryStore {
        fn put(&self, secret_ref: &str, creds: &Credentials) -> Result<(), StoreError> {
            self.map
                .lock()
                .unwrap()
                .insert(secret_ref.to_string(), creds.clone());
            Ok(())
        }

        fn get(&self, secret_ref: &str) -> Result<Option<Credentials>, StoreError> {
            Ok(self.map.lock().unwrap().get(secret_ref).cloned())
        }

        fn delete(&self, secret_ref: &str) -> Result<(), StoreError> {
            self.map.lock().unwrap().remove(secret_ref);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(windows, target_os = "macos"))]
    #[test]
    #[ignore = "touches the real system credential store; run locally with `cargo test -p ossfs-tray -- --ignored`"]
    fn corrupt_store_entry_reads_as_failure_not_missing() {
        // Regression: an entry whose payload is not the expected JSON must
        // surface as Err (reported as a read failure), not as Ok(None)
        // ("entry missing, re-enter credentials") — the entry is still there.
        let store = system_store().expect("platform store must exist");
        let secret_ref = "profile-test-corrupt-payload";
        keyring::Entry::new(SERVICE, secret_ref)
            .and_then(|e| e.set_password("this is not a JSON credential payload"))
            .expect("seed corrupt entry");
        let got = store.get(secret_ref);
        assert!(got.is_err(), "corrupt entry must read as Err, got {got:?}");
        store.delete(secret_ref).expect("cleanup");
    }

    #[cfg(any(windows, target_os = "macos"))]
    #[test]
    #[ignore = "touches the real system credential store; run locally with `cargo test -p ossfs-tray -- --ignored`"]
    fn real_system_store_roundtrip() {
        let store = system_store().expect("platform store must exist");
        let secret_ref = "profile-test-real-roundtrip";
        let creds = Credentials {
            access_key: "ak-roundtrip".into(),
            secret_key: "sk-roundtrip".into(),
        };
        store.put(secret_ref, &creds).expect("put");
        assert_eq!(store.get(secret_ref).expect("get"), Some(creds));
        store.delete(secret_ref).expect("delete");
        assert_eq!(store.get(secret_ref).expect("get after delete"), None);
    }
}
