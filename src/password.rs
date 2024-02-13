use crate::map_join_error;
use argon2::password_hash::{PasswordHashString, SaltString};
use argon2::{password_hash, Argon2, PasswordHash, PasswordHasher};
use color_eyre::eyre::WrapErr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// An Argon2 password hashing and verifying pool which runs using Tokio blocking tasks.
///
/// Uses a semaphore internally to avoid spawning more threads than can actually run concurrently.
#[derive(Clone)]
pub struct PasswordHashingPool {
    // Note: we have to use `std::sync::Arc` for `Semaphore::acquire_owned()` anyway
    // so the value in using `triomphe::Arc` here is minimal.
    argon2: Arc<Argon2<'static>>,
    semaphore: Arc<Semaphore>,
}

impl PasswordHashingPool {
    pub fn new(max_blocking_tasks: NonZeroUsize) -> Self {
        PasswordHashingPool {
            argon2: init_argon2().into(),
            semaphore: Arc::new(Semaphore::new(max_blocking_tasks.get())),
        }
    }

    pub async fn hash(&self, password: impl Into<Vec<u8>>) -> crate::Result<PasswordHashString> {
        let password = password.into();

        let argon2 = self.argon2.clone();

        self.blocking(move || hash_with_random_salt(&argon2, &password))
            .await?
    }

    pub async fn verify(
        &self,
        password: impl Into<Vec<u8>>,
        hash: impl Into<String>,
    ) -> crate::Result<bool> {
        let password = password.into();
        let hash = hash.into();

        let argon2 = self.argon2.clone();

        self.blocking(move || {
            let hash = PasswordHash::new(&hash)
                .wrap_err_with(|| format!("invalid password hash: {hash:?}"))?;

            match hash.verify_password(&[&*argon2], &password) {
                Ok(()) => Ok(true),
                Err(password_hash::Error::Password) => Ok(false),
                Err(other) => Err(other).wrap_err("error verifying password"),
            }
        })
        .await?
    }

    async fn blocking<R: Send + 'static>(
        &self,
        work: impl FnOnce() -> R + Send + 'static,
    ) -> crate::Result<R> {
        // Acquire the permit _before_ we spawn the task, so it doesn't start a thread unnecessarily.
        let permit = self.semaphore.clone().acquire_owned().await?;

        tokio::task::spawn_blocking(move || {
            // Move `permit` into this scope; it'll be dropped when the task ends.
            let _permit = permit;
            work()
        })
        .await
        .map_err(map_join_error)
    }
}

pub fn init_argon2() -> Argon2<'static> {
    Argon2::default()
}

pub fn hash_with_random_salt(
    argon2: &Argon2<'_>,
    password: &[u8],
) -> crate::Result<PasswordHashString> {
    let salt = SaltString::generate(rand::thread_rng());

    let hash = argon2
        .hash_password(password, &salt)
        .wrap_err("error hashing password")?;

    Ok(hash.serialize())
}
