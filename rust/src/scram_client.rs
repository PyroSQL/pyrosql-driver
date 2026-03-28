//! SCRAM-SHA-256 client implementation (RFC 5802).
//!
//! Implements the client side of the SASL SCRAM-SHA-256 authentication
//! mechanism used by PostgreSQL 10+ and PyroSQL.

use crate::error::ClientError;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use ring::{digest, hmac, pbkdf2, rand as ring_rand};

/// Generate the client-first-message for SCRAM-SHA-256.
///
/// Returns `(client_first_message, client_first_bare, client_nonce)`.
/// - `client_first_message` is the full GS2 header + bare message: `"n,,n=<user>,r=<nonce>"`
/// - `client_first_bare` is the part without the GS2 header: `"n=<user>,r=<nonce>"`
/// - `client_nonce` is the raw nonce string for later verification
pub fn scram_client_first(user: &str) -> (String, String, String) {
    // Generate 18 random bytes and base64-encode for the nonce.
    let mut nonce_bytes = [0u8; 18];
    let rng = ring_rand::SystemRandom::new();
    ring_rand::SecureRandom::fill(&rng, &mut nonce_bytes)
        .expect("system RNG failed");
    let client_nonce = BASE64.encode(nonce_bytes);

    let client_first_bare = format!("n={user},r={client_nonce}");
    let client_first_message = format!("n,,{client_first_bare}");

    (client_first_message, client_first_bare, client_nonce)
}

/// Parse the server-first-message and compute the client-final-message.
///
/// Returns `(client_final_message, expected_server_signature)`.
pub fn scram_client_final(
    password: &str,
    client_first_bare: &str,
    server_first: &str,
    client_nonce: &str,
) -> Result<(String, Vec<u8>), ClientError> {
    // Parse server-first-message: r=<nonce>,s=<salt>,i=<iterations>
    let mut combined_nonce = None;
    let mut salt_b64 = None;
    let mut iterations = None;

    for part in server_first.split(',') {
        if let Some(val) = part.strip_prefix("r=") {
            combined_nonce = Some(val);
        } else if let Some(val) = part.strip_prefix("s=") {
            salt_b64 = Some(val);
        } else if let Some(val) = part.strip_prefix("i=") {
            iterations = Some(val.parse::<u32>().map_err(|_| {
                ClientError::Protocol(format!("SCRAM: invalid iteration count: {val}"))
            })?);
        }
    }

    let combined_nonce = combined_nonce.ok_or_else(|| {
        ClientError::Protocol("SCRAM: server-first missing nonce (r=)".into())
    })?;
    let salt_b64 = salt_b64.ok_or_else(|| {
        ClientError::Protocol("SCRAM: server-first missing salt (s=)".into())
    })?;
    let iterations = iterations.ok_or_else(|| {
        ClientError::Protocol("SCRAM: server-first missing iterations (i=)".into())
    })?;

    // Verify server nonce starts with our client nonce
    if !combined_nonce.starts_with(client_nonce) {
        return Err(ClientError::Protocol(
            "SCRAM: server nonce does not start with client nonce".into(),
        ));
    }

    let salt = BASE64.decode(salt_b64).map_err(|e| {
        ClientError::Protocol(format!("SCRAM: invalid base64 salt: {e}"))
    })?;

    let iterations_nonzero = std::num::NonZeroU32::new(iterations).ok_or_else(|| {
        ClientError::Protocol("SCRAM: iteration count must be > 0".into())
    })?;

    // client-final-without-proof
    // c= is base64("n,,") = "biws", r= is the combined nonce
    let client_final_without_proof = format!("c=biws,r={combined_nonce}");

    // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");

    // SaltedPassword = PBKDF2-SHA256(password, salt, iterations, 32)
    let mut salted_password = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations_nonzero,
        &salt,
        password.as_bytes(),
        &mut salted_password,
    );

    // ClientKey = HMAC-SHA256(SaltedPassword, "Client Key")
    let client_key_hmac = hmac::Key::new(hmac::HMAC_SHA256, &salted_password);
    let client_key = hmac::sign(&client_key_hmac, b"Client Key");

    // StoredKey = SHA-256(ClientKey)
    let stored_key = digest::digest(&digest::SHA256, client_key.as_ref());

    // ClientSignature = HMAC-SHA256(StoredKey, AuthMessage)
    let stored_key_hmac = hmac::Key::new(hmac::HMAC_SHA256, stored_key.as_ref());
    let client_signature = hmac::sign(&stored_key_hmac, auth_message.as_bytes());

    // ClientProof = ClientKey XOR ClientSignature
    let mut client_proof = [0u8; 32];
    for i in 0..32 {
        client_proof[i] = client_key.as_ref()[i] ^ client_signature.as_ref()[i];
    }

    let client_final = format!(
        "{client_final_without_proof},p={}",
        BASE64.encode(client_proof)
    );

    // ServerKey = HMAC-SHA256(SaltedPassword, "Server Key")
    let server_key_hmac = hmac::Key::new(hmac::HMAC_SHA256, &salted_password);
    let server_key = hmac::sign(&server_key_hmac, b"Server Key");

    // ServerSignature = HMAC-SHA256(ServerKey, AuthMessage)
    let server_sig_hmac = hmac::Key::new(hmac::HMAC_SHA256, server_key.as_ref());
    let expected_server_sig = hmac::sign(&server_sig_hmac, auth_message.as_bytes());

    Ok((client_final, expected_server_sig.as_ref().to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_first_format() {
        let (msg, bare, nonce) = scram_client_first("testuser");
        assert!(msg.starts_with("n,,n=testuser,r="));
        assert!(bare.starts_with("n=testuser,r="));
        assert_eq!(msg, format!("n,,{bare}"));
        assert!(!nonce.is_empty());
    }

    #[test]
    fn round_trip_known_vectors() {
        // We can at least verify the function doesn't panic and produces
        // correctly formatted output.
        let (_msg, bare, nonce) = scram_client_first("user");
        let fake_server_first = format!(
            "r={nonce}SERVERNONCE,s={},i=4096",
            BASE64.encode(b"randomsalt")
        );
        let result = scram_client_final("password", &bare, &fake_server_first, &nonce);
        assert!(result.is_ok());
        let (client_final, server_sig) = result.unwrap();
        assert!(client_final.starts_with("c=biws,r="));
        assert!(client_final.contains(",p="));
        assert_eq!(server_sig.len(), 32);
    }

    #[test]
    fn rejects_bad_nonce() {
        let (_msg, bare, _nonce) = scram_client_first("user");
        let fake_server_first = format!(
            "r=COMPLETELYDIFFERENT,s={},i=4096",
            BASE64.encode(b"salt")
        );
        let result = scram_client_final("password", &bare, &fake_server_first, "originalnonce");
        assert!(result.is_err());
    }
}
