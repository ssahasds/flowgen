use std::path::PathBuf;

const DEFAULT_SCOPES: &str = "https://www.googleapis.com/auth/cloud-platform";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse the credentials file")]
    ParseCredentials(#[source] serde_json::Error),
    #[error("Cannot parse url")]
    ParseUrl(#[source] url::ParseError),
    #[error("Other error: {}", _0)]
    Other(String),
}

/// Used to store GCP client.
#[allow(clippy::type_complexity)]
pub struct Client {
    /// Scopes used GCP authorization.
    scopes: Vec<String>,
    /// Token result as a composite of access_token / refresh__token etc.
    pub token_result: Option<google_cloud_auth::AccessToken>,
}

impl flowgen_core::client::Client for Client {
    type Error = Error;
    /// Authorizes to GCP based on provided credentials.
    async fn connect(mut self) -> Result<Self, Error> {
        let credential_config = google_cloud_auth::CredentialConfigBuilder::new()
            .scopes(self.scopes.clone())
            .build()
            .unwrap();

        let token_result = google_cloud_auth::Credential::find_default(credential_config)
            .await
            .unwrap()
            .access_token()
            .await
            .unwrap();

        self.token_result = Some(token_result);
        Ok(self)
    }
}

#[derive(Default)]
/// Used to store GCP client configuration.
pub struct Builder {
    credentials_path: PathBuf,
}

impl Builder {
    #[allow(clippy::new_ret_no_self)]
    /// Creates a new instance of a Builder.
    pub fn new() -> Builder {
        Builder::default()
    }
    /// Pass path to the fail so that credentials can be loaded.
    pub fn with_credentials_path(&mut self, credentials_path: PathBuf) -> &mut Builder {
        self.credentials_path = credentials_path;
        self
    }

    /// Generates a new client or return error in case
    /// provided credentials path is not valid.
    pub fn build(&self) -> Result<Client, Error> {
        std::env::set_var(
            "GOOGLE_APPLICATION_CREDENTIALS",
            self.credentials_path.clone(),
        );

        Ok(Client {
            scopes: vec![DEFAULT_SCOPES.to_string()],
            token_result: None,
        })
    }
}
