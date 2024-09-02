/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
#[derive(thiserror::Error, Debug)]
pub enum Error {}

#[derive(Debug)]
pub struct Service {
    endpoint: Option<String>,
    pub channel: Option<tonic::transport::Channel>,
}

impl Service {
    pub async fn connect(mut self) -> Result<Service, Box<dyn std::error::Error>> {
        if let Some(endpoint) = self.endpoint.take() {
            let tls_config = tonic::transport::ClientTlsConfig::new();
            let channel = tonic::transport::Channel::from_shared(endpoint)?
                .tls_config(tls_config)?
                .connect()
                .await?;
            self.channel = Some(channel);
        }
        Ok(self)
    }
}

#[derive(Default)]
pub struct ServiceBuilder {
    endpoint: Option<String>,
}

impl ServiceBuilder {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Self {
        ServiceBuilder {
            ..Default::default()
        }
    }

    pub fn with_endpoint(&mut self, endpoint: String) -> &mut Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn build(&mut self) -> Result<Service, Error> {
        Ok(Service {
            endpoint: self.endpoint.take(),
            channel: None,
        })
    }
}
