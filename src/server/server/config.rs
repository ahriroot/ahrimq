use std::{env, error::Error};

use serde::{Deserialize, Serialize};

use amq::utils::resolve_config_path;

#[derive(Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_host")]
    host: String,

    #[serde(default = "default_port")]
    port: u16,
}

fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    60001
}

impl Config {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let args: Vec<String> = env::args().collect();
        let config_file = if args.len() > 1 {
            &args[1]
        } else {
            "./config.toml"
        };

        match resolve_config_path(config_file) {
            Ok(abs_path) => {
                if !abs_path.exists() || !abs_path.is_file() {
                    Ok(Self::default())
                } else {
                    let content = std::fs::read_to_string(abs_path)?;
                    let mut config = toml::from_str::<Config>(&content)?;
                    config.parse_env_var();
                    Ok(config)
                }
            }
            Err(e) => {
                panic!("Failed to resolve config file path: {}", e);
            }
        }
    }

    pub fn parse_env_var(&mut self) {
        if let Ok(host) = std::env::var("AMQ_HOST") {
            self.host = host;
        }
        if let Ok(port) = std::env::var("AMQ_PORT") {
            self.port = port.parse().unwrap();
        }
    }

    pub fn get_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}
