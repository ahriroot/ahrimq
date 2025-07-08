use std::{env, error::Error, ffi::OsString, path::Path};

use serde::{Deserialize, Serialize};

use amq::utils::{normalize_path, resolve_config_path};

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    #[serde(default = "default_string")]
    pub path: String,

    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_string")]
    pub access_key: String,

    #[serde(default = "default_string")]
    pub access_secret: String,

    #[serde(default = "default_retry_times")]
    pub retry_times: u8,

    #[serde(default = "default_retry_interval")]
    pub retry_interval: u64,
}

fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    60001
}

fn default_string() -> String {
    "".into()
}

fn default_retry_times() -> u8 {
    3
}

fn default_retry_interval() -> u64 {
    60
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

    #[cfg(unix)]
    pub fn get_unix_path(&self) -> String {
        let path = self.path.trim();
        if path.starts_with("./") {
            let current_dir = env::current_dir().unwrap();
            let abs_path = current_dir.join(path);
            let abs_path = normalize_path(&abs_path);
            abs_path.to_str().unwrap().to_string()
        } else if path.starts_with("~") {
            let home_dir = env::var_os("HOME")
                .or_else(|| env::var_os("USERPROFILE")) // Windows 兼容
                .unwrap_or(OsString::from("./"));
            let abs_path = Path::new(&home_dir).join(path.trim_start_matches("~/"));
            abs_path.to_str().unwrap().to_string()
        } else {
            path.to_string()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: default_string(),
            host: default_host(),
            port: default_port(),
            access_key: default_string(),
            access_secret: default_string(),
            retry_times: default_retry_times(),
            retry_interval: default_retry_interval(),
        }
    }
}
