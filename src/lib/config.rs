use std::{env, error::Error};
#[cfg(unix)]
use std::{ffi::OsString, path::Path};

use serde::{Deserialize, Serialize};

#[cfg(unix)]
use crate::utils::normalize_path;
use crate::utils::resolve_config_path;

/// # Configuration.
///
/// ## Fields
///
/// - `path`: Path to the unix socket or tcp address.
/// - `host`: Host of the tcp address.
/// - `port`: Port of the tcp address.
/// - `access_key`: Access key for authentication.
/// - `access_secret`: Access secret for authentication.
/// - `retry_times`: Retry times when connection failed.
/// - `retry_interval`: Retry interval when connection failed.
///
/// ## Default values
///
/// - `path`: Empty string.
/// - `host`: "0.0.0.0".
/// - `port`: 60001.
/// - `access_key`: Empty string.
/// - `access_secret`: Empty string.
/// - `retry_times`: 3.
/// - `retry_interval`: 60.
///
/// ## Environment variables (when config file not exists, cover default values)
///
/// - `AMQ_PATH`: Path to the unix socket or tcp address.
/// - `AMQ_HOST`: Host of the tcp address.
/// - `AMQ_PORT`: Port of the tcp address.
/// - `AMQ_ACCESS_KEY`: Access key for authentication.
/// - `AMQ_ACCESS_SECRET`: Access secret for authentication.
/// - `AMQ_RETRY_TIMES`: Retry times when connection failed.
/// - `AMQ_RETRY_INTERVAL`: Retry interval when connection failed.
///
/// ## Example
///
/// ```toml
/// path = "/tmp/amq.sock"  # use host and port if path is empty
/// host = "127.0.0.1"      # ignored if path is not empty
/// port = 60001            # ignored if path is not empty
/// access_key = "access_key"
/// access_secret = "access_secret"
/// retry_times = 3
/// retry_interval = 60
/// ```
///
/// ```bash
/// export AMQ_PATH="/tmp/amq.sock"
/// export AMQ_HOST="127.0.0.1"
/// export AMQ_PORT=60001
/// export AMQ_ACCESS_KEY="access_key"
/// export AMQ_ACCESS_SECRET="access_secret"
/// export AMQ_RETRY_TIMES=3
/// export AMQ_RETRY_INTERVAL=60
/// ```
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
    /// # Create a new configuration.
    ///
    /// 1. use the first argument as config file path, if exists.
    /// 2. use the `./config.toml` as config file path.
    /// 3. use environment variables to cover default values.
    /// 4. use default values.
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
                    Ok(toml::from_str::<Config>(&content)?)
                }
            }
            Err(e) => {
                panic!("Failed to resolve config file path: {}", e);
            }
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
        let mut c = Self {
            path: default_string(),
            host: default_host(),
            port: default_port(),
            access_key: default_string(),
            access_secret: default_string(),
            retry_times: default_retry_times(),
            retry_interval: default_retry_interval(),
        };
        // AMQ_PATH
        if let Ok(path) = std::env::var("AMQ_PATH") {
            c.path = path;
        }
        // AMQ_HOST
        if let Ok(host) = std::env::var("AMQ_HOST") {
            c.host = host;
        }
        // AMQ_PORT
        if let Ok(port) = std::env::var("AMQ_PORT") {
            c.port = port.parse().unwrap();
        }
        // AMQ_ACCESS_KEY
        if let Ok(key) = std::env::var("AMQ_ACCESS_KEY") {
            c.access_key = key;
        }
        // AMQ_ACCESS_SECRET
        if let Ok(secret) = std::env::var("AMQ_ACCESS_SECRET") {
            c.access_secret = secret;
        }
        // AMQ_RETRY_TIMES
        if let Ok(times) = std::env::var("AMQ_RETRY_TIMES") {
            c.retry_times = times.parse().unwrap();
        }
        // AMQ_RETRY_INTERVAL
        if let Ok(interval) = std::env::var("AMQ_RETRY_INTERVAL") {
            c.retry_interval = interval.parse().unwrap();
        }
        c
    }
}
