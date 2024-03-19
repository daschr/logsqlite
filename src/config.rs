use configparser::ini::Ini;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug)]
pub struct Config {
    pub unix_socket_path: PathBuf,
    pub databases_dir: PathBuf,
    pub state_database: PathBuf,
    pub cleanup_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    pub max_lines_per_tx: u64,
    pub max_size_per_tx: usize, // bytes
    pub message_read_timeout: Duration,
    pub cleanup_age: Option<Duration>,
    pub cleanup_max_lines: Option<u64>,
    pub cleanup_interval: Duration,
    pub delete_when_stopped: bool,
}

#[allow(unused)]
pub enum ConfigSource<T> {
    File(T),
    Text(T),
}

#[derive(Debug, Serialize)]
pub enum ParsingError {
    IniError(String),
    ConfError(String),
}

impl From<String> for ParsingError {
    fn from(value: String) -> Self {
        ParsingError::IniError(value)
    }
}

impl From<ParseIntError> for ParsingError {
    fn from(value: ParseIntError) -> Self {
        ParsingError::ConfError(value.to_string())
    }
}

fn get_dir<T: Into<PathBuf>>(
    conf: &Ini,
    section: &str,
    name: &str,
    default: T,
) -> Result<PathBuf, ParsingError> {
    match conf.get(section, name) {
        Some(s) => {
            let p = PathBuf::from(s.as_str());
            if !p.exists() {
                return Err(ParsingError::ConfError(format!("{s} does not exist")));
            }

            if !p.is_dir() {
                return Err(ParsingError::ConfError(format!("{s} is not a directory")));
            }

            Ok(p)
        }
        None => Ok(default.into()),
    }
}

fn parse_as_duration(v: &str) -> Result<Duration, String> {
    let pos = {
        let mut r = 0;
        for c in v.chars() {
            if !c.is_ascii_digit() {
                break;
            }
            r += 1;
        }
        r
    };

    if pos == 0 {
        return Err(String::from("Cannot parse time: no number"));
    }

    let mut num: u64 = v[0..pos].parse::<u64>().unwrap();
    match &v[pos..] {
        "w" | "W" => num *= 7 * 24 * 60 * 60,
        "d" | "D" => num *= 24 * 60 * 60,
        "h" | "H" => num *= 60 * 60,
        "m" | "M" => num *= 60,
        "s" | "S" => (),
        s => {
            return Err(format!("Unknown time specifier \"{}\"", s));
        }
    }

    Ok(Duration::from_secs(num))
}

fn parse_si_prefixed_size(v: &str) -> Result<usize, String> {
    let pos = {
        let mut r = 0;
        for c in v.chars() {
            if !c.is_ascii_digit() {
                break;
            }
            r += 1;
        }
        r
    };

    if pos == 0 {
        return Err(String::from("Cannot parse size: no number"));
    }

    let mut num: u64 = v[0..pos].parse::<u64>().unwrap();
    match &v[pos..] {
        "g" | "G" => num *= 1024 * 1024 * 1024,
        "m" | "M" => num *= 1024 * 1024,
        "k" | "K" => num *= 1024,
        "b" | "B" => (),
        s => {
            return Err(format!("Unknown time specifier \"{}\"", s));
        }
    }

    Ok(num as usize)
}

impl TryFrom<ConfigSource<String>> for Config {
    type Error = ParsingError;

    fn try_from(v: ConfigSource<String>) -> Result<Self, ParsingError> {
        let mut config = Ini::new();
        match v {
            ConfigSource::File(f) => config.load(f)?,
            ConfigSource::Text(t) => config.read(t)?,
        };

        let c = Config {
            unix_socket_path: get_dir(
                &config,
                "general",
                "plugins_dir",
                "/var/run/docker/plugins/",
            )?
            .join("logsqlite.sock"),
            databases_dir: get_dir(&config, "general", "databases_dir", "/var/spool/logsqlite/")?,
            state_database: match config.get("general", "state_database") {
                Some(p) => PathBuf::from(p),
                None => {
                    return Err(ParsingError::ConfError(String::from(
                        "Missing \"state_database\"",
                    )))
                }
            },
            cleanup_interval: config
                .getuint("cleanup", "interval")?
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(30 * 60)),
        };

        Ok(c)
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            max_lines_per_tx: 10_000,
            max_size_per_tx: 10 * 1024 * 1024,
            message_read_timeout: Duration::from_millis(100),
            cleanup_age: None,
            cleanup_max_lines: Some(10_000_000),
            cleanup_interval: Duration::from_secs(10 * 60),
            delete_when_stopped: true,
        }
    }
}

impl TryFrom<&Option<HashMap<String, String>>> for LogConfig {
    type Error = ParsingError;

    fn try_from(cust_conf: &Option<HashMap<String, String>>) -> Result<Self, Self::Error> {
        let mut conf = LogConfig::default();

        if cust_conf.is_none() {
            return Ok(conf);
        }

        let cust_conf = cust_conf.as_ref().unwrap();
        for (opt, val) in cust_conf.iter() {
            match opt.as_str() {
                "message_read_timeout" => {
                    conf.message_read_timeout = Duration::from_millis(val.parse::<u64>()?);
                }
                "max_lines_per_tx" => {
                    conf.max_lines_per_tx = val.parse::<u64>()?;
                }
                "max_size_per_tx" => {
                    conf.max_size_per_tx = parse_si_prefixed_size(val)?;
                }
                "cleanup_age" => {
                    conf.cleanup_age = Some(parse_as_duration(val)?);
                }
                "cleanup_max_lines" => {
                    conf.cleanup_max_lines = Some(val.parse::<u64>()?);
                }
                "delete_when_stopped" => {
                    conf.delete_when_stopped = match val.to_lowercase().as_str() {
                        "true" => true,
                        "false" => false,
                        _ => {
                            return Err(ParsingError::ConfError(String::from(
                                "delete_when_stopped is neither \"true\" or \"false\"",
                            )));
                        }
                    }
                }
                _ => (),
            }
        }

        Ok(conf)
    }
}
