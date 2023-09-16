use configparser::ini::Ini;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug)]
pub struct Config {
    pub unix_socket_path: PathBuf,
    pub databases_dir: PathBuf,
    pub cleanup_age: Option<Duration>,
}

#[allow(unused)]
pub enum ConfigSource<T> {
    File(T),
    Text(T),
}

#[derive(Debug)]
pub enum ParsingError {
    IniError(String),
    ConfError(String),
}

impl From<String> for ParsingError {
    fn from(value: String) -> Self {
        ParsingError::IniError(value)
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

impl TryFrom<ConfigSource<String>> for Config {
    type Error = ParsingError;

    fn try_from(v: ConfigSource<String>) -> Result<Self, ParsingError> {
        let mut config = Ini::new();
        match v {
            ConfigSource::File(f) => config.load(f)?,
            ConfigSource::Text(t) => config.read(t)?,
        };

        let c = Config {
            unix_socket_path: get_dir(&config, "general", "plugins_dir", "/run/docker/plugins/")?
                .join("logsqlite.sock"),
            databases_dir: get_dir(&config, "general", "databases_dir", "/var/spool/logsqlite/")?,
            cleanup_age: config
                .getuint("general", "cleanup_age")?
                .map(Duration::from_secs),
        };

        Ok(c)
    }
}
