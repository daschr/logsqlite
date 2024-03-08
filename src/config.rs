use configparser::ini::Ini;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug)]
pub struct Config {
    pub unix_socket_path: PathBuf,
    pub databases_dir: PathBuf,
    pub state_database: PathBuf,
    pub cleanup_age: Option<Duration>,
    pub cleanup_interval: Duration,
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
            cleanup_age: match config.get("cleanup", "age") {
                Some(s) => Some(parse_as_duration(s.as_str())?),
                None => None,
            },
            cleanup_interval: config
                .getuint("cleanup", "interval")?
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(30 * 60)),
        };

        Ok(c)
    }
}
