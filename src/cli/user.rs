use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use color_eyre::eyre::WrapErr;
use rand::seq::IteratorRandom;
use tashi_collections::HashMap;

use crate::cli::LogFormat;
use crate::config::users::{AuthConfig, User, UsersConfig};

const DEFAULT_PASSWORD_LEN: usize = 12;

const PASSWORD_CHARS: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ@#$%-+";

const DEFAULT_OUTPUT_FILE: &str = "dmq/users.toml";

#[derive(clap::Args, Clone, Debug)]
pub struct UserArgs {
    #[command(subcommand)]
    command: UserCommand,
}

#[derive(clap::Subcommand, Clone, Debug)]
enum UserCommand {
    /// Add a user record to the credentials file, creating it if it doesn't exist.
    ///
    /// If a username and/or password are not provided, you will be prompted for them interactively.
    Add(AddUserArgs),
    /// Generate a user record and return it on stdout.
    ///
    /// If a password is not provided, it will be read from the first line piped to stdin.
    Generate {
        #[clap(short, long, default_value = "full")]
        log: LogFormat,

        /// The username of the user. Must be unique within the users file.
        username: String,

        /// The password for the user.
        ///
        /// If omitted, the password will be read from the first line on stdin.
        password: Option<String>,
    },
}

#[derive(clap::Args, Clone, Debug)]
struct AddUserArgs {
    #[clap(short, long, default_value = "full")]
    log: LogFormat,

    /// The file to write the generated user record to.
    ///
    /// If any parent directories in the path do not already exist, they will be created.
    #[clap(long, short = 'O', default_value = "dmq/users.toml")]
    output_file: Option<PathBuf>,

    /// Controls how this command writes to `--output-file`.
    #[clap(long, short = 'm', default_value = "append")]
    write_mode: WriteMode,

    /// The username of the user. Must be unique within the users file.
    ///
    /// If omitted, you will be prompted to enter it interactively.
    username: Option<String>,

    /// The password for the user.
    ///
    /// If omitted, you will be prompted to enter it interactively, or be given the option
    /// to generate a random one.
    password: Option<String>,
}

#[derive(clap::ValueEnum, Clone, Debug, num_enum::TryFromPrimitive)]
#[repr(usize)]
enum WriteMode {
    /// Append to the file if it already exists, or create it if it doesn't.
    Append,
    /// Truncate the file if it already exists, or create it if it doesn't.
    Truncate,
}

impl UserArgs {
    pub fn log_format(&self) -> LogFormat {
        match &self.command {
            UserCommand::Add(args) => args.log,
            UserCommand::Generate { log, .. } => *log,
        }
    }
}

pub fn main(args: UserArgs) -> crate::Result<()> {
    match args.command {
        UserCommand::Add(args) => add_user_interactively(args),
        UserCommand::Generate {
            log: _log,
            username,
            password,
        } => {
            let password = match password {
                Some(password) => password,
                None => {
                    let mut password = String::new();
                    std::io::stdin()
                        .lock()
                        .read_line(&mut password)
                        .wrap_err("error reading from stdin")?;

                    // `.read_line()` includes the newline, but that's not part of the password
                    // WTB something like `.trim_end()` that operates on an owned string
                    while matches!(password.chars().last(), Some('\r') | Some('\n')) {
                        password.pop();
                    }

                    password
                }
            };

            generate_user_record(username, password.as_bytes(), std::io::stdout().lock())
        }
    }
}

fn add_user_interactively(args: AddUserArgs) -> crate::Result<()> {
    let term = dialoguer::console::Term::stderr();

    let username = match args.username {
        Some(username) => username,
        None => dialoguer::Input::new()
            .with_prompt("Enter username")
            .interact_on(&term)?,
    };

    let password = match args.password {
        Some(password) => {
            eprintln!("Using password provided on the command-line.");
            password
        }
        None => {
            let mut password = dialoguer::Password::new()
                .with_prompt(format!(
                    "Enter password for {username} \
                     (press RETURN without inputting a password to generate a random one)",
                ))
                .allow_empty_password(true)
                .interact_on(&term)?;

            let prompt = if password.is_empty() {
                // Prompt the user to generate a password.

                let password_len: usize = dialoguer::Input::new()
                    .with_prompt("Enter the length of password to generate")
                    .default(DEFAULT_PASSWORD_LEN)
                    .interact_on(&term)?;

                password.extend(
                    PASSWORD_CHARS
                        .chars()
                        .choose_multiple(&mut rand::thread_rng(), password_len),
                );

                format!("Enter generated password {password:?} to confirm (note: this line will be erased from the console)")
            } else {
                "Enter password again to confirm".to_string()
            };

            // We don't actually care what this second input is, we're just confirming.
            dialoguer::Password::new()
                .with_prompt(prompt)
                .allow_empty_password(false)
                .validate_with(|entered_pw: &String| {
                    if entered_pw == &password {
                        Ok(())
                    } else {
                        Err("passwords do not match")
                    }
                })
                .interact_on(&term)?;

            term.clear_last_lines(2)?;

            password
        }
    };

    let output_file = match args.output_file {
        Some(path) => path,
        None => {
            let path: String = dialoguer::Input::new()
                .with_prompt("Enter output file (use \"-\" to write to stdout)")
                .default(DEFAULT_OUTPUT_FILE.to_string())
                .interact_on(&term)?;

            path.into()
        }
    };

    if output_file == Path::new("-") {
        generate_user_record(username, password.as_bytes(), std::io::stdout().lock())?;
        return Ok(());
    }

    if let Some(parent_dir) = output_file.parent() {
        // Create any parent directories that don't already exist.
        fs::create_dir_all(parent_dir).wrap_err_with(|| {
            format!("error creating parent directory: {}", parent_dir.display())
        })?;
    }

    // It didn't seem important for `write_mode` to be prompted for interactively.
    //
    // The default of `append` should almost always be suitable.
    let mut file = match args.write_mode {
        WriteMode::Append => OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_file),
        WriteMode::Truncate => File::create(&output_file),
    }
    .wrap_err_with(|| format!("error opening {} for writing", output_file.display()))?;

    if file.metadata().is_ok_and(|meta| meta.len() > 0) {
        // If the file is not empty, add a blank line before the entry for readability.
        file.write_all(b"\n")?;
    }

    generate_user_record(username, password.as_bytes(), file)?;

    eprintln!("User record written to {}", output_file.display());

    Ok(())
}

fn generate_user_record(
    username: String,
    password: &[u8],
    mut output: impl Write,
) -> crate::Result<()> {
    let argon2 = crate::password::init_argon2();

    let password_hash = crate::password::hash_with_random_salt(&argon2, password)
        .wrap_err("error hashing password")?
        .to_string();

    let users_toml = toml::to_string(&UsersConfig {
        by_username: HashMap::from_iter([(username, User { password_hash })]),
        auth: AuthConfig::default(),
    })
    .wrap_err("error serializing user record to TOML")?;

    // The TOML has a trailing newline already.
    output.write_all(users_toml.as_bytes())?;

    Ok(())
}
