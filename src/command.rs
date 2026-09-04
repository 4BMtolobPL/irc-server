#[derive(Debug)]
pub(crate) enum Command {
    Join(String),
    List(Option<String>),
    Names(String),
    Nick(String),
    Notice {
        target: String,
        message: String,
    },
    User {
        username: String,
        realname: String,
    },
    Part(String),
    Privmsg {
        target: String,
        message: String,
    },
    Ping(String),
    Pong(String),
    Quit,
    Topic {
        channel: String,
        topic: Option<String>,
    },
    NeedMoreParams(String),
    Unknown(String),
}

pub(crate) fn parse_command(line: &str) -> Command {
    let mut parts = line.splitn(2, char::is_whitespace);

    let command = parts.next().unwrap_or("").to_uppercase();
    let rest = parts.next().unwrap_or("").trim();

    match command.as_str() {
        "LIST" => {
            if rest.is_empty() {
                Command::List(None)
            } else {
                Command::List(Some(rest.to_string()))
            }
        }
        "NAMES" => {
            if rest.is_empty() {
                Command::NeedMoreParams("NAMES".to_string())
            } else {
                Command::Names(rest.to_string())
            }
        }
        "NICK" => {
            if rest.is_empty() {
                Command::NeedMoreParams("NICK".to_string())
            } else {
                Command::Nick(rest.to_string())
            }
        }
        "NOTICE" => {
            let mut parts = rest.splitn(2, char::is_whitespace);

            let target = parts.next().unwrap_or("");

            if target.is_empty() {
                Command::NeedMoreParams("NOTICE".to_string())
            } else {
                let message = parts
                    .next()
                    .unwrap_or("")
                    .trim_start_matches(':')
                    .to_string();

                Command::Notice {
                    target: target.to_string(),
                    message,
                }
            }
        }
        "USER" => {
            let mut parts = rest.splitn(4, char::is_whitespace);

            let username = parts.next().unwrap_or("");
            let hostname = parts.next().unwrap_or("");
            let servername = parts.next().unwrap_or("");
            let realname = parts.next().unwrap_or("");

            if username.is_empty()
                || hostname.is_empty()
                || servername.is_empty()
                || realname.is_empty()
            {
                Command::NeedMoreParams("USER".to_string())
            } else {
                Command::User {
                    username: username.to_string(),
                    realname: realname.trim_start_matches(':').to_string(),
                }
            }
        }
        "JOIN" => {
            if rest.is_empty() {
                Command::NeedMoreParams("JOIN".to_string())
            } else {
                Command::Join(rest.to_string())
            }
        }
        "PART" => {
            if rest.is_empty() {
                Command::NeedMoreParams("PART".to_string())
            } else {
                Command::Part(rest.to_string())
            }
        }
        "PRIVMSG" => {
            let mut parts = rest.splitn(2, char::is_whitespace);

            let target = parts.next().unwrap_or("");
            let message = parts.next().unwrap_or("");

            if target.is_empty() || message.is_empty() {
                Command::NeedMoreParams("PRIVMSG".to_string())
            } else {
                Command::Privmsg {
                    target: target.to_string(),
                    message: message.trim_start_matches(':').to_string(),
                }
            }
        }
        "PING" => Command::Ping(rest.trim_start_matches(':').to_string()),
        "PONG" => Command::Pong(rest.trim_start_matches(':').to_string()),
        "QUIT" => Command::Quit,
        "TOPIC" => match parse_topic(rest) {
            Some((channel, topic)) => Command::Topic { channel, topic },
            None => Command::NeedMoreParams("TOPIC".to_string()),
        },
        _ => Command::Unknown(line.to_string()),
    }
}

fn parse_topic(rest: &str) -> Option<(String, Option<String>)> {
    // TODO: topic이 비어있을때 :가 있으면 Unset, :가 없으면 query
    // 고도화 할때 명세에 따라 변경해야
    let mut parts = rest.splitn(2, ':');

    let channel = parts.next()?.trim();

    if channel.is_empty() {
        return None;
    }

    let topic = parts.next().map(str::to_string);

    Some((channel.to_string(), topic))
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    #[test]
    fn parse_list() {
        let command = parse_command("LIST");

        assert_matches!(command, Command::List(None));
    }

    #[test]
    fn parse_list_with_channel() {
        let command = parse_command("LIST #rust");

        assert_matches!(command, Command::List(Some(channel)) if channel == "#rust");
    }

    #[test]
    fn parse_names_requires_parameter() {
        let command = parse_command("NAMES");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "NAMES");
    }

    #[test]
    fn parse_nick() {
        let command = parse_command("NICK alice");

        assert_matches!(command, Command::Nick(nickname) if nickname == "alice");
    }

    #[test]
    fn parse_nick_requires_parameter() {
        let command = parse_command("NICK");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "NICK");
    }

    #[test]
    fn parse_notice() {
        let command = parse_command("NOTICE bob :hello");

        assert_matches!(command, Command::Notice { target, message } if target == "bob" && message == "hello");
    }

    #[test]
    fn parse_notice_requires_target() {
        let command = parse_command("NOTICE");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "NOTICE");
    }

    #[test]
    fn parse_user() {
        let command = parse_command("USER alice 0 * :Alice Smith");

        assert_matches!(command, Command::User { username, realname } if username == "alice" && realname == "Alice Smith")
    }

    #[test]
    fn parse_user_requires_parameters() {
        let command = parse_command("USER alice");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "USER");
    }

    #[test]
    fn parse_join() {
        let command = parse_command("JOIN #rust");

        assert_matches!(command, Command::Join(channel) if channel == "#rust");
    }

    #[test]
    fn parse_join_requires_parameter() {
        let command = parse_command("JOIN");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "JOIN");
    }

    #[test]
    fn parse_privmsg() {
        let command = parse_command("PRIVMSG #rust :hello");

        assert_matches!(command, Command::Privmsg { target, message } if target == "#rust" && message == "hello");
    }

    #[test]
    fn parse_privmsg_preserves_message_spaces() {
        let command = parse_command("PRIVMSG #rust :hello rust server");

        assert_matches!(command, Command::Privmsg { target, message } if target == "#rust" && message == "hello rust server");
    }

    #[test]
    fn parse_privmsg_is_case_insensitive() {
        let command = parse_command("privmsg #rust :hello");

        assert_matches!(command, Command::Privmsg { target, message } if target == "#rust" && message == "hello");
    }

    #[test]
    fn parse_privmsg_requires_target_and_message() {
        let command = parse_command("PRIVMSG");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "PRIVMSG");
    }

    #[test]
    fn parse_privmsg_requires_message() {
        let command = parse_command("PRIVMSG #rust");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "PRIVMSG");
    }

    #[test]
    fn parse_part() {
        let command = parse_command("PART #rust");

        assert_matches!(command, Command::Part(channel) if channel == "#rust");
    }

    #[test]
    fn parse_part_requires_parameter() {
        let command = parse_command("PART");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "PART");
    }

    #[test]
    fn parse_ping() {
        let command = parse_command("PING :12345");

        assert_matches!(command, Command::Ping(token) if token == "12345")
    }

    #[test]
    fn parse_topic_set() {
        let command = parse_command("TOPIC #rust :Rust programming");

        assert_matches!(command, Command::Topic { channel, topic } if channel == "#rust" && topic == Some("Rust programming".to_string()));
    }

    #[test]
    fn parse_topic_query() {
        let command = parse_command("TOPIC #rust");

        assert_matches!(command, Command::Topic { channel, topic } if channel == "#rust" && topic.is_none());
    }

    #[test]
    fn parse_topic_requires_channel() {
        let command = parse_command("TOPIC");

        assert_matches!(command, Command::NeedMoreParams(command) if command == "TOPIC");
    }

    #[test]
    fn parse_command_is_case_insensitive() {
        let command = parse_command("ping :12345");

        assert_matches!(command, Command::Ping(token) if token == "12345");
    }

    #[test]
    fn parse_command_accepts_whitespace_separator() {
        let command = parse_command("NICK\talice");

        assert_matches!(command, Command::Nick(nickname) if nickname == "alice");
    }
}
