use tokio::sync::mpsc;

#[derive(Debug)]
pub(crate) struct Client {
    pub(crate) nickname: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) realname: Option<String>,
    pub(crate) sender: mpsc::Sender<String>,
}

impl Client {
    pub(crate) fn new(sender: mpsc::Sender<String>) -> Self {
        Self {
            nickname: None,
            username: None,
            realname: None,
            sender,
        }
    }

    /// NICK + USER -> registration
    pub(crate) fn is_registered(&self) -> bool {
        self.nickname.is_some() && self.username.is_some()
    }
}
