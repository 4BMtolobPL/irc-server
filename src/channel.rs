use std::collections::HashSet;

use crate::ClientId;

#[derive(Debug)]
pub(crate) struct Channel {
    pub(crate) name: String,
    pub(crate) topic: Option<String>,
    pub(crate) members: HashSet<ClientId>,
}

impl Channel {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            topic: None,
            members: HashSet::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_channel_has_no_topic() {
        let channel = Channel::new("#rust");

        assert_eq!(channel.topic, None);
    }
}
