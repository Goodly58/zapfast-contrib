//! Sends of ours that are running, so that one cut off can be retried.
//!
//! A message we send is filed as pending before it goes out, and only the
//! running send can move it on. When ZapFast quits during a send, nothing
//! would: the message would wait as pending for good. Each running send is
//! noted here until it finishes, so the next start can tell the ones that
//! were cut off and show them as not sent, ready to be sent again.

use rusqlite::{OptionalExtension, params};

use super::{Archive, Batch, Result, status_rank};
use crate::model::Delivery;

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sending (
    chat TEXT NOT NULL,
    id TEXT NOT NULL,
    PRIMARY KEY (chat, id)
);
";

impl Archive {
    /// Notes that a send of this message of ours is running.
    pub fn note_sending(&self, chat: &str, id: &str) -> Result<()> {
        self.connection.execute(
            "INSERT OR IGNORE INTO sending (chat, id) VALUES (?1, ?2)",
            params![chat, id],
        )?;
        Ok(())
    }

    /// Forgets a send once it has finished, sent or failed.
    pub fn sending_finished(&self, chat: &str, id: &str) -> Result<()> {
        self.connection.execute(
            "DELETE FROM sending WHERE chat = ?1 AND id = ?2",
            params![chat, id],
        )?;
        Ok(())
    }

    /// Marks our messages whose send was cut off as failed, and returns how
    /// many there were. Only for startup, while no send can be running.
    ///
    /// Only noted sends count: a message the phone still had pending when
    /// history was synced is the phone's to send.
    pub fn fail_cut_off_sends(&self) -> Result<usize> {
        let transaction = Batch::begin(&self.connection)?;
        let failed = transaction.execute(
            "UPDATE messages SET status = ?1
             WHERE from_me = 1 AND status = ?2 AND EXISTS (
                 SELECT 1 FROM sending
                 WHERE sending.chat = messages.chat AND sending.id = messages.id
             )",
            params![
                status_rank(Delivery::Failed),
                status_rank(Delivery::Pending)
            ],
        )?;
        transaction.execute("DELETE FROM sending", [])?;
        transaction.commit()?;
        Ok(failed)
    }

    /// Readies one of our messages that was not sent for another send: it is
    /// pending again and noted as sending. A group's members that never
    /// received it are dropped, so the new send's audience replaces them.
    /// Returns false, changing nothing, unless it is ours and failed.
    pub fn retry_send(&self, chat: &str, id: &str) -> Result<bool> {
        let transaction = Batch::begin(&self.connection)?;
        let failed = transaction
            .query_row(
                "SELECT 1 FROM messages
                 WHERE chat = ?1 AND id = ?2 AND from_me = 1 AND status = ?3",
                params![chat, id, status_rank(Delivery::Failed)],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !failed {
            return Ok(false);
        }
        transaction.execute(
            "UPDATE messages SET status = ?3 WHERE chat = ?1 AND id = ?2",
            params![chat, id, status_rank(Delivery::Pending)],
        )?;
        transaction.execute(
            "DELETE FROM group_receipts WHERE chat = ?1 AND id = ?2 AND status = 0",
            params![chat, id],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO sending (chat, id) VALUES (?1, ?2)",
            params![chat, id],
        )?;
        transaction.commit()?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::tests::message;

    const GROUP: &str = "120363000000000001@g.us";

    fn status(archive: &Archive, chat: &str, id: &str) -> Delivery {
        archive
            .message(chat, id)
            .expect("read")
            .expect("stored")
            .status
    }

    #[test]
    fn a_send_cut_off_shows_as_not_sent_at_the_next_start() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("archive.db");
        {
            let archive = Archive::open_with_key(&path, &[7; 32]).expect("open");
            for id in ["cut", "finished", "theirs"] {
                archive
                    .insert_message(&message(GROUP, id, 100, true), None)
                    .expect("insert");
            }
            archive.note_sending(GROUP, "cut").expect("note");
            archive.note_sending(GROUP, "finished").expect("note");
            archive
                .set_status(GROUP, "finished", Delivery::Sent, 101)
                .expect("sent");
            archive.sending_finished(GROUP, "finished").expect("done");
        }
        let archive = Archive::open_with_key(&path, &[7; 32]).expect("reopen");
        assert_eq!(archive.fail_cut_off_sends().expect("fail"), 1);
        assert_eq!(status(&archive, GROUP, "cut"), Delivery::Failed);
        assert_eq!(status(&archive, GROUP, "finished"), Delivery::Sent);
        assert_eq!(
            status(&archive, GROUP, "theirs"),
            Delivery::Pending,
            "a pending message no send of ours noted is the phone's"
        );
        assert_eq!(
            archive.fail_cut_off_sends().expect("again"),
            0,
            "the notes go with the start that read them"
        );
    }

    #[test]
    fn a_retry_makes_a_failed_message_pending_with_a_fresh_audience() {
        let archive = Archive::in_memory().expect("opens");
        archive
            .insert_message(&message(GROUP, "m", 100, true), None)
            .expect("insert");
        archive
            .snapshot_group_recipients(GROUP, "m", &["a".into(), "b".into()])
            .expect("audience");
        archive
            .group_receipt(GROUP, "m", "a", Delivery::Delivered, 102)
            .expect("receipt");
        assert!(
            !archive.retry_send(GROUP, "m").expect("retry"),
            "a message still sending is not retried"
        );
        archive
            .set_status(GROUP, "m", Delivery::Failed, 103)
            .expect("failed");

        assert!(archive.retry_send(GROUP, "m").expect("retry"));
        assert_eq!(status(&archive, GROUP, "m"), Delivery::Pending);
        let receipts = archive.receipts(GROUP, "m").expect("receipts");
        let recipients: Vec<&str> = receipts.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            recipients,
            ["a"],
            "a member who received it stays, one who never did goes"
        );
        assert_eq!(archive.fail_cut_off_sends().expect("cut off"), 1);
        assert_eq!(
            status(&archive, GROUP, "m"),
            Delivery::Failed,
            "the retry is noted as a running send"
        );
    }

    #[test]
    fn only_our_failed_messages_are_retried() {
        let archive = Archive::in_memory().expect("opens");
        let mut incoming = message(GROUP, "in", 100, false);
        incoming.status = Delivery::Failed;
        archive.insert_message(&incoming, None).expect("insert");
        assert!(!archive.retry_send(GROUP, "in").expect("retry"));
        assert!(!archive.retry_send(GROUP, "missing").expect("retry"));
    }
}
