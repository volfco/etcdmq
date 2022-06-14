use anyhow::bail;
use chrono::Utc;
use etcd_client::{
    Client, EventType, GetOptions, SortOrder, SortTarget,
    WatchOptions,
};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc};
use tokio::sync::Mutex;
use tracing::{debug, instrument, trace, warn, Instrument};
use uuid::Uuid;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageState {
    /// Task has been created
    ENQUEUED,
    /// Task has been consumed, and returned to the client
    CONSUMED,
    /// Task has
    FINISHED(Option<Result<Vec<u8>, Vec<u8>>>),
    /// Task has been lost, and needs to be re-scheduled
    LOST,
    /// Task needs to be re-scheduled
    PENDING(u8),
}

// pub enum Constraints {
//     Any(Vec<(Constraint, Conditional, ConditionalValue)>),
//     All(Vec<(Constraint, Conditional, ConditionalValue)>),
// }
// pub enum Conditional {
//     GreaterThan,
//     LessThan,
//     Equal,
//     NotEqual,
//     Present,
//     Absent,
// }
//
// pub enum ConditionalValue {
//     String(String),
//     Number(i64),
//     Boolean(bool),
// }

#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    #[allow(dead_code)]
    #[serde(skip_serializing, skip_deserializing)]
    etcd_client: Option<Client>,
    #[allow(dead_code)]
    #[serde(skip_serializing, skip_deserializing)]
    etcd_path: Option<String>,

    id: String,

    state: MessageState,
    /// Timeline of when each state change occurred.
    timeline: Vec<(MessageState, chrono::DateTime<Utc>)>,

    /// Body object
    body: Vec<u8>,
}
impl Message {
    #[instrument(skip(etcd_client))]
    pub(crate) async fn read(mut etcd_client: Client, etcd_path: String) -> anyhow::Result<Self> {
        debug!("loading message object");
        let query = etcd_client.get(etcd_path.clone(), None).await?;
        match query.kvs().first() {
            None => bail!("key not found"),
            Some(key) => {
                let mut message: Message = serde_json::from_slice(key.value())?;
                message.etcd_path = Some(etcd_path);
                message.etcd_client = Some(etcd_client);

                Ok(message)
            }
        }
    }

    /// TODO Add message ID to the span info
    #[instrument(skip(self))]
    pub(crate) async fn write(&mut self) -> anyhow::Result<()> {
        debug!(message = self.id.as_str(), "writing message object");
        let new_self = self.clone();
        let client: &mut Client = self.etcd_client.as_mut().unwrap();
        client
            .put(
                self.etcd_path.as_ref().unwrap().clone(),
                serde_json::to_vec(&new_self)?,
                None,
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub(crate) fn set_state(&mut self, state: MessageState) {
        debug!(message = self.id.as_str(), "changing state to {:?}", &state);
        self.state = state.clone();
        self.timeline.push((state, Utc::now()));
    }

    pub fn body(&self) -> &Vec<u8> {
        &self.body
    }

    pub fn consume(self) -> Vec<u8> {
        self.body
    }

    pub async fn finished(&mut self, body: Option<Result<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()> {
        self.set_state(MessageState::FINISHED(body));
        self.write().await
    }

    pub async fn lost(&mut self) -> anyhow::Result<()> {
        self.set_state(MessageState::LOST);
        self.write().await
    }
}
// impl Drop for Message {
//     fn drop(&mut self) {
//         todo!()
//     }
// }
impl Debug for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("timeline", &self.timeline)
            .field("body", &self.body)
            .finish()
    }
}

/// Returns the keys in the keyspace, ordered by the ascending creation time
#[instrument(skip(client, path))]
async fn get_keys(mut client: Client, path: Vec<u8>) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    trace!("enter get_keys");
    Ok(client
        .get(
            path.clone(),
            Some(
                GetOptions::new()
                    .with_prefix()
                    .with_sort(SortTarget::Create, SortOrder::Ascend),
            ),
        )
        .instrument(tracing::trace_span!("read_keys"))
        .await?
        .kvs()
        .iter()
        .map(|kv| (kv.key().to_vec(), kv.value().to_vec()))
        .collect())
}

type PdnMap = Arc<Mutex<Vec<(Vec<u8>, Vec<u8>)>>>;

#[derive(Debug, Clone)]
pub struct QueueOptions {}

#[derive(Clone)]
pub struct Queue {
    etcd_client: Client,
    namespace: String,

    /// Key -> Key ID
    pdn_map: PdnMap,
}
impl Queue {
    #[instrument(skip(etcd_client))]
    pub async fn new(
        etcd_client: Client,
        namespace: String,
        options: Option<QueueOptions>,
    ) -> anyhow::Result<Self> {
        // check if there is already an existing tree
        let pdn_key = format!("/{}/queue/pdn", &namespace);

        // list all keys and store them in `task_map`
        let keys = get_keys(etcd_client.clone(), pdn_key.as_bytes().to_vec()).await?;
        trace!(
            "found {} existing pending message(s) to be consumed",
            keys.len()
        );

        let task_map = Arc::new(Mutex::new(keys));

        tokio::task::spawn(task_watcher(etcd_client.clone(), pdn_key, task_map.clone()));

        Ok(Self {
            etcd_client,
            namespace,
            pdn_map: task_map,
        })
    }

    #[instrument(skip(self, body))]
    pub async fn send(&mut self, body: impl Into<Vec<u8>>) -> anyhow::Result<()> {
        let id = Uuid::new_v4().to_string();
        let pdn_key = format!("/{}/queue/pdn/{}", self.namespace, &id);
        let msg_key = format!("/{}/queue/{}", self.namespace, &id);

        let message = Message {
            etcd_client: None,
            etcd_path: None,
            id: id.clone(),
            state: MessageState::ENQUEUED,
            timeline: vec![(MessageState::ENQUEUED, Utc::now())],
            body: body.into(),
        };

        debug!("resulting message object: {:?}", &message);

        // write the message body
        self.etcd_client
            .put(
                msg_key,
                tracing::trace_span!("to_json").in_scope(|| serde_json::to_vec(&message))?,
                None,
            )
            .instrument(tracing::trace_span!("write_message"))
            .await?;

        // write the pdn
        self.etcd_client
            // the pdn key has no value. it's just a place holder
            .put(pdn_key, id.as_bytes(), None)
            .instrument(tracing::trace_span!("write_message"))
            .await?;

        Ok(())
    }

    /// Try and consume a message
    #[instrument(skip(self))]
    pub async fn try_recv(&mut self) -> anyhow::Result<Option<Message>> {
        loop {
            trace!("trying to consume a message");
            // pull a record off the pdn map
            let mut handle = self.pdn_map.lock().await;
            if handle.len() == 0 {
                // if there is no IDs in the map, then just return None
                debug!("there are no records in the local pdn task map");
                return Ok(None);
            }
            // remove the 0th element, returning the key. this will be the message we try and consume
            let key = handle.remove(0);
            drop(handle);

            trace!("trying to consume message with key = {:?}", &key.1);

            // delete the key
            let op = self.etcd_client.delete(key.0, None).await?;
            if op.deleted() == 0 {
                warn!("unable to delete pdn key. {:?}", op);
                continue;
            }

            // load the message object
            let msg_key = format!("/{}/queue/{}", self.namespace, String::from_utf8(key.1)?);
            let mut message = Message::read(self.etcd_client.clone(), msg_key).await?;

            // set the message state
            message.set_state(MessageState::CONSUMED);
            message.write().await?;

            // return struct containing result + state tracking
            return Ok(Some(message));
        }
    }
}
impl Debug for Queue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue").finish()
    }
}

#[instrument(skip(client, pdn_map))]
async fn task_watcher(
    mut client: Client,
    key: String,
    pdn_map: PdnMap,
) {
    // let _span_ = span!(Level::TRACE, "task_watcher", namespace = namespace.as_str()).entered();

    trace!("starting watcher");
    let (_watcher, mut stream) = client
        .watch(key, Some(WatchOptions::new().with_prefix().with_all_keys()))
        .await
        .unwrap();

    while let Some(resp) = stream.message().await.unwrap() {
        if resp.canceled() {
            // TODO under what case can this be canceled?
            warn!("watcher canceled");
            break;
        }

        trace!("received watch event. {:?}", &resp);

        for event in resp.events() {
            if event.kv().is_none() {
                debug!("event does not contain key/value information. ignoring");
                continue;
            }

            // let event_processing_span = span!(Level::TRACE, "event_processing").entered();

            let mut handle = pdn_map.lock().await;
            let kv = event.kv().unwrap();
            let key = kv.key().to_vec();
            let value = kv.value().to_vec();

            match event.event_type() {
                EventType::Delete => match handle.iter().position(|v| v.1 == key) {
                    None => debug!("got delete event on an unknown message id. message might have been produced locally"),
                    Some(index) => {
                        trace!(
                            i = &index,
                            "got delete event, dropping message from internal index"
                        );
                        let _ = handle.remove(index);
                    }
                },
                EventType::Put => {
                    trace!("got put event, adding key to internal index");
                    handle.push((key, value))
                }
            }

            drop(handle);
            // event_processing_span.exit();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn publish_consume() {

        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
