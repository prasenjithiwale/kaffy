//! Committed offsets of consumer groups through the Admin API's
//! ListConsumerGroupOffsets, which rust-rdkafka does not wrap yet.
//!
//! One admin request per group, all in flight together on a client kaffy
//! already has. The alternative, a consumer client per group asking about
//! every partition in the cluster, costs threads and sockets per group and
//! takes tens of seconds on clusters with a few hundred groups.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CStr, CString, c_int, c_void};
use std::ptr;
use std::time::{Duration, Instant};

use rdkafka::bindings as rd;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::types::RDKafkaErrorCode;

/// Committed offset per (topic, partition).
pub(crate) type Offsets = BTreeMap<(String, i32), i64>;

/// Requests kept in flight at once, so a cluster with thousands of groups
/// doesn't flood the coordinators.
const IN_FLIGHT: usize = 256;

/// The committed offsets of each group in `groups`, for every partition the
/// group has committed. A group that failed on its own maps to its error; the
/// whole call fails only when the requests can't be made or time out.
pub(crate) fn committed_offsets(
    client: &BaseConsumer,
    groups: &[String],
    timeout: Duration,
) -> KafkaResult<HashMap<String, KafkaResult<Offsets>>> {
    let rk = client.client().native_ptr();
    let mut results = HashMap::with_capacity(groups.len());
    for chunk in groups.chunks(IN_FLIGHT) {
        // SAFETY: `rk` stays valid while `client` is borrowed, and everything
        // `request` creates is destroyed before it returns.
        unsafe { request(rk, chunk, timeout, &mut results)? };
    }
    Ok(results)
}

/// The committed offsets of one group.
pub(crate) fn group_committed_offsets(
    client: &BaseConsumer,
    group: &str,
    timeout: Duration,
) -> KafkaResult<Offsets> {
    committed_offsets(client, &[group.to_owned()], timeout)?
        .remove(group)
        .unwrap_or_else(|| Ok(Offsets::new()))
}

unsafe fn request(
    rk: *mut rd::rd_kafka_t,
    groups: &[String],
    timeout: Duration,
    results: &mut HashMap<String, KafkaResult<Offsets>>,
) -> KafkaResult<()> {
    let queue = Queue(unsafe { rd::rd_kafka_queue_new(rk) });
    let timeout_ms = c_int::try_from(timeout.as_millis()).unwrap_or(c_int::MAX);
    for (i, group) in groups.iter().enumerate() {
        let name = CString::new(group.as_str()).map_err(|_| {
            KafkaError::AdminOpCreation(format!("group name {group:?} has a NUL byte"))
        })?;
        // librdkafka copies the request and the options, so both are freed here.
        let options = unsafe { Options::new(rk, timeout_ms)? };
        // The opaque tells which group a result belongs to, even when the
        // request failed as a whole.
        unsafe { rd::rd_kafka_AdminOptions_set_opaque(options.0, (i + 1) as *mut c_void) };
        let mut list =
            unsafe { rd::rd_kafka_ListConsumerGroupOffsets_new(name.as_ptr(), ptr::null()) };
        unsafe {
            rd::rd_kafka_ListConsumerGroupOffsets(rk, &mut list, 1, options.0, queue.0);
            rd::rd_kafka_ListConsumerGroupOffsets_destroy(list);
        }
    }

    // librdkafka answers every request, with a timeout error at worst; the
    // deadline only guards against waiting forever.
    let deadline = Instant::now() + timeout + Duration::from_secs(5);
    let mut pending = groups.len();
    while pending > 0 {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut));
        }
        let wait = c_int::try_from(left.as_millis()).unwrap_or(c_int::MAX);
        let event = Event(unsafe { rd::rd_kafka_queue_poll(queue.0, wait) });
        if event.0.is_null()
            || unsafe { rd::rd_kafka_event_type(event.0) }
                != rd::RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT
        {
            continue;
        }
        let index = unsafe { rd::rd_kafka_event_opaque(event.0) } as usize;
        let Some(group) = index.checked_sub(1).and_then(|i| groups.get(i)) else {
            continue;
        };
        pending -= 1;
        results.insert(group.clone(), unsafe { group_result(event.0) });
    }
    Ok(())
}

/// The offsets in a ListConsumerGroupOffsets result event, or its error.
unsafe fn group_result(event: *mut rd::rd_kafka_event_t) -> KafkaResult<Offsets> {
    let err = unsafe { rd::rd_kafka_event_error(event) };
    if err != rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(KafkaError::AdminOp(err.into()));
    }
    let result = unsafe { rd::rd_kafka_event_ListConsumerGroupOffsets_result(event) };
    if result.is_null() {
        return Err(KafkaError::AdminOpCreation(
            "empty ListConsumerGroupOffsets result".to_owned(),
        ));
    }
    let mut count = 0;
    let groups = unsafe { rd::rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut count) };
    if groups.is_null() || count == 0 {
        return Ok(Offsets::new());
    }
    // One request names one group, so there is one group result.
    let group = unsafe { *groups };
    let error = unsafe { rd::rd_kafka_group_result_error(group) };
    if !error.is_null() {
        return Err(KafkaError::AdminOp(
            unsafe { rd::rd_kafka_error_code(error) }.into(),
        ));
    }
    let list = unsafe { rd::rd_kafka_group_result_partitions(group) };
    if list.is_null() {
        return Ok(Offsets::new());
    }
    let (elems, cnt) = unsafe { ((*list).elems, (*list).cnt) };
    if elems.is_null() || cnt <= 0 {
        return Ok(Offsets::new());
    }
    let elems = unsafe { std::slice::from_raw_parts(elems, cnt as usize) };
    Ok(elems
        .iter()
        // Negative offsets mean "nothing committed"; per-partition errors
        // leave that partition out, like the consumer's committed() did.
        .filter(|e| e.offset >= 0 && e.err == rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR)
        .filter(|e| !e.topic.is_null())
        .map(|e| {
            let topic = unsafe { CStr::from_ptr(e.topic) }
                .to_string_lossy()
                .into_owned();
            ((topic, e.partition), e.offset)
        })
        .collect())
}

struct Queue(*mut rd::rd_kafka_queue_t);

impl Drop for Queue {
    fn drop(&mut self) {
        // SAFETY: created by rd_kafka_queue_new and destroyed only here.
        unsafe { rd::rd_kafka_queue_destroy(self.0) }
    }
}

struct Options(*mut rd::rd_kafka_AdminOptions_t);

impl Options {
    unsafe fn new(rk: *mut rd::rd_kafka_t, timeout_ms: c_int) -> KafkaResult<Self> {
        let options = Self(unsafe {
            rd::rd_kafka_AdminOptions_new(
                rk,
                rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
            )
        });
        let mut errstr = [0 as std::ffi::c_char; 256];
        let err = unsafe {
            rd::rd_kafka_AdminOptions_set_request_timeout(
                options.0,
                timeout_ms,
                errstr.as_mut_ptr(),
                errstr.len(),
            )
        };
        if err != rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let message = unsafe { CStr::from_ptr(errstr.as_ptr()) }
                .to_string_lossy()
                .into_owned();
            return Err(KafkaError::AdminOpCreation(message));
        }
        Ok(options)
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        // SAFETY: created by rd_kafka_AdminOptions_new and destroyed only here.
        unsafe { rd::rd_kafka_AdminOptions_destroy(self.0) }
    }
}

struct Event(*mut rd::rd_kafka_event_t);

impl Drop for Event {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: returned by rd_kafka_queue_poll and destroyed only here.
            unsafe { rd::rd_kafka_event_destroy(self.0) }
        }
    }
}
