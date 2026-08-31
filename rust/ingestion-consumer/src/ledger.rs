use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use common_kafka_consumer::{Charge, Offset, OffsetLedger};
use metrics::{counter, gauge};
use tracing::warn;

use crate::config::LedgerMode;
use crate::order_sentinel::OffsetSpan;

pub(crate) type TopicPartition = (String, i32);

/// One batch's ledger offsets for one partition, stamped with the assignment
/// epoch that was current when they were buffered.
#[derive(Debug)]
pub(crate) struct EpochOffsets {
    pub(crate) epoch: u64,
    pub(crate) offsets: Vec<Offset>,
}

/// A mismatch between the current commit calculation and the ledger frontier.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LedgerMismatch {
    pub(crate) topic_partition: TopicPartition,
    pub(crate) committed: i64,
    pub(crate) frontier: Option<Offset>,
}

/// One assignment's ledger. Work stamped with an earlier epoch belongs to a
/// previous assignment of the partition, and its offsets replay under this
/// one.
struct EpochLedger {
    epoch: u64,
    ledger: OffsetLedger,
}

/// Maintains the offset ledgers and, in commit mode, supplies the frontier
/// the consumer commits.
pub(crate) struct LedgerObserver {
    mode: LedgerMode,
    /// Shared with the rebalance callback, which bumps it on every assign.
    assignment_epoch: Arc<AtomicU64>,
    partitions: Mutex<HashMap<TopicPartition, EpochLedger>>,
}

impl LedgerObserver {
    pub(crate) fn new(mode: LedgerMode, assignment_epoch: Arc<AtomicU64>) -> Self {
        Self {
            mode,
            assignment_epoch,
            partitions: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn owns_commits(&self) -> bool {
        self.mode == LedgerMode::Commit
    }

    /// The current assignment epoch, for stamping work as it is buffered.
    pub(crate) fn epoch(&self) -> u64 {
        self.assignment_epoch.load(Ordering::Relaxed)
    }

    pub(crate) fn charge(
        &self,
        topic: &str,
        partition: i32,
        epoch: u64,
        offset_charges: impl IntoIterator<Item = (Offset, Charge)>,
    ) {
        let mut partitions = self.partitions.lock().unwrap();
        let entry = match partitions.entry((topic.to_string(), partition)) {
            Entry::Occupied(occupied) => {
                let entry = occupied.into_mut();
                // The ledger belongs to a newer assignment than the slice:
                // the slice's offsets replay under that assignment. A slice
                // that merely spans an unrelated epoch bump has
                // epoch >= entry.epoch and charges normally.
                if epoch < entry.epoch {
                    counter!(
                        "ingestion_consumer_ledger_stale_slices_total",
                        "stage" => "charge",
                        "reason" => "stale_epoch"
                    )
                    .increment(1);
                    return;
                }
                entry
            }
            Entry::Vacant(vacant) => {
                // No ledger means the partition was revoked after the slice
                // was buffered; only a slice from the current assignment may
                // found the new ledger.
                if epoch != self.epoch() {
                    counter!(
                        "ingestion_consumer_ledger_stale_slices_total",
                        "stage" => "charge",
                        "reason" => "no_ledger"
                    )
                    .increment(1);
                    return;
                }
                vacant.insert(EpochLedger {
                    epoch,
                    ledger: OffsetLedger::new(),
                })
            }
        };
        entry.ledger.charge(offset_charges);
        gauge!(
            "ingestion_consumer_ledger_uncommitted_offsets",
            "topic" => topic.to_string(),
            "partition" => partition.to_string()
        )
        .set(entry.ledger.len() as f64);
    }

    /// Mark one completed batch and compare the resulting frontiers with the
    /// current commit spans. The caller chooses whether `offset_spans` or the
    /// frontiers own the Kafka commit. Completions stamped before a
    /// partition's current assignment are stragglers and drop.
    pub(crate) fn complete_and_compare_frontiers(
        &self,
        completed: &HashMap<TopicPartition, EpochOffsets>,
        offset_spans: &HashMap<TopicPartition, OffsetSpan>,
    ) -> (HashMap<TopicPartition, Offset>, Vec<LedgerMismatch>) {
        let mut partitions = self.partitions.lock().unwrap();
        let mut frontiers = HashMap::new();
        let mut mismatches = Vec::new();

        for (topic_partition, batch) in completed {
            // A rebalance dropped this partition's ledger between delivery
            // and completion: skip the completion, its offsets replay from
            // the last commit.
            let Some(entry) = partitions.get_mut(topic_partition) else {
                counter!(
                    "ingestion_consumer_ledger_stale_slices_total",
                    "stage" => "complete",
                    "reason" => "no_ledger"
                )
                .increment(1);
                continue;
            };
            // The ledger belongs to a newer assignment than the batch: the
            // batch's offsets were already dropped with the old ledger and
            // replay under the new one.
            if batch.epoch < entry.epoch {
                counter!(
                    "ingestion_consumer_ledger_stale_slices_total",
                    "stage" => "complete",
                    "reason" => "stale_epoch"
                )
                .increment(1);
                continue;
            }
            entry.ledger.complete(&batch.offsets);

            // The frontier is next-to-read; the commit path submits
            // span.last + 1, so compare against that same value.
            let committed = offset_spans
                .get(topic_partition)
                .expect("completed offsets must have a commit span")
                .last
                + 1;
            let frontier = entry.ledger.frontier();
            if let Some(frontier) = frontier {
                frontiers.insert(topic_partition.clone(), frontier);
            }
            if frontier != Some(Offset(committed)) {
                let direction = match frontier {
                    Some(frontier) if frontier.0 > committed => "ahead",
                    _ => "behind",
                };
                counter!(
                    "ingestion_consumer_ledger_mismatch_total",
                    "topic" => topic_partition.0.clone(),
                    "partition" => topic_partition.1.to_string(),
                    "direction" => direction
                )
                .increment(1);
                warn!(
                    topic = %topic_partition.0,
                    partition = topic_partition.1,
                    committed,
                    frontier = ?frontier,
                    direction,
                    depth = entry.ledger.len(),
                    batch_epoch = batch.epoch,
                    ledger_epoch = entry.epoch,
                    "Offset ledger frontier differs from current commit"
                );
                mismatches.push(LedgerMismatch {
                    topic_partition: topic_partition.clone(),
                    committed,
                    frontier,
                });
            }
        }

        (frontiers, mismatches)
    }

    /// Consume the completed prefix after a successful commit request, with
    /// the same straggler checks as the comparison.
    pub(crate) fn take_frontiers(&self, completed: &HashMap<TopicPartition, EpochOffsets>) {
        let mut partitions = self.partitions.lock().unwrap();
        for (topic_partition, batch) in completed {
            let Some(entry) = partitions.get_mut(topic_partition) else {
                continue;
            };
            if batch.epoch < entry.epoch {
                continue;
            }
            entry.ledger.take_frontier();
            gauge!(
                "ingestion_consumer_ledger_uncommitted_offsets",
                "topic" => topic_partition.0.clone(),
                "partition" => topic_partition.1.to_string()
            )
            .set(entry.ledger.len() as f64);
        }
    }

    /// Shadow-mode completion: compare the current commit source, then drain
    /// at the same point as the actual commit request.
    pub(crate) fn complete_and_compare(
        &self,
        completed: &HashMap<TopicPartition, EpochOffsets>,
        offset_spans: &HashMap<TopicPartition, OffsetSpan>,
    ) -> Vec<LedgerMismatch> {
        let (_, mismatches) = self.complete_and_compare_frontiers(completed, offset_spans);
        self.take_frontiers(completed);
        mismatches
    }

    /// Drop the revoked partitions' ledgers before their replay is charged:
    /// a kept ledger sees replayed offsets as duplicate delivery and panics.
    pub(crate) fn forget_partitions<'a>(&self, revoked: impl IntoIterator<Item = (&'a str, i32)>) {
        let mut partitions = self.partitions.lock().unwrap();
        for (topic, partition) in revoked {
            partitions.remove(&(topic.to_string(), partition));
            gauge!(
                "ingestion_consumer_ledger_uncommitted_offsets",
                "topic" => topic.to_string(),
                "partition" => partition.to_string()
            )
            .set(0.0);
        }
    }

    #[cfg(test)]
    fn depth(&self, topic: &str, partition: i32) -> usize {
        self.partitions
            .lock()
            .unwrap()
            .get(&(topic.to_string(), partition))
            .map(|entry| entry.ledger.len())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(last: i64) -> OffsetSpan {
        OffsetSpan { first: last, last }
    }

    fn new_observer() -> (Arc<AtomicU64>, LedgerObserver) {
        let epoch = Arc::new(AtomicU64::new(1));
        let observer = LedgerObserver::new(LedgerMode::Shadow, Arc::clone(&epoch));
        (epoch, observer)
    }

    fn batch(epoch: u64, offsets: Vec<Offset>) -> EpochOffsets {
        EpochOffsets { epoch, offsets }
    }

    #[test]
    fn matching_frontier_is_drained_after_comparison() {
        let (_, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.charge("events", 0, 1, [(Offset(11), Charge::ZERO)]);

        let topic_partition = ("events".to_string(), 0);
        let completed = HashMap::from([(
            topic_partition.clone(),
            batch(1, vec![Offset(10), Offset(11)]),
        )]);
        let offset_spans = HashMap::from([(topic_partition, span(11))]);

        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(observer.depth("events", 0), 0);
    }

    #[test]
    fn revoked_partitions_drop_their_ledger() {
        let (_, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.forget_partitions([("events", 0)]);

        assert_eq!(observer.depth("events", 0), 0);
    }

    #[test]
    fn mismatch_does_not_drain_offsets_above_an_incomplete_prefix() {
        let (_, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.charge("events", 0, 1, [(Offset(11), Charge::ZERO)]);

        let topic_partition = ("events".to_string(), 0);
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(11)]))]);
        let offset_spans = HashMap::from([(topic_partition.clone(), span(11))]);

        assert_eq!(
            observer.complete_and_compare(&completed, &offset_spans),
            vec![LedgerMismatch {
                topic_partition: topic_partition.clone(),
                committed: 12,
                frontier: None,
            }]
        );
        assert_eq!(observer.depth("events", 0), 2);

        // The late completion arrives with the next batch: clean compare,
        // and the held offsets drain.
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(10)]))]);
        let offset_spans = HashMap::from([(topic_partition, span(11))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(observer.depth("events", 0), 0);
    }

    #[test]
    fn a_batch_spanning_partitions_settles_each_independently() {
        let (_, observer) = new_observer();
        observer.charge(
            "events",
            0,
            1,
            [(Offset(10), Charge::ZERO), (Offset(11), Charge::ZERO)],
        );
        observer.charge(
            "events",
            1,
            1,
            [(Offset(20), Charge::ZERO), (Offset(21), Charge::ZERO)],
        );

        let settled = ("events".to_string(), 0);
        let held = ("events".to_string(), 1);
        let completed = HashMap::from([
            (settled.clone(), batch(1, vec![Offset(10), Offset(11)])),
            (held.clone(), batch(1, vec![Offset(21)])),
        ]);
        let offset_spans = HashMap::from([(settled.clone(), span(11)), (held.clone(), span(21))]);

        let mismatches = observer.complete_and_compare(&completed, &offset_spans);
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].topic_partition, held);
        assert_eq!(
            observer.depth("events", 0),
            0,
            "the settled partition drains"
        );
        assert_eq!(
            observer.depth("events", 1),
            2,
            "the held partition keeps its offsets"
        );
    }

    #[test]
    fn frontiers_map_carries_only_partitions_with_a_frontier() {
        let observer = LedgerObserver::new(LedgerMode::Commit, Arc::new(AtomicU64::new(1)));
        assert!(observer.owns_commits());
        assert!(
            !LedgerObserver::new(LedgerMode::Shadow, Arc::new(AtomicU64::new(1))).owns_commits()
        );

        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.charge("events", 1, 1, [(Offset(10), Charge::ZERO)]);
        observer.charge("events", 1, 1, [(Offset(11), Charge::ZERO)]);

        let done = ("events".to_string(), 0);
        let held = ("events".to_string(), 1);
        let completed = HashMap::from([
            (done.clone(), batch(1, vec![Offset(10)])),
            (held.clone(), batch(1, vec![Offset(11)])),
        ]);
        let offset_spans = HashMap::from([(done.clone(), span(10)), (held.clone(), span(11))]);

        let (frontiers, mismatches) =
            observer.complete_and_compare_frontiers(&completed, &offset_spans);
        assert_eq!(frontiers, HashMap::from([(done, Offset(11))]));
        assert_eq!(
            mismatches.len(),
            1,
            "the partition with no frontier compares as a mismatch"
        );
    }

    #[test]
    fn only_take_frontiers_drains_the_ledgers() {
        let observer = LedgerObserver::new(LedgerMode::Commit, Arc::new(AtomicU64::new(1)));
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);

        let topic_partition = ("events".to_string(), 0);
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(10)]))]);
        let offset_spans = HashMap::from([(topic_partition, span(10))]);
        observer.complete_and_compare_frontiers(&completed, &offset_spans);
        assert_eq!(observer.depth("events", 0), 1, "observing does not consume");

        observer.take_frontiers(&completed);
        assert_eq!(observer.depth("events", 0), 0);
    }

    #[test]
    fn take_frontiers_skips_a_forgotten_partition() {
        let observer = LedgerObserver::new(LedgerMode::Commit, Arc::new(AtomicU64::new(1)));
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.forget_partitions([("events", 0)]);

        let topic_partition = ("events".to_string(), 0);
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(10)]))]);
        observer.take_frontiers(&completed);
    }

    #[test]
    fn completions_for_a_forgotten_partition_are_skipped() {
        let (_, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.forget_partitions([("events", 0)]);

        let topic_partition = ("events".to_string(), 0);
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(10)]))]);
        let offset_spans = HashMap::from([(topic_partition, span(10))]);

        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
    }

    #[test]
    fn partitions_are_keyed_by_topic_and_partition() {
        let (_, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.charge("overflow", 0, 1, [(Offset(10), Charge::ZERO)]);

        let topic_partition = ("events".to_string(), 0);
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(10)]))]);
        let offset_spans = HashMap::from([(topic_partition, span(10))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());

        assert_eq!(observer.depth("events", 0), 0);
        assert_eq!(observer.depth("overflow", 0), 1);
    }

    #[test]
    fn stale_completions_from_a_previous_assignment_are_dropped() {
        let (epoch, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);

        // The partition is revoked and reassigned to this consumer while the
        // batch is still in flight; the replay recharges the same offset.
        observer.forget_partitions([("events", 0)]);
        epoch.store(2, Ordering::Relaxed);
        observer.charge("events", 0, 2, [(Offset(10), Charge::ZERO)]);

        let topic_partition = ("events".to_string(), 0);
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(10)]))]);
        let offset_spans = HashMap::from([(topic_partition.clone(), span(10))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(
            observer.depth("events", 0),
            1,
            "the replayed offset stays uncompleted"
        );

        // The replay's own completion settles the new assignment's ledger.
        let completed = HashMap::from([(topic_partition.clone(), batch(2, vec![Offset(10)]))]);
        let offset_spans = HashMap::from([(topic_partition, span(10))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(observer.depth("events", 0), 0);
    }

    #[test]
    fn an_epoch_spanning_batch_drops_only_the_reassigned_partition() {
        let (epoch, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.charge("events", 1, 1, [(Offset(20), Charge::ZERO)]);

        // Only partition 0 is revoked and reassigned; the epoch bump is
        // global.
        observer.forget_partitions([("events", 0)]);
        epoch.store(2, Ordering::Relaxed);
        observer.charge("events", 0, 2, [(Offset(10), Charge::ZERO)]);

        let reassigned = ("events".to_string(), 0);
        let untouched = ("events".to_string(), 1);
        let completed = HashMap::from([
            (reassigned.clone(), batch(1, vec![Offset(10)])),
            (untouched.clone(), batch(1, vec![Offset(20)])),
        ]);
        let offset_spans = HashMap::from([
            (reassigned.clone(), span(10)),
            (untouched.clone(), span(20)),
        ]);

        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(
            observer.depth("events", 0),
            1,
            "the reassigned partition ignores the stale completion"
        );
        assert_eq!(
            observer.depth("events", 1),
            0,
            "the untouched partition completes and drains"
        );
    }

    #[test]
    fn slices_spanning_an_unrelated_epoch_bump_still_charge() {
        let (epoch, observer) = new_observer();
        observer.charge("events", 1, 1, [(Offset(20), Charge::ZERO)]);

        // Another partition's reassignment bumps the epoch; this partition's
        // ledger survives and its old-stamped slice must not be lost.
        epoch.store(2, Ordering::Relaxed);
        observer.charge("events", 1, 1, [(Offset(21), Charge::ZERO)]);
        assert_eq!(observer.depth("events", 1), 2);

        let topic_partition = ("events".to_string(), 1);
        let completed = HashMap::from([(
            topic_partition.clone(),
            batch(1, vec![Offset(20), Offset(21)]),
        )]);
        let offset_spans = HashMap::from([(topic_partition, span(21))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(observer.depth("events", 1), 0, "no message is lost");
    }

    #[test]
    fn a_partition_lost_for_an_epoch_returns_under_a_later_epoch() {
        let (epoch, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);

        // The partition leaves for another consumer, then returns two
        // assignments later; the in-flight batch settles in between.
        observer.forget_partitions([("events", 0)]);
        epoch.store(3, Ordering::Relaxed);

        let topic_partition = ("events".to_string(), 0);
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(10)]))]);
        let offset_spans = HashMap::from([(topic_partition.clone(), span(10))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(observer.depth("events", 0), 0, "no ledger, nothing lands");

        observer.charge("events", 0, 3, [(Offset(10), Charge::ZERO)]);
        let completed = HashMap::from([(topic_partition.clone(), batch(3, vec![Offset(10)]))]);
        let offset_spans = HashMap::from([(topic_partition, span(10))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(observer.depth("events", 0), 0);
    }

    #[test]
    fn batches_from_older_epochs_settle_against_a_surviving_ledger() {
        let (epoch, observer) = new_observer();
        observer.charge("events", 1, 1, [(Offset(20), Charge::ZERO)]);
        epoch.store(2, Ordering::Relaxed);
        observer.charge("events", 1, 2, [(Offset(21), Charge::ZERO)]);
        epoch.store(4, Ordering::Relaxed);

        // Both in-flight batches predate the current epoch; the partition was
        // never revoked, so both must land.
        let topic_partition = ("events".to_string(), 1);
        let completed = HashMap::from([(topic_partition.clone(), batch(1, vec![Offset(20)]))]);
        let offset_spans = HashMap::from([(topic_partition.clone(), span(20))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());

        let completed = HashMap::from([(topic_partition.clone(), batch(2, vec![Offset(21)]))]);
        let offset_spans = HashMap::from([(topic_partition, span(21))]);
        assert!(observer
            .complete_and_compare(&completed, &offset_spans)
            .is_empty());
        assert_eq!(observer.depth("events", 1), 0, "no message is lost");
    }

    #[test]
    fn slices_from_every_older_epoch_cannot_refound_a_dropped_ledger() {
        let (epoch, observer) = new_observer();
        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.forget_partitions([("events", 0)]);
        epoch.store(3, Ordering::Relaxed);

        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);
        observer.charge("events", 0, 2, [(Offset(10), Charge::ZERO)]);
        assert_eq!(observer.depth("events", 0), 0);

        observer.charge("events", 0, 3, [(Offset(10), Charge::ZERO)]);
        assert_eq!(observer.depth("events", 0), 1);
    }

    #[test]
    fn charges_buffered_before_a_rebalance_are_dropped() {
        let (epoch, observer) = new_observer();
        epoch.store(2, Ordering::Relaxed);

        observer.charge("events", 0, 1, [(Offset(10), Charge::ZERO)]);

        assert_eq!(observer.depth("events", 0), 0);
    }
}
