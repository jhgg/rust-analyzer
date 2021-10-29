//! rust-analyzer is lazy and doesn't compute anything unless asked. This
//! sometimes is counter productive when, for example, the first goto definition
//! request takes longer to compute. This modules implemented prepopulation of
//! various caches, it's not really advanced at the moment.

use std::{collections::VecDeque, hash::Hash};

use crossbeam_channel::Sender;
use hir::db::DefDatabase;
use ide_db::base_db::{
    salsa::ParallelDatabase, Cancelled, CrateGraph, CrateId, SourceDatabase, SourceDatabaseExt,
};
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::info;

use crate::RootDatabase;

/// We started indexing a crate.
#[derive(Debug)]
pub struct PrimeCachesProgress {
    pub on_crate: String,
    pub n_done: usize,
    pub n_total: usize,
}

pub enum ParallelPrimeCachesProgress {
    BeginPriming { n_total: usize },
    BeginCrate { crate_name: String, crate_id: CrateId },
    EndCrate { crate_name: String, crate_id: CrateId, cancelled: bool },
    End { cancelled: bool },
}

pub(crate) fn prime_caches(db: &RootDatabase, cb: &(dyn Fn(PrimeCachesProgress) + Sync)) {
    let _p = profile::span("prime_caches");
    let graph = db.crate_graph();
    // We're only interested in the transitive dependencies of all workspace crates.
    let to_prime = non_library_crate_ids(db);

    let topo = toposort(&graph, &to_prime);

    // FIXME: This would be easy to parallelize, since it's in the ideal ordering for that.
    // Unfortunately rayon prevents panics from propagation out of a `scope`, which breaks
    // cancellation, so we cannot use rayon.
    for (i, &crate_id) in topo.iter().enumerate() {
        let crate_name = graph[crate_id].display_name.as_deref().unwrap_or_default().to_string();

        cb(PrimeCachesProgress { on_crate: crate_name, n_done: i, n_total: topo.len() });
        db.crate_def_map(crate_id);
        db.import_map(crate_id);
    }
}

fn non_library_crate_ids(db: &RootDatabase) -> FxHashSet<CrateId> {
    let graph = db.crate_graph();
    graph
        .iter()
        .filter(|&id| {
            let file_id = graph[id].root_file_id;
            let root_id = db.file_source_root(file_id);
            !db.source_root(root_id).is_library
        })
        .flat_map(|id| graph.transitive_deps(id))
        .collect()
}

fn toposort(graph: &CrateGraph, crates: &FxHashSet<CrateId>) -> Vec<CrateId> {
    // Just subset the full topologically sorted set for simplicity.

    let all = graph.crates_in_topological_order();
    let mut result = Vec::with_capacity(crates.len());
    for krate in all {
        if crates.contains(&krate) {
            result.push(krate);
        }
    }
    result
}

pub(crate) fn parallel_prime_caches(
    db: &RootDatabase,
    cb: &(dyn Fn(ParallelPrimeCachesProgress) + Sync),
) {
    let _p = profile::span("parallel_prime_caches");
    let graph = db.crate_graph();
    let mut tg = Topograph::new();

    for crate_id in non_library_crate_ids(db) {
        let crate_data = &graph[crate_id];
        let dependencies = crate_data.dependencies.iter().map(|d| d.crate_id);
        tg.add(crate_id, dependencies);
    }

    let mut tg = tg.prepare();

    return crossbeam_utils::thread::scope(move |s| {
        let (work_sender, work_receiver) = crossbeam_channel::unbounded::<(CrateId, String)>();
        let (result_sender, result_receiver) = crossbeam_channel::unbounded();

        for _ in 0..16 {
            let work_receiver = work_receiver.clone();
            let result_sender = result_sender.clone();
            let db = db.snapshot();

            s.spawn(move |_| {
                while let Ok((crate_id, crate_name)) = work_receiver.recv() {
                    if result_sender
                        .send(ParallelPrimeCachesProgress::BeginCrate {
                            crate_name: crate_name.clone(),
                            crate_id,
                        })
                        .is_err()
                    {
                        return;
                    }

                    info!("got work {}", crate_name);
                    let cancelled = Cancelled::catch(|| {
                        db.crate_def_map(crate_id);
                        db.import_map(crate_id);
                    })
                    .is_err();

                    result_sender
                        .send(ParallelPrimeCachesProgress::EndCrate {
                            crate_name: crate_name.clone(),
                            crate_id,
                            cancelled,
                        })
                        .ok();

                    if cancelled {
                        break;
                    }
                    // ...
                }
            });
        }

        (cb)(ParallelPrimeCachesProgress::BeginPriming { n_total: tg.len() });

        send_ready(&graph, &work_sender, &mut tg);

        for result in result_receiver {
            let mut is_cancelled = false;
            if let ParallelPrimeCachesProgress::EndCrate { crate_id, cancelled, .. } = &result {
                tg.mark_done(*crate_id);
                is_cancelled = *cancelled;

                if !is_cancelled {
                    send_ready(&graph, &work_sender, &mut tg);
                }
            }

            (cb)(result);

            if is_cancelled || tg.len() == 0 {
                (cb)(ParallelPrimeCachesProgress::End { cancelled: is_cancelled });
                return;
            }
        }

        return (cb)(ParallelPrimeCachesProgress::End { cancelled: false });
    })
    .unwrap();

    fn send_ready(
        graph: &CrateGraph,
        sender: &Sender<(CrateId, String)>,
        tg: &mut PreparedTopograph<CrateId>,
    ) {
        while let Ok(crate_id) = tg.take_ready() {
            let crate_name =
                graph[crate_id].display_name.as_deref().unwrap_or_default().to_string();

            info!("sending work...! {}", crate_name);
            sender.send((crate_id, crate_name)).ok();
        }
    }
}

struct TopographEntry<T> {
    successors: Vec<T>,
    num_predecessors: usize,
}

impl<T> Default for TopographEntry<T> {
    fn default() -> Self {
        Self { successors: Default::default(), num_predecessors: 0 }
    }
}

struct Topograph<T> {
    nodes: FxHashMap<T, TopographEntry<T>>,
}

impl<T> Topograph<T>
where
    T: Copy + Eq + PartialEq + Hash,
{
    fn new() -> Self {
        Self { nodes: Default::default() }
    }

    fn add(&mut self, item: T, predecessors: impl IntoIterator<Item = T>) {
        let mut num_predecessors = 0;
        for predecessor in predecessors.into_iter() {
            self.get_entry(predecessor).successors.push(item);
            num_predecessors += 1;
        }

        let entry = self.get_entry(item);
        entry.num_predecessors += num_predecessors;
    }

    fn get_entry(&mut self, item: T) -> &mut TopographEntry<T> {
        self.nodes.entry(item).or_default()
    }

    fn prepare(self) -> PreparedTopograph<T> {
        let ready = self
            .nodes
            .iter()
            .filter_map(
                |(item, entry)| if entry.num_predecessors == 0 { Some(*item) } else { None },
            )
            .collect();

        PreparedTopograph { nodes: self.nodes, ready }
    }
}

struct PreparedTopograph<T> {
    ready: VecDeque<T>,
    nodes: FxHashMap<T, TopographEntry<T>>,
}

enum ReadyError {
    Empty,
    NoneReadyYet,
}

impl<T> PreparedTopograph<T>
where
    T: Copy + Eq + PartialEq + Hash,
{
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn take_ready(&mut self) -> Result<T, ReadyError> {
        if let Some(item) = self.ready.pop_front() {
            return Ok(item);
        }

        Err(if self.nodes.is_empty() { ReadyError::Empty } else { ReadyError::NoneReadyYet })
    }

    fn mark_done(&mut self, item: T) {
        let entry = self.nodes.remove(&item).expect("invariant: unknown item marked as done");

        for successor in entry.successors {
            let successor_entry =
                self.nodes.get_mut(&successor).expect("invariant: unknown successor");

            successor_entry.num_predecessors -= 1;
            if successor_entry.num_predecessors == 0 {
                self.ready.push_back(successor);
            }
        }
    }
}
