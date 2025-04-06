use super::buckets::{ABucket, CBucket, FBucket, TBucket};
use crate::{
  schemas::{
    DataEdge, DataVertex, EBase, LabelRef, PatternAttr, PatternEdge, PatternVertex, Vid, VidRef,
  },
  storage::{AdvancedStorageAdapter, StorageAdapter},
  utils::{
    dyn_graph::DynGraph,
    expand_graph::{ExpandGraph, union_then_intersect_on_connective_v},
    parallel,
  },
};
use hashbrown::{HashMap, HashSet};
use rayon::iter::{
  IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use std::sync::Arc;

async fn does_data_v_satisfy_pattern(
  dg_vid: VidRef<'_>,
  pat_vid: VidRef<'_>,
  pat_v_entities: &HashMap<Vid, PatternVertex>,
  storage_adapter: &impl StorageAdapter,
) -> bool {
  let pat_v = pat_v_entities.get(pat_vid).unwrap();
  let data_v = storage_adapter.get_v(dg_vid).await.unwrap();

  if pat_v.label != data_v.label {
    return false;
  }

  if pat_v.attr.is_none() {
    true
  } else if data_v.attrs.is_empty() {
    false
  } else {
    let pat_attr = pat_v.attr.as_ref().unwrap();
    if !data_v.attrs.contains_key(&pat_attr.key) {
      return false;
    }
    let data_value = data_v.attrs.get(&pat_attr.key).unwrap();
    pat_attr.op.operate_on(data_value, &pat_attr.value)
  }
}

impl FBucket {
  pub async fn from_c_bucket(c_bucket: CBucket) -> Self {
    let mut all_matched = vec![];
    let mut matched_with_pivots = HashMap::new();

    let all_expanded = c_bucket.all_expanded;
    let mut expanded_with_pivots = c_bucket.expanded_with_frontiers;

    for (idx, matched) in all_expanded.into_iter().enumerate() {
      all_matched.push(matched.into());
      matched_with_pivots
        .entry(idx)
        .or_insert_with(Vec::new)
        .extend(expanded_with_pivots.remove(&idx).unwrap_or_default());
    }

    Self {
      all_matched,
      matched_with_frontiers: matched_with_pivots,
    }
  }
}

impl ABucket {
  pub fn from_f_bucket(f_bucket: FBucket, curr_pat_vid: VidRef) -> Self {
    Self {
      curr_pat_vid: curr_pat_vid.to_string(),
      all_matched: f_bucket.all_matched.into_iter().map(Some).collect(),
      matched_with_frontiers: f_bucket.matched_with_frontiers,
      next_pat_grouped_expanding: HashMap::new(),
    }
  }

  // TODO: parallelize this function
  pub async fn incremental_load_new_edges(
    &mut self,
    pattern_es: Vec<PatternEdge>,
    pattern_vs: &HashMap<Vid, PatternVertex>,
    storage_adapter: &impl AdvancedStorageAdapter,
  ) -> HashSet<String> {
    let mut connected_data_vids = HashSet::new();

    // iter: `matched` data_graphs
    for (idx, frontiers) in self.matched_with_frontiers.drain() {
      let matched_dg = Arc::new(self.all_matched[idx].take().unwrap());

      // iter: `frontier_vid` on current data_graph
      for frontier_vid in frontiers.iter() {
        #[cfg(feature = "trace_get_adj")]
        use colored::Colorize;

        #[cfg(feature = "trace_get_adj")]
        println!(
          "🧩  Current frontier vid: {}",
          frontier_vid.to_string().green()
        );

        let mut is_frontier_formalized = false;

        // iter: `pattern_edges`
        for pat_e in pattern_es.iter() {
          #[cfg(feature = "trace_get_adj")]
          println!(
            "  🔗  Current pattern edge: {}",
            pat_e.eid().to_string().purple()
          );

          let e_label = pat_e.label();
          let e_attr = pat_e.attr.as_ref();
          let mut next_vid_grouped_conn_es = HashMap::new();
          let mut next_vid_grouped_conn_pat_strs = HashMap::new();
          let next_pat_vid;

          let is_matched_data_es_empty = if self.curr_pat_vid == pat_e.src_vid() {
            next_pat_vid = pat_e.dst_vid();
            let next_v_label = pattern_vs.get(next_pat_vid).unwrap().label.as_str();
            let next_v_attr = pattern_vs.get(next_pat_vid).unwrap().attr.as_ref();

            let matched_data_es = incremental_match_adj_e(LoadWithCondCtx {
              pattern_vs,
              storage_adapter,
              curr_matched_dg: &matched_dg,
              frontier_vid,
              curr_pat_e: pat_e,
              e_label,
              e_attr,
              next_v_label,
              next_v_attr,
              is_src_curr_pat: true,
            })
            .await;

            #[cfg(feature = "trace_get_adj")]
            println!(
              "    ✨  Found {} edges that match: ({}: {})-[{}]->({})",
              matched_data_es.len().to_string().yellow(),
              frontier_vid.to_string().green(),
              self.curr_pat_vid.as_str().cyan(),
              pat_e.eid().purple(),
              next_pat_vid.cyan()
            );

            let is_matched_data_es_empty = matched_data_es.is_empty();

            // group by: next data_vertex
            for e in matched_data_es {
              next_vid_grouped_conn_pat_strs
                .entry(e.dst_vid().to_string())
                .or_insert_with(Vec::new)
                .push(pat_e.eid().to_string());
              next_vid_grouped_conn_es
                .entry(e.dst_vid().to_string())
                .or_insert_with(Vec::new)
                .push(e);
            }
            is_matched_data_es_empty
          } else {
            next_pat_vid = pat_e.src_vid();
            let next_v_label = pattern_vs.get(next_pat_vid).unwrap().label.as_str();
            let next_v_attr = pattern_vs.get(next_pat_vid).unwrap().attr.as_ref();

            let matched_data_es = incremental_match_adj_e(LoadWithCondCtx {
              pattern_vs,
              storage_adapter,
              curr_matched_dg: &matched_dg,
              frontier_vid,
              curr_pat_e: pat_e,
              e_label,
              e_attr,
              next_v_label,
              next_v_attr,
              is_src_curr_pat: false,
            })
            .await;

            #[cfg(feature = "trace_get_adj")]
            println!(
              "    ✨  Found {} edges that match: ({}: {})<-[{}]-({})",
              matched_data_es.len().to_string().yellow(),
              frontier_vid.to_string().green(),
              self.curr_pat_vid.as_str().cyan(),
              pat_e.eid().purple(),
              next_pat_vid.cyan()
            );

            let is_matched_data_es_empty = matched_data_es.is_empty();

            // group by: next data_vertex
            for e in matched_data_es {
              next_vid_grouped_conn_pat_strs
                .entry(e.src_vid().to_string())
                .or_insert_with(Vec::new)
                .push(pat_e.eid().to_string());
              next_vid_grouped_conn_es
                .entry(e.src_vid().to_string())
                .or_insert_with(Vec::new)
                .push(e);
            }
            is_matched_data_es_empty
          };

          if is_matched_data_es_empty {
            // no matched_data_es, just skip current `frontier_vid`
            break;
          }

          is_frontier_formalized = true;

          // build `expanding_graph`
          // note that each `next_data_vertex` holds a `expanding_graph`
          for (next_vid, edges) in next_vid_grouped_conn_es {
            let mut expanding_graph = ExpandGraph::from(matched_dg.clone());
            let pat_strs = next_vid_grouped_conn_pat_strs
              .remove(&next_vid)
              .unwrap_or_default();

            expanding_graph
              .update_valid_dangling_edges(edges.iter().zip(pat_strs.iter().map(String::as_str)));
            self
              .next_pat_grouped_expanding
              .entry(next_pat_vid.to_string())
              .or_default()
              .push(expanding_graph);
          }
        }

        #[cfg(feature = "trace_get_adj")]
        println!();

        if is_frontier_formalized {
          connected_data_vids.insert(frontier_vid.to_string());
        } else {
          // if no edges are connected, this frontier is invalid, current matched_dg is invalid,
          // so we just skip the matched_dg
          break;
        }
      }
    }

    self.all_matched.clear();

    connected_data_vids
  }
}

struct LoadWithCondCtx<'a, S: AdvancedStorageAdapter> {
  pattern_vs: &'a HashMap<String, PatternVertex>,
  storage_adapter: &'a S,
  curr_matched_dg: &'a DynGraph,
  frontier_vid: VidRef<'a>,
  curr_pat_e: &'a PatternEdge,
  e_label: LabelRef<'a>,
  e_attr: Option<&'a PatternAttr>,
  next_v_label: LabelRef<'a>,
  next_v_attr: Option<&'a PatternAttr>,
  is_src_curr_pat: bool,
}

#[cfg(not(feature = "batched_incremental_match_adj_e"))]
async fn incremental_match_adj_e<'a, S: AdvancedStorageAdapter>(
  ctx: LoadWithCondCtx<'a, S>,
) -> Vec<DataEdge> {
  use futures::{StreamExt, stream};
  use itertools::Itertools;

  let next_pat_vid = if ctx.is_src_curr_pat {
    ctx.curr_pat_e.dst_vid()
  } else {
    ctx.curr_pat_e.src_vid()
  };

  // load all edges first
  let potential_edges = if ctx.is_src_curr_pat {
    ctx
      .storage_adapter
      .load_e_with_src_and_dst_filter(
        ctx.frontier_vid,
        ctx.e_label,
        ctx.e_attr,
        ctx.next_v_label,
        ctx.next_v_attr,
      )
      .await
  } else {
    ctx
      .storage_adapter
      .load_e_with_dst_and_src_filter(
        ctx.frontier_vid,
        ctx.e_label,
        ctx.e_attr,
        ctx.next_v_label,
        ctx.next_v_attr,
      )
      .await
  };

  // filter out the edges that are already matched
  let filtered_edges = potential_edges
    .into_iter()
    .filter(|e| !ctx.curr_matched_dg.has_eid(e.eid()))
    .collect_vec();

  // process each edge in parallel
  stream::iter(filtered_edges)
    .map(|e| {
      let next_vid_str = if ctx.is_src_curr_pat {
        e.dst_vid()
      } else {
        e.src_vid()
      }
      .to_string();

      async move {
        let satisfies = does_data_v_satisfy_pattern(
          &next_vid_str,
          next_pat_vid,
          ctx.pattern_vs,
          ctx.storage_adapter,
        )
        .await;
        if satisfies { Some(e) } else { None }
      }
    })
    .buffer_unordered(num_cpus::get() / 2)
    .filter_map(|r| async move { r })
    .collect::<Vec<_>>()
    .await
}

#[cfg(feature = "batched_incremental_match_adj_e")]
async fn incremental_match_adj_e<'a, S: AdvancedStorageAdapter>(
  ctx: LoadWithCondCtx<'a, S>,
) -> Vec<DataEdge> {
  use futures::future;
  use itertools::Itertools;

  const BATCH_SIZE: usize = 32;

  let next_pat_vid = if ctx.is_src_curr_pat {
    ctx.curr_pat_e.dst_vid()
  } else {
    ctx.curr_pat_e.src_vid()
  };

  // load all edges first
  let potential_edges = if ctx.is_src_curr_pat {
    ctx
      .storage_adapter
      .load_e_with_src_and_dst_filter(
        ctx.frontier_vid,
        ctx.e_label,
        ctx.e_attr,
        ctx.next_v_label,
        ctx.next_v_attr,
      )
      .await
  } else {
    ctx
      .storage_adapter
      .load_e_with_dst_and_src_filter(
        ctx.frontier_vid,
        ctx.e_label,
        ctx.e_attr,
        ctx.next_v_label,
        ctx.next_v_attr,
      )
      .await
  };

  // filter out the edges that are already matched
  let filtered_edges = potential_edges
    .into_iter()
    .filter(|e| !ctx.curr_matched_dg.has_eid(e.eid()))
    .collect_vec();

  // split into chunks for parallel processing
  let chunks = filtered_edges
    .chunks(BATCH_SIZE)
    .map(Vec::from)
    .collect_vec();

  // process each chunk in parallel
  let batch_futures = chunks.into_iter().map(|chunk| async move {
    let mut results = Vec::new();
    for e in chunk {
      let check_vid = if ctx.is_src_curr_pat {
        e.dst_vid()
      } else {
        e.src_vid()
      };
      let satisfies =
        does_data_v_satisfy_pattern(check_vid, next_pat_vid, ctx.pattern_vs, ctx.storage_adapter)
          .await;

      if satisfies {
        results.push(e);
      }
    }
    results
  });

  // wait for all futures to complete
  let batch_results = future::join_all(batch_futures).await;

  // flatten the results into a single vector
  batch_results.into_iter().flatten().collect_vec()
}

impl CBucket {
  pub async fn build_from_a_group(
    a_group: Vec<ExpandGraph>,
    loaded_v_pat_pairs: Vec<(DataVertex, String)>,
  ) -> Self {
    let mut all_expanded = vec![];
    let mut expanded_with_frontiers = HashMap::new();

    // parallelize the process: update_valid_target_vertices
    let loaded_v_pat_pairs = Arc::new(loaded_v_pat_pairs);
    let pre = parallel::spawn_blocking(move || {
      a_group
        .into_par_iter()
        .enumerate()
        .map(|(idx, mut expanding)| {
          let valid_targets = expanding.update_valid_target_vertices(loaded_v_pat_pairs.as_ref());
          (expanding, idx, valid_targets)
        })
        .collect_vec_list()
    })
    .await;

    for (expanding, idx, valid_targets) in pre.into_iter().flatten() {
      all_expanded.push(expanding);
      expanded_with_frontiers
        .entry(idx)
        .or_insert_with(Vec::new)
        .extend(valid_targets);
    }

    Self {
      all_expanded,
      expanded_with_frontiers,
    }
  }

  pub async fn build_from_t(
    t_bucket: TBucket,
    loaded_v_pat_pairs: Vec<(DataVertex, String)>,
  ) -> Self {
    let mut all_expanded = vec![];
    let mut expanded_with_frontiers = HashMap::new();

    // parallelize the process: update_valid_target_vertices
    let loaded_v_pat_pairs = Arc::new(loaded_v_pat_pairs);
    let pre = parallel::spawn_blocking(move || {
      t_bucket
        .expanding_graphs
        .into_par_iter()
        .enumerate()
        .map(|(idx, mut expanding)| {
          let valid_targets = expanding.update_valid_target_vertices(loaded_v_pat_pairs.as_ref());
          (expanding, idx, valid_targets)
        })
        .collect_vec_list()
    })
    .await;

    for (expanding, idx, valid_targets) in pre.into_iter().flatten() {
      all_expanded.push(expanding);
      expanded_with_frontiers
        .entry(idx)
        .or_insert_with(Vec::new)
        .extend(valid_targets);
    }

    Self {
      all_expanded,
      expanded_with_frontiers,
    }
  }
}

impl TBucket {
  pub async fn build_from_a_a(
    left: Vec<ExpandGraph>,
    right: Vec<ExpandGraph>,
    target_pat_vid: VidRef<'_>,
  ) -> Self {
    let expanding_graphs = Self::expand_edges_of_two(left, right).await;
    Self {
      target_pat_vid: target_pat_vid.to_owned(),
      expanding_graphs,
    }
  }

  pub async fn build_from_t_a(t_bucket: TBucket, a_group: Vec<ExpandGraph>) -> Self {
    let left_group = t_bucket.expanding_graphs;
    let right_group = a_group;

    let expanding_graphs = Self::expand_edges_of_two(left_group, right_group).await;
    Self {
      target_pat_vid: t_bucket.target_pat_vid,
      expanding_graphs,
    }
  }

  async fn expand_edges_of_two(
    left_group: Vec<ExpandGraph>,
    right_group: Vec<ExpandGraph>,
  ) -> Vec<ExpandGraph> {
    if left_group.is_empty() || right_group.is_empty() {
      return Vec::new();
    }

    parallel::spawn_blocking(move || {
      let right_group = Arc::new(right_group);

      left_group
        .into_par_iter()
        .flat_map(|left| {
          right_group
            .clone()
            .par_iter()
            .flat_map(move |right| {
              union_then_intersect_on_connective_v(left.clone(), right.clone())
            })
            .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
    })
    .await
  }
}
