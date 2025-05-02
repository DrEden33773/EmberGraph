use itertools::Itertools;

use super::*;

impl ABucket {
  pub fn from_f_bucket(f_bucket: FBucket, curr_pat_vid: VidRef) -> Self {
    Self {
      curr_pat_vid: curr_pat_vid.to_string(),
      all_matched: f_bucket.all_matched.into_iter().map(Some).collect(),
      matched_with_frontiers: f_bucket.matched_with_frontiers,
      next_pat_grouped_expanding: HashMap::new(),
    }
  }

  pub async fn incremental_load_new_edges(
    &mut self,
    pattern_es: Vec<PatternEdge>,
    pattern_vs: HashMap<Vid, PatternVertex>,
    storage_adapter: Arc<impl AdvancedStorageAdapter + 'static>,
  ) {
    let curr_pat_vid: Arc<str> = self.curr_pat_vid.as_str().into();
    let pattern_es = Arc::new(pattern_es);
    let pattern_vs = Arc::new(pattern_vs);

    // channel
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    // batch configuration
    const BATCH_SIZE: usize = 8;
    let total_batches = self.matched_with_frontiers.len().div_ceil(BATCH_SIZE);
    let mut matched_graph_handles = Vec::with_capacity(total_batches);

    let matched_with_frontiers = self.matched_with_frontiers.drain().collect_vec();

    let mut all_matched_data = Vec::with_capacity(self.all_matched.len());
    all_matched_data.append(&mut self.all_matched);

    // iter: `matched` data_graphs
    for chunk in matched_with_frontiers
      .into_iter()
      .collect_vec()
      .chunks(BATCH_SIZE)
    {
      let curr_pat_vid = curr_pat_vid.clone();
      let pattern_es = pattern_es.clone();
      let pattern_vs = pattern_vs.clone();
      let storage_adapter = storage_adapter.clone();

      let matched_dg_with_frontiers = chunk
        .iter()
        .cloned()
        .map(|(idx, frontiers)| (Arc::new(all_matched_data[idx].take().unwrap()), frontiers))
        .collect_vec();

      let sender = tx.clone();

      let matched_graph_handle = tokio::spawn(async move {
        for (matched_dg, frontiers) in matched_dg_with_frontiers {
          // iter: `frontier_vid` on current data_graph
          for frontier_vid in frontiers.iter() {
            #[cfg(feature = "trace_get_adj")]
            println!(
              "\t\t🧩  Current frontier vid: {}",
              frontier_vid.to_string().green()
            );

            let mut is_frontier_formalized = false;

            // iter: `pattern_edges`
            for pat_e in pattern_es.iter() {
              #[cfg(feature = "trace_get_adj")]
              println!(
                "\t\t  🔗  Current pattern edge: {}",
                pat_e.eid().to_string().purple()
              );

              let e_label = pat_e.label();
              let e_attr = pat_e.attr.as_ref();
              let mut next_vid_grouped_conn_es = HashMap::new();
              let mut next_vid_grouped_conn_pat_strs = HashMap::new();

              let (next_pat_vid, is_matched_data_es_empty) =
                if curr_pat_vid.as_ref() == pat_e.src_vid() {
                  let next_pat_vid = pat_e.dst_vid();
                  let next_v_label = pattern_vs.get(next_pat_vid).unwrap().label.as_str();
                  let next_v_attr = pattern_vs.get(next_pat_vid).unwrap().attr.as_ref();

                  let matched_data_es = incremental_match_adj_e(LoadWithCondCtx {
                    storage_adapter: storage_adapter.as_ref(),
                    curr_matched_dg: &matched_dg,
                    frontier_vid: frontier_vid.as_str(),
                    e_label,
                    e_attr,
                    next_v_label,
                    next_v_attr,
                    is_src_curr_pat: true,
                  })
                  .await;

                  #[cfg(feature = "trace_get_adj")]
                  println!(
                    "\t\t    ✨  Found {} edges that match: ({}: {})-[{}]->({})",
                    matched_data_es.len().to_string().yellow(),
                    frontier_vid.to_string().green(),
                    curr_pat_vid.as_ref().cyan(),
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

                  (next_pat_vid, is_matched_data_es_empty)
                } else {
                  let next_pat_vid = pat_e.src_vid();
                  let next_v_label = pattern_vs.get(next_pat_vid).unwrap().label.as_str();
                  let next_v_attr = pattern_vs.get(next_pat_vid).unwrap().attr.as_ref();

                  let matched_data_es = incremental_match_adj_e(LoadWithCondCtx {
                    storage_adapter: storage_adapter.as_ref(),
                    curr_matched_dg: &matched_dg,
                    frontier_vid: frontier_vid.as_str(),
                    e_label,
                    e_attr,
                    next_v_label,
                    next_v_attr,
                    is_src_curr_pat: false,
                  })
                  .await;

                  #[cfg(feature = "trace_get_adj")]
                  println!(
                    "\t\t    ✨  Found {} edges that match: ({}: {})<-[{}]-({})",
                    matched_data_es.len().to_string().yellow(),
                    frontier_vid.to_string().green(),
                    curr_pat_vid.as_ref().cyan(),
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

                  (next_pat_vid, is_matched_data_es_empty)
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

                expanding_graph.update_valid_dangling_edges(
                  edges.iter().zip(pat_strs.iter().map(String::as_str)),
                );
                expanding_graph.sort_key_after_update_valid_target_vertices();

                let sender = sender.clone();

                sender
                  .send((next_pat_vid.to_string(), expanding_graph))
                  .unwrap_or_else(|_| {
                    panic!(
                      "❌  Failed to send {} to channel",
                      format!("({}, <expanding_graph>)", next_pat_vid).yellow()
                    );
                  });
              }
            }

            if !is_frontier_formalized {
              // if no edges are connected, this frontier is invalid, current matched_dg is invalid,
              // so we just skip the matched_dg
              break;
            }
          }
        }
      });

      matched_graph_handles.push(matched_graph_handle);
    }

    // don't forget to close the channel
    drop(tx);

    // receive the (<next_pat_vid>, <expanding_graph>) pairs
    while let Some((next_pat_vid, expanding_graph)) = rx.recv().await {
      // update the `next_pat_grouped_expanding` with the new expanding_graph
      self
        .next_pat_grouped_expanding
        .entry(next_pat_vid)
        .or_default()
        .push(expanding_graph);
    }

    // wait for all tasks to complete
    for handle in matched_graph_handles {
      if let Err(e) = handle.await {
        eprintln!("❌  Task failed: {}", e);
      }
    }

    self.all_matched.clear();
  }
}

struct LoadWithCondCtx<'a, S: AdvancedStorageAdapter> {
  storage_adapter: &'a S,
  curr_matched_dg: &'a DynGraph,
  frontier_vid: VidRef<'a>,
  e_label: LabelRef<'a>,
  e_attr: Option<&'a PatternAttr>,
  next_v_label: LabelRef<'a>,
  next_v_attr: Option<&'a PatternAttr>,
  is_src_curr_pat: bool,
}

async fn incremental_match_adj_e<'a, S: AdvancedStorageAdapter>(
  ctx: LoadWithCondCtx<'a, S>,
) -> Vec<DataEdge> {
  // load all edges first
  let loaded_edges = if ctx.is_src_curr_pat {
    ctx
      .storage_adapter
      .load_e_with_src_and_dst_filter(
        ctx.frontier_vid.as_ref(),
        ctx.e_label.as_ref(),
        ctx.e_attr,
        ctx.next_v_label.as_ref(),
        ctx.next_v_attr,
      )
      .await
  } else {
    ctx
      .storage_adapter
      .load_e_with_dst_and_src_filter(
        ctx.frontier_vid.as_ref(),
        ctx.e_label.as_ref(),
        ctx.e_attr,
        ctx.next_v_label.as_ref(),
        ctx.next_v_attr,
      )
      .await
  };

  // filter out the edges that are already matched
  // NOTE: DO NOT block the task
  loaded_edges
    .into_par_iter()
    .filter(|e| !ctx.curr_matched_dg.has_eid(e.eid()))
    .collect()
}
