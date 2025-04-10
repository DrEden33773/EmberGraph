use super::*;

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
          let valid_targets =
            expanding.intersect_on_valid_target_vertices(loaded_v_pat_pairs.as_ref());
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
          let valid_targets =
            expanding.intersect_on_valid_target_vertices(loaded_v_pat_pairs.as_ref());
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
