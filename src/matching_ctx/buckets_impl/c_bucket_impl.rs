use super::*;
use crate::storage::StorageAdapter;
#[cfg(not(feature = "use_tokio_mpsc_unbounded_channel"))]
use futures::TryFutureExt;

impl CBucket {
  pub async fn build_from_a_group_lazy(
    a_group: Vec<ExpandGraph>,
    pattern_str: Arc<str>,
    expected_label: Arc<str>,
    expected_attr: Option<Arc<PatternAttr>>,
    storage_adapter: Arc<impl StorageAdapter + 'static>,
  ) -> Self {
    let mut all_expanded = vec![];
    let mut expanded_with_frontiers = HashMap::new();

    // channel
    #[cfg(feature = "use_tokio_mpsc_unbounded_channel")]
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    #[cfg(not(feature = "use_tokio_mpsc_unbounded_channel"))]
    let (tx, mut rx) = tokio::sync::mpsc::channel(a_group.len() + 4);

    for (idx, mut expanding) in a_group.into_iter().enumerate() {
      let tx = tx.clone();
      let pattern_str = pattern_str.clone();
      let storage_adapter = storage_adapter.clone();
      let expected_label = expected_label.clone();
      let expected_attr = expected_attr.clone();

      tokio::spawn(async move {
        let valid_targets = expanding
          .lazy_intersect_valid_target_vertices(
            pattern_str.clone(),
            expected_label,
            expected_attr,
            storage_adapter.clone(),
          )
          .await;

        #[cfg(feature = "use_tokio_mpsc_unbounded_channel")]
        tx.send((expanding, idx, valid_targets))
          .unwrap_or_else(|_| {
            panic!(
              "❌  Failed to send {} to channel",
              format!("({}, <pattern_v>)", idx).yellow()
            );
          });
        #[cfg(not(feature = "use_tokio_mpsc_unbounded_channel"))]
        tx.send((expanding, idx, valid_targets))
          .unwrap_or_else(|_| {
            panic!(
              "❌  Failed to send {} to channel",
              format!("({}, <pattern_v>)", idx).yellow()
            );
          })
          .await;
      });
    }

    // don't forget to close the channel
    drop(tx);

    while let Some((expanding, idx, mut valid_targets)) = rx.recv().await {
      all_expanded.push(expanding);
      expanded_with_frontiers
        .entry(idx)
        .or_insert_with(Vec::new)
        .append(&mut valid_targets);
    }

    Self {
      all_expanded,
      expanded_with_frontiers,
    }
  }

  pub async fn build_from_a_group(
    a_group: Vec<ExpandGraph>,
    #[cfg(feature = "use_sort_merge_join")] loaded_v_pat_pairs_asc: Vec<(DataVertex, String)>,
    #[cfg(not(feature = "use_sort_merge_join"))] loaded_v_pat_pairs: Vec<(DataVertex, String)>,
  ) -> Self {
    let mut all_expanded = vec![];
    let mut expanded_with_frontiers = HashMap::new();

    // parallelize the process: update_valid_target_vertices
    #[cfg(feature = "use_sort_merge_join")]
    let loaded_v_pat_pairs_asc = Arc::new(loaded_v_pat_pairs_asc);
    #[cfg(not(feature = "use_sort_merge_join"))]
    let loaded_v_pat_pairs = Arc::new(loaded_v_pat_pairs);

    let pre = parallel::spawn_blocking(move || {
      a_group
        .into_par_iter()
        .enumerate()
        .map(|(idx, mut expanding)| {
          #[cfg(feature = "use_sort_merge_join")]
          let valid_targets =
            expanding.intersect_valid_target_vertices(loaded_v_pat_pairs_asc.as_ref());
          #[cfg(not(feature = "use_sort_merge_join"))]
          let valid_targets =
            expanding.intersect_valid_target_vertices(loaded_v_pat_pairs.as_ref());
          (expanding, idx, valid_targets)
        })
        .collect_vec_list()
    })
    .await;

    for (expanding, idx, mut valid_targets) in pre.into_iter().flatten() {
      all_expanded.push(expanding);
      expanded_with_frontiers
        .entry(idx)
        .or_insert_with(Vec::new)
        .append(&mut valid_targets);
    }

    Self {
      all_expanded,
      expanded_with_frontiers,
    }
  }

  pub async fn build_from_t_lazy(
    t_bucket: TBucket,
    pattern_str: Arc<str>,
    expected_label: Arc<str>,
    expected_attr: Option<Arc<PatternAttr>>,
    storage_adapter: Arc<impl StorageAdapter + 'static>,
  ) -> Self {
    let mut all_expanded = vec![];
    let mut expanded_with_frontiers = HashMap::new();

    // channel
    #[cfg(feature = "use_tokio_mpsc_unbounded_channel")]
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    #[cfg(not(feature = "use_tokio_mpsc_unbounded_channel"))]
    let (tx, mut rx) = tokio::sync::mpsc::channel(t_bucket.expanding_graphs.len() + 4);

    for (idx, mut expanding) in t_bucket.expanding_graphs.into_iter().enumerate() {
      let tx = tx.clone();
      let pattern_str = pattern_str.clone();
      let storage_adapter = storage_adapter.clone();
      let expected_label = expected_label.clone();
      let expected_attr = expected_attr.clone();

      tokio::spawn(async move {
        let valid_targets = expanding
          .lazy_intersect_valid_target_vertices(
            pattern_str.clone(),
            expected_label,
            expected_attr,
            storage_adapter.clone(),
          )
          .await;

        #[cfg(feature = "use_tokio_mpsc_unbounded_channel")]
        tx.send((expanding, idx, valid_targets))
          .unwrap_or_else(|_| {
            panic!(
              "❌  Failed to send {} to channel",
              format!("({}, <pattern_v>)", idx).yellow()
            );
          });
        #[cfg(not(feature = "use_tokio_mpsc_unbounded_channel"))]
        tx.send((expanding, idx, valid_targets))
          .unwrap_or_else(|_| {
            panic!(
              "❌  Failed to send {} to channel",
              format!("({}, <pattern_v>)", idx).yellow()
            );
          })
          .await;
      });
    }

    // don't forget to close the channel
    drop(tx);

    while let Some((expanding, idx, mut valid_targets)) = rx.recv().await {
      all_expanded.push(expanding);
      expanded_with_frontiers
        .entry(idx)
        .or_insert_with(Vec::new)
        .append(&mut valid_targets);
    }

    Self {
      all_expanded,
      expanded_with_frontiers,
    }
  }

  pub async fn build_from_t(
    t_bucket: TBucket,
    #[cfg(feature = "use_sort_merge_join")] loaded_v_pat_pairs_asc: Vec<(DataVertex, String)>,
    #[cfg(not(feature = "use_sort_merge_join"))] loaded_v_pat_pairs: Vec<(DataVertex, String)>,
  ) -> Self {
    let mut all_expanded = vec![];
    let mut expanded_with_frontiers = HashMap::new();

    // parallelize the process: update_valid_target_vertices
    #[cfg(feature = "use_sort_merge_join")]
    let loaded_v_pat_pairs_asc = Arc::new(loaded_v_pat_pairs_asc);
    #[cfg(not(feature = "use_sort_merge_join"))]
    let loaded_v_pat_pairs = Arc::new(loaded_v_pat_pairs);

    let pre = parallel::spawn_blocking(move || {
      t_bucket
        .expanding_graphs
        .into_par_iter()
        .enumerate()
        .map(|(idx, mut expanding)| {
          #[cfg(feature = "use_sort_merge_join")]
          let valid_targets =
            expanding.intersect_valid_target_vertices(loaded_v_pat_pairs_asc.as_ref());
          #[cfg(not(feature = "use_sort_merge_join"))]
          let valid_targets =
            expanding.intersect_valid_target_vertices(loaded_v_pat_pairs.as_ref());
          (expanding, idx, valid_targets)
        })
        .collect_vec_list()
    })
    .await;

    for (expanding, idx, mut valid_targets) in pre.into_iter().flatten() {
      all_expanded.push(expanding);
      expanded_with_frontiers
        .entry(idx)
        .or_insert_with(Vec::new)
        .append(&mut valid_targets);
    }

    Self {
      all_expanded,
      expanded_with_frontiers,
    }
  }
}
