use super::*;

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
