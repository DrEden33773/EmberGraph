use super::*;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
#[cfg(not(feature = "intersection_force_element_paralleled"))]
use rayon::slice::ParallelSlice;

#[cfg(not(feature = "intersection_force_element_paralleled"))]
/// the min chunk size
const MIN_CHUNK_SIZE: usize = 128;
#[cfg(not(feature = "intersection_force_element_paralleled"))]
/// the max number of threads
const MAX_THREADS: usize = 32;
#[cfg(not(feature = "intersection_force_element_paralleled"))]
/// the threshold of the small dataset
const THRESHOLD_SMALL: usize = 1024;

impl TBucket {
  pub async fn build_from_a_a(
    left: Vec<ExpandGraph>,
    right: Vec<ExpandGraph>,
    target_pat_vid: VidRef<'_>,
  ) -> Self {
    let expanding_graphs = Self::intersect_two_expanding_graphs(left, right).await;
    Self {
      target_pat_vid: target_pat_vid.to_owned(),
      expanding_graphs,
    }
  }

  pub async fn build_from_a_t(a_group: Vec<ExpandGraph>, t_bucket: TBucket) -> Self {
    let left_group = a_group;
    let right_group = t_bucket.expanding_graphs;

    let expanding_graphs = Self::intersect_two_expanding_graphs(left_group, right_group).await;

    Self {
      target_pat_vid: t_bucket.target_pat_vid,
      expanding_graphs,
    }
  }

  pub async fn build_from_t_a(t_bucket: TBucket, a_group: Vec<ExpandGraph>) -> Self {
    let left_group = t_bucket.expanding_graphs;
    let right_group = a_group;

    let expanding_graphs = Self::intersect_two_expanding_graphs(left_group, right_group).await;
    Self {
      target_pat_vid: t_bucket.target_pat_vid,
      expanding_graphs,
    }
  }

  pub async fn build_from_t_t(t_bucket_1: TBucket, t_bucket_2: TBucket) -> Self {
    let left_group = t_bucket_1.expanding_graphs;
    let right_group = t_bucket_2.expanding_graphs;

    let expanding_graphs = Self::intersect_two_expanding_graphs(left_group, right_group).await;

    #[cfg(debug_assertions)]
    assert_eq!(t_bucket_1.target_pat_vid, t_bucket_2.target_pat_vid);

    Self {
      target_pat_vid: t_bucket_1.target_pat_vid,
      expanding_graphs,
    }
  }

  async fn intersect_two_expanding_graphs(
    left_group: Vec<ExpandGraph>,
    right_group: Vec<ExpandGraph>,
  ) -> Vec<ExpandGraph> {
    if left_group.is_empty() || right_group.is_empty() {
      return vec![];
    }

    let (shorter, longer) = if left_group.len() < right_group.len() {
      (left_group, right_group)
    } else {
      (right_group, left_group)
    };

    #[cfg(not(feature = "intersection_force_element_paralleled"))]
    {
      let num_threads = Self::calculate_optimal_threads(longer.len());
      let chunk_size = Self::calculate_chunk_size(longer.len(), num_threads);
      let total_elements = longer.len() * shorter.len();

      parallel::spawn_blocking(move || {
        let shorter = Arc::new(shorter);

        if total_elements < THRESHOLD_SMALL {
          // small dataset: simple parallel strategy
          Self::process_element_paralleled(&longer, &shorter)
        } else {
          // large dataset: chunk parallel strategy
          Self::process_chunk_paralleled(&longer, &shorter, chunk_size)
        }
      })
      .await
    }
    #[cfg(feature = "intersection_force_element_paralleled")]
    {
      let shorter = Arc::new(shorter);
      Self::process_element_paralleled(&longer, &shorter)
    }
  }

  #[cfg(not(feature = "intersection_force_element_paralleled"))]
  #[inline]
  fn calculate_optimal_threads(data_size: usize) -> usize {
    let available_threads = num_cpus::get();
    let optimal_threads = data_size.div_ceil(MIN_CHUNK_SIZE);
    // restrict the max number of threads
    optimal_threads.min(available_threads).min(MAX_THREADS)
  }

  #[cfg(not(feature = "intersection_force_element_paralleled"))]
  #[inline]
  fn calculate_chunk_size(data_size: usize, num_threads: usize) -> usize {
    let base_chunk_size = data_size.div_ceil(num_threads);
    // ensure the chunk size is not less than the min chunk size
    base_chunk_size.max(MIN_CHUNK_SIZE)
  }

  fn process_element_paralleled(
    longer: &[ExpandGraph],
    shorter: &Arc<Vec<ExpandGraph>>,
  ) -> Vec<ExpandGraph> {
    longer
      .par_iter()
      .flat_map(|left| {
        // inner use normal iterator, avoid thread nesting
        shorter
          .iter()
          // this `filter` will lead to a HUGE performance improvement
          .filter(|right| left.has_common_pending_v(right))
          .flat_map(|right| union_then_intersect_on_connective_v(left, right))
          .collect::<Vec<_>>()
      })
      .collect::<Vec<_>>()
  }

  #[cfg(not(feature = "intersection_force_element_paralleled"))]
  fn process_chunk_paralleled(
    longer: &[ExpandGraph],
    shorter: &Arc<Vec<ExpandGraph>>,
    chunk_size: usize,
  ) -> Vec<ExpandGraph> {
    longer
      .par_chunks(chunk_size)
      .flat_map(|chunk| {
        // inner use normal iterator, avoid thread nesting
        chunk
          .iter()
          .flat_map(|left| {
            shorter
              .iter()
              // this `filter` will lead to a HUGE performance improvement
              .filter(|right| left.has_common_pending_v(right))
              .flat_map(|right| union_then_intersect_on_connective_v(left, right))
              .collect::<Vec<_>>()
          })
          .collect::<Vec<_>>()
      })
      .collect::<Vec<_>>()
  }
}
