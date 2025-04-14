use super::*;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use rayon::slice::ParallelSlice;

/// the min chunk size
const MIN_CHUNK_SIZE: usize = 100;
/// the max number of threads
const MAX_THREADS: usize = 32;
/// the threshold of the small dataset
const THRESHOLD_SMALL: usize = 1000;

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

  pub async fn build_from_a_t(a_group: Vec<ExpandGraph>, t_bucket: TBucket) -> Self {
    let left_group = a_group;
    let right_group = t_bucket.expanding_graphs;

    let expanding_graphs = Self::expand_edges_of_two(left_group, right_group).await;

    Self {
      target_pat_vid: t_bucket.target_pat_vid,
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

  pub async fn build_from_t_t(t_bucket_1: TBucket, t_bucket_2: TBucket) -> Self {
    let left_group = t_bucket_1.expanding_graphs;
    let right_group = t_bucket_2.expanding_graphs;

    let expanding_graphs = Self::expand_edges_of_two(left_group, right_group).await;

    #[cfg(debug_assertions)]
    assert_eq!(t_bucket_1.target_pat_vid, t_bucket_2.target_pat_vid);

    Self {
      target_pat_vid: t_bucket_1.target_pat_vid,
      expanding_graphs,
    }
  }

  async fn expand_edges_of_two(
    left_group: Vec<ExpandGraph>,
    right_group: Vec<ExpandGraph>,
  ) -> Vec<ExpandGraph> {
    if left_group.is_empty() || right_group.is_empty() {
      return vec![];
    }

    #[cfg(feature = "monitor_intersect_expanding_graphs")]
    let start_time = std::time::Instant::now();

    let (shorter, longer) = if left_group.len() < right_group.len() {
      (left_group, right_group)
    } else {
      (right_group, left_group)
    };

    let num_threads = Self::calculate_optimal_threads(longer.len());
    let chunk_size = Self::calculate_chunk_size(longer.len(), num_threads);

    let total_elements = longer.len() * shorter.len();
    #[cfg(feature = "monitor_intersect_expanding_graphs")]
    println!(
      "Processing {} elements with {} threads, chunk size: {}",
      total_elements, num_threads, chunk_size
    );

    let result = parallel::spawn_blocking(move || {
      let shorter = Arc::new(shorter);

      if total_elements < THRESHOLD_SMALL {
        // small dataset: simple parallel strategy
        Self::process_small_dataset(&longer, &shorter)
      } else {
        // large dataset: chunk parallel strategy
        Self::process_large_dataset(&longer, &shorter, chunk_size)
      }
    })
    .await;

    #[cfg(feature = "monitor_intersect_expanding_graphs")]
    {
      let duration = start_time.elapsed();
      println!("Processed {} elements in {:?}", total_elements, duration);
    }

    result
  }

  fn calculate_optimal_threads(data_size: usize) -> usize {
    let available_threads = num_cpus::get();
    let optimal_threads = data_size.div_ceil(MIN_CHUNK_SIZE);
    // restrict the max number of threads
    optimal_threads.min(available_threads).min(MAX_THREADS)
  }

  fn calculate_chunk_size(data_size: usize, num_threads: usize) -> usize {
    let base_chunk_size = data_size.div_ceil(num_threads);
    // ensure the chunk size is not less than the min chunk size
    base_chunk_size.max(MIN_CHUNK_SIZE)
  }

  fn process_small_dataset(
    longer: &[ExpandGraph],
    shorter: &Arc<Vec<ExpandGraph>>,
  ) -> Vec<ExpandGraph> {
    longer
      .par_iter()
      .flat_map(|left| {
        // inner use normal iterator, avoid thread nesting
        shorter
          .iter()
          .flat_map(|right| union_then_intersect_on_connective_v(left, right))
          .collect::<Vec<_>>()
      })
      .collect::<Vec<_>>()
  }

  fn process_large_dataset(
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
              .flat_map(|right| union_then_intersect_on_connective_v(left, right))
              .collect::<Vec<_>>()
          })
          .collect::<Vec<_>>()
      })
      .collect::<Vec<_>>()
  }
}
