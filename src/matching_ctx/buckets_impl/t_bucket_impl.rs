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
/// the threshold for using special parallel algorithm for extremely large datasets
const THRESHOLD_VERY_LARGE: usize = 50_000;
/// the threshold for minimum element-to-thread ratio
const MIN_ELEMENTS_PER_THREAD: usize = 64;
/// the number of partition buckets for parallel histogram approach
const HISTOGRAM_BUCKETS: usize = 256;

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

      // first check if it is a very large dataset
      let is_very_large_dataset = total_elements > THRESHOLD_VERY_LARGE * 10;

      parallel::spawn_blocking(move || {
        let shorter = Arc::new(shorter);

        if total_elements < THRESHOLD_SMALL {
          // small dataset: simple parallel strategy
          Self::process_element_paralleled(&longer, &shorter)
        } else if is_very_large_dataset {
          // very large dataset: use SIMD-accelerated histogram parallel strategy
          Self::process_simd_paralleled(&longer, &shorter)
        } else if total_elements > THRESHOLD_VERY_LARGE {
          // large dataset: use histogram parallel optimization strategy
          Self::process_histogram_paralleled(&longer, &shorter)
        } else {
          // normal dataset: chunk parallel strategy
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
    // consider the data size to dynamically adjust the number of threads
    let optimal_threads = (data_size / MIN_ELEMENTS_PER_THREAD).max(1);
    // limit the maximum number of threads
    optimal_threads.min(available_threads).min(MAX_THREADS)
  }

  #[cfg(not(feature = "intersection_force_element_paralleled"))]
  #[inline]
  fn calculate_chunk_size(data_size: usize, num_threads: usize) -> usize {
    let base_chunk_size = data_size.div_ceil(num_threads);
    // ensure the chunk size is not less than the minimum chunk size
    base_chunk_size.max(MIN_CHUNK_SIZE)
  }

  fn process_element_paralleled(
    longer: &[ExpandGraph],
    shorter: &Arc<Vec<ExpandGraph>>,
  ) -> Vec<ExpandGraph> {
    // check if the data size is large enough to use the optimized version
    if longer.len() > 500 && shorter.len() > 500 {
      // use the optimized version for large datasets
      longer
        .par_iter()
        .flat_map(|left| {
          shorter
            .par_iter()
            .filter(|right| left.has_common_pending_v_optimized(right))
            .flat_map(|right| union_then_intersect_on_connective_v(left, right))
            .collect::<Vec<_>>()
        })
        .collect()
    } else {
      // use the standard version for small datasets
      longer
        .par_iter()
        .flat_map(|left| {
          shorter
            .par_iter()
            .filter(|right| left.has_common_pending_v(right))
            .flat_map(|right| union_then_intersect_on_connective_v(left, right))
            .collect::<Vec<_>>()
        })
        .collect()
    }
  }

  #[cfg(not(feature = "intersection_force_element_paralleled"))]
  fn process_chunk_paralleled(
    longer: &[ExpandGraph],
    shorter: &Arc<Vec<ExpandGraph>>,
    chunk_size: usize,
  ) -> Vec<ExpandGraph> {
    // check if the data size is large enough to use the optimized version
    let use_optimized = longer.len() > 500 && shorter.len() > 500;

    longer
      .par_chunks(chunk_size)
      .flat_map(|chunk| {
        // create a buffer of appropriate size to reduce memory allocation
        let mut results = Vec::with_capacity(chunk.len());

        for left in chunk {
          let mut left_results = Vec::new();

          // select the optimized method based on the data size
          if use_optimized {
            for right in shorter.iter() {
              if left.has_common_pending_v_optimized(right) {
                left_results.extend(union_then_intersect_on_connective_v(left, right));
              }
            }
          } else {
            for right in shorter.iter() {
              if left.has_common_pending_v(right) {
                left_results.extend(union_then_intersect_on_connective_v(left, right));
              }
            }
          }

          results.extend(left_results);
        }

        results
      })
      .collect()
  }

  #[cfg(not(feature = "intersection_force_element_paralleled"))]
  fn process_histogram_paralleled(
    longer: &[ExpandGraph],
    shorter: &Arc<Vec<ExpandGraph>>,
  ) -> Vec<ExpandGraph> {
    // 1. split the longer collection into buckets, grouped by the first character of pending_v or hash value
    let mut buckets = vec![vec![]; HISTOGRAM_BUCKETS];

    // sort the data into buckets, grouped by the first character of pending_v or hash value
    for (i, graph) in longer.iter().enumerate() {
      if let Some((key, _)) = graph.pending_v_grouped_dangling_eids.first() {
        let hash = key.bytes().next().unwrap_or(0) as usize % HISTOGRAM_BUCKETS;
        buckets[hash].push(i);
      }
    }

    // 2. preprocess the hash distribution of the shorter collection - performance optimization point
    let mut shorter_hash_map = vec![Vec::new(); HISTOGRAM_BUCKETS];
    for (i, graph) in shorter.iter().enumerate() {
      for (key, _) in &graph.pending_v_grouped_dangling_eids {
        let hash = key.bytes().next().unwrap_or(0) as usize % HISTOGRAM_BUCKETS;
        shorter_hash_map[hash].push(i);
      }
    }

    // 3. parallel process each bucket
    buckets
      .into_par_iter()
      .enumerate()
      .flat_map(|(bucket_idx, indices)| {
        // if the bucket is empty, skip it
        if indices.is_empty() {
          return vec![];
        }

        // collect the actual elements in the bucket
        let bucket_graphs: Vec<&ExpandGraph> = indices.iter().map(|&i| &longer[i]).collect();

        // get the indices of the elements in the shorter collection that may match the current bucket
        let shorter_indices = &shorter_hash_map[bucket_idx];

        // if there is no potential match, skip it
        if shorter_indices.is_empty() {
          return vec![];
        }

        // get the actual elements in the shorter collection that may match the current bucket
        let filtered_shorter: Vec<&ExpandGraph> =
          shorter_indices.iter().map(|&i| &shorter[i]).collect();

        // use the optimized version of the common point detection for intersection calculation
        bucket_graphs
          .into_iter()
          .flat_map(|left| {
            filtered_shorter
              .iter()
              .filter(|&&right| left.has_common_pending_v_optimized(right))
              .flat_map(|&right| union_then_intersect_on_connective_v(left, right))
              .collect::<Vec<_>>()
          })
          .collect::<Vec<_>>()
      })
      .collect()
  }

  #[cfg(not(feature = "intersection_force_element_paralleled"))]
  fn process_simd_paralleled(
    longer: &[ExpandGraph],
    shorter: &Arc<Vec<ExpandGraph>>,
  ) -> Vec<ExpandGraph> {
    use rayon::prelude::*;

    // maximum number of parallel groups, adjusted according to the number of available threads
    let max_groups = num_cpus::get();

    // 1. split the data into groups
    let longer_chunks: Vec<_> = longer.chunks(longer.len().div_ceil(max_groups)).collect();
    let num_chunks = longer_chunks.len();

    // 2. parallel process each group
    (0..num_chunks)
      .into_par_iter()
      .flat_map(|chunk_idx| {
        let chunk = longer_chunks[chunk_idx];

        // create a character mask for the current chunk
        let mut char_masks = [false; 256];

        // collect all possible prefix characters
        for graph in chunk {
          for (vid, _) in &graph.pending_v_grouped_dangling_eids {
            if let Some(first_byte) = vid.bytes().next() {
              char_masks[first_byte as usize] = true;
            }
          }
        }

        // filter the potential matches
        let potential_matches: Vec<_> = shorter
          .iter()
          .enumerate()
          .filter(|(_, right)| {
            // quick check if there is any potential match
            right.pending_v_grouped_dangling_eids.keys().any(|vid| {
              if let Some(first_byte) = vid.bytes().next() {
                char_masks[first_byte as usize]
              } else {
                false
              }
            })
          })
          .collect();

        // process each graph in the chunk with the potential matches
        chunk
          .par_iter()
          .flat_map(|left| {
            let mut results = Vec::new();

            for (_, right) in &potential_matches {
              if left.has_common_pending_v_optimized(right) {
                results.extend(union_then_intersect_on_connective_v(left, right));
              }
            }

            results
          })
          .collect::<Vec<_>>()
      })
      .collect()
  }
}
