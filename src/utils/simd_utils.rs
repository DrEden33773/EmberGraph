//! SIMD-accelerated utilities
//!
//! This module provides SIMD-accelerated utilities for specific operations.
//! It uses standard library features of Rust, without external dependencies,
//! and will get better performance on CPUs that support SIMD.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Helper structure for fast string hashing.
///
/// This is a near SIMD-optimized string hashing algorithm, aiming to improve hash performance.
pub struct FastStringHasher {
  state: u64,
}

impl FastStringHasher {
  #[inline]
  pub fn new() -> Self {
    Self { state: 0 }
  }

  #[inline]
  pub fn hash_str(&mut self, s: &str) -> u64 {
    // handle the special case of short strings
    if s.len() <= 8 {
      let mut hasher = DefaultHasher::new();
      s.hash(&mut hasher);
      return hasher.finish();
    }

    // for long strings, use batch processing to improve efficiency
    let bytes = s.as_bytes();
    let chunks = bytes.chunks_exact(8);
    let remainder = chunks.remainder();

    // handle 8-byte blocks
    for chunk in chunks {
      let value = u64::from_ne_bytes([
        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
      ]);
      self.state = self.state.wrapping_mul(0x517cc1b727220a95).rotate_left(5) ^ value;
    }

    // handle the remaining bytes
    if !remainder.is_empty() {
      let mut buf = [0u8; 8];
      buf[..remainder.len()].copy_from_slice(remainder);
      let value = u64::from_ne_bytes(buf);
      self.state = self.state.wrapping_add(value);
    }

    // final mix
    self.state = self.state ^ (self.state >> 32);
    self.state
  }
}

impl Default for FastStringHasher {
  fn default() -> Self {
    Self::new()
  }
}

/// Helper function for batch string comparison.
///
/// This function uses precomputed hashes to accelerate the intersection judgment of string collections.
#[inline]
pub fn has_intersection_with_hashes<'a, I, J>(shorter: I, longer: J) -> bool
where
  I: Iterator<Item = &'a str>,
  J: Iterator<Item = &'a str>,
{
  // precompute the hashes of the first collection
  let mut hasher = FastStringHasher::new();
  let shorter_hashes: Vec<u64> = shorter
    .map(|s| {
      hasher.state = 0;
      hasher.hash_str(s)
    })
    .collect();

  if shorter_hashes.is_empty() {
    return false;
  }

  // compute the hashes of the second collection and check for intersection
  for s in longer {
    hasher.state = 0;
    let hash = hasher.hash_str(s);
    if shorter_hashes.contains(&hash) {
      return true;
    }
  }

  false
}

/// Helper structure for batch string collection operations.
///
/// Simplified version suitable for TBucket module.
pub struct BatchStringMatcher {
  /// The mask array, used to quickly filter, save the occurrence of the first character
  pub char_masks: [bool; 256],

  /// Other possible mapping data
  pub group_mapping: Vec<usize>,
}

impl BatchStringMatcher {
  pub fn new() -> Self {
    Self {
      char_masks: [false; 256],
      group_mapping: Vec::new(),
    }
  }
}

impl Default for BatchStringMatcher {
  fn default() -> Self {
    Self::new()
  }
}

/// Create the intersection of two string collections.
///
/// Use bucket strategy to optimize processing of large collections.
#[inline]
pub fn fast_string_intersection<'a, I, J>(set1: I, set2: J) -> Vec<&'a str>
where
  I: Iterator<Item = &'a str>,
  J: Iterator<Item = &'a str>,
{
  // split the elements of the first collection into 256 buckets
  let mut buckets = vec![Vec::new(); 256];
  for s in set1 {
    if let Some(first_byte) = s.bytes().next() {
      buckets[first_byte as usize].push(s);
    }
  }

  // check each element of the second collection whether it is in the corresponding bucket
  let mut result = Vec::new();
  for s in set2 {
    if let Some(first_byte) = s.bytes().next() {
      let bucket = &buckets[first_byte as usize];
      if bucket.contains(&s) {
        result.push(s);
      }
    }
  }

  result
}
