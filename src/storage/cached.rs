use super::{AdvancedStorageAdapter, AsyncDefault, StorageAdapter};
use crate::schemas::{DataEdge, DataVertex, LabelRef, PatternAttr, VidRef};
use lru::LruCache;
use parking_lot::Mutex;
use std::{
  hash::{DefaultHasher, Hash, Hasher},
  num::NonZeroUsize,
  sync::Arc,
};
use tokio::sync::RwLock;

const DEFAULT_CACHE_SIZE: usize = 256;

#[derive(Clone, Eq, PartialEq, Hash)]
enum CacheKey {
  /// Single vertex by ID.
  Vertex(
    String, // vid
  ),

  /// Vertices by label and optional attribute.
  VerticesByLabel(
    String,                    // v_label
    Option<CachedPatternAttr>, // v_attr
  ),

  /// Edges by source vertex, label, and optional attribute.
  EdgesBySrc(
    String,                    // src_vid
    String,                    // e_label
    Option<CachedPatternAttr>, // e_attr
  ),

  /// Edges by destination vertex, label, and optional attribute.
  EdgesByDst(
    String,                    // dst_vid
    String,                    // e_label
    Option<CachedPatternAttr>, // e_attr
  ),

  /// Edges by source vertex with destination filter, label, and optional attribute.
  EdgesBySrcWithDstFilter(
    String,                    // src_vid
    String,                    // e_label
    Option<CachedPatternAttr>, // e_attr
    String,                    // dst_v_label
    Option<CachedPatternAttr>, // dst_v_attr
  ),

  /// Edges by destination vertex with source filter, label, and optional attribute.
  EdgesByDstWithSrcFilter(
    String,                    // dst_vid
    String,                    // e_label
    Option<CachedPatternAttr>, // e_attr
    String,                    // src_v_label
    Option<CachedPatternAttr>, // src_v_attr
  ),
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct CachedPatternAttr {
  key: String,
  op_repr: String,
  value_hash: u64,
}

impl From<&PatternAttr> for CachedPatternAttr {
  fn from(attr: &PatternAttr) -> Self {
    let mut hasher = DefaultHasher::new();
    format!("{:?}", attr.value).hash(&mut hasher);

    Self {
      key: attr.key.clone(),
      op_repr: format!("{:?}", attr.op.to_neo4j_sqlite_repr()),
      value_hash: hasher.finish(),
    }
  }
}

struct StorageCache {
  vertex_cache: Mutex<LruCache<CacheKey, Option<DataVertex>>>,
  vertices_cache: Mutex<LruCache<CacheKey, Vec<DataVertex>>>,
  edges_cache: Mutex<LruCache<CacheKey, Vec<DataEdge>>>,
}

impl StorageCache {
  fn new(cache_size: usize) -> Self {
    let cache_size =
      NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap());

    Self {
      vertex_cache: Mutex::new(LruCache::new(cache_size)),
      vertices_cache: Mutex::new(LruCache::new(cache_size)),
      edges_cache: Mutex::new(LruCache::new(cache_size)),
    }
  }
}

#[derive(Clone)]
pub struct CachedStorageAdapter<S: StorageAdapter> {
  inner: S,
  cache: Arc<RwLock<StorageCache>>,
}

impl<S: StorageAdapter> CachedStorageAdapter<S> {
  pub fn new(inner: S, cache_size: usize) -> Self {
    let cache = Arc::new(RwLock::new(StorageCache::new(cache_size)));
    Self { inner, cache }
  }

  /// equivalent to `python.@lru_cache.cache_clear`
  pub async fn cache_clear(&self) {
    let cache = self.cache.write().await;
    cache.vertex_cache.lock().clear();
    cache.vertices_cache.lock().clear();
    cache.edges_cache.lock().clear();
  }
}

impl<S: StorageAdapter> AsyncDefault for CachedStorageAdapter<S> {
  async fn async_default() -> Self {
    let inner = S::async_default().await;
    Self::new(inner, DEFAULT_CACHE_SIZE)
  }
}

impl<S: StorageAdapter> StorageAdapter for CachedStorageAdapter<S> {
  async fn get_v(&self, vid: VidRef<'_>) -> Option<DataVertex> {
    let key = CacheKey::Vertex(vid.to_string());

    // try to get cache
    let cache = self.cache.read().await;
    if let Some(vertex) = { cache.vertex_cache.lock() }.get(&key) {
      return vertex.clone();
    }
    drop(cache); // release read lock

    // not found in cache, fetch from inner storage
    let result = self.inner.get_v(vid).await;

    // update cache with result
    let cache = self.cache.write().await;
    cache.vertex_cache.lock().put(key, result.clone());

    result
  }

  async fn load_v(&self, v_label: LabelRef<'_>, v_attr: Option<&PatternAttr>) -> Vec<DataVertex> {
    let attr_cache = v_attr.map(CachedPatternAttr::from);
    let key = CacheKey::VerticesByLabel(v_label.to_string(), attr_cache);

    let cache = self.cache.read().await;
    if let Some(result) = { cache.vertices_cache.lock() }.get(&key).cloned() {
      return result;
    }
    drop(cache);

    let result = self.inner.load_v(v_label, v_attr).await;

    let cache = self.cache.write().await;
    { cache.vertices_cache.lock() }.put(key, result.clone());

    result
  }

  async fn load_e(&self, e_label: LabelRef<'_>, e_attr: Option<&PatternAttr>) -> Vec<DataEdge> {
    // This is a broad query that might return a lot of data.
    // We'll skip caching for this case to avoid memory pressure.
    self.inner.load_e(e_label, e_attr).await
  }

  async fn load_e_with_src(
    &self,
    src_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let attr_cache = e_attr.map(CachedPatternAttr::from);
    let key = CacheKey::EdgesBySrc(src_vid.to_string(), e_label.to_string(), attr_cache);

    let cache = self.cache.read().await;
    if let Some(result) = { cache.edges_cache.lock() }.get(&key).cloned() {
      return result;
    }
    drop(cache);

    let result = self.inner.load_e_with_src(src_vid, e_label, e_attr).await;

    let cache = self.cache.write().await;
    { cache.edges_cache.lock() }.put(key, result.clone());

    result
  }

  async fn load_e_with_dst(
    &self,
    dst_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let attr_cache = e_attr.map(CachedPatternAttr::from);
    let key = CacheKey::EdgesByDst(dst_vid.to_string(), e_label.to_string(), attr_cache);

    let cache = self.cache.read().await;
    if let Some(result) = { cache.edges_cache.lock() }.get(&key).cloned() {
      return result;
    }
    drop(cache);

    let result = self.inner.load_e_with_dst(dst_vid, e_label, e_attr).await;

    let cache = self.cache.write().await;
    { cache.edges_cache.lock() }.put(key, result.clone());

    result
  }
}

impl<S: AdvancedStorageAdapter> AdvancedStorageAdapter for CachedStorageAdapter<S> {
  async fn load_e_with_src_and_dst_filter(
    &self,
    src_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
    dst_v_label: LabelRef<'_>,
    dst_v_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let e_attr_cache = e_attr.map(CachedPatternAttr::from);
    let dst_v_attr_cache = dst_v_attr.map(CachedPatternAttr::from);
    let key = CacheKey::EdgesBySrcWithDstFilter(
      src_vid.to_string(),
      e_label.to_string(),
      e_attr_cache,
      dst_v_label.to_string(),
      dst_v_attr_cache,
    );

    let cache = self.cache.read().await;
    if let Some(result) = { cache.edges_cache.lock() }.get(&key).cloned() {
      return result;
    }
    drop(cache);

    let result = self
      .inner
      .load_e_with_src_and_dst_filter(src_vid, e_label, e_attr, dst_v_label, dst_v_attr)
      .await;

    let cache = self.cache.write().await;
    { cache.edges_cache.lock() }.put(key, result.clone());

    result
  }

  async fn load_e_with_dst_and_src_filter(
    &self,
    dst_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
    src_v_label: LabelRef<'_>,
    src_v_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let e_attr_cache = e_attr.map(CachedPatternAttr::from);
    let src_v_attr_cache = src_v_attr.map(CachedPatternAttr::from);
    let key = CacheKey::EdgesByDstWithSrcFilter(
      dst_vid.to_string(),
      e_label.to_string(),
      e_attr_cache,
      src_v_label.to_string(),
      src_v_attr_cache,
    );

    let cache = self.cache.read().await;
    if let Some(result) = { cache.edges_cache.lock() }.get(&key).cloned() {
      return result;
    }
    drop(cache);

    let result = self
      .inner
      .load_e_with_dst_and_src_filter(dst_vid, e_label, e_attr, src_v_label, src_v_attr)
      .await;

    let cache = self.cache.write().await;
    { cache.edges_cache.lock() }.put(key, result.clone());

    result
  }
}
