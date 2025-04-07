use crate::{
  schemas::{Eid, Label, PatternAttr, PatternEdge, PatternVertex, Vid},
  utils::dyn_graph::DynGraph,
};
use hashbrown::HashMap;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct PatternParser {
  src: String,
  line: usize,

  v_labels: HashMap<Vid, Label>,
  v_attrs: HashMap<Vid, PatternAttr>,

  e_2_vv: HashMap<Eid, (Vid, Vid)>,
  e_labels: HashMap<Eid, Label>,
  e_attrs: HashMap<Eid, PatternAttr>,
}

impl PatternParser {
  pub fn new(src: String) -> Self {
    Self {
      src,
      line: 1,
      v_labels: HashMap::new(),
      v_attrs: HashMap::new(),
      e_2_vv: HashMap::new(),
      e_labels: HashMap::new(),
      e_attrs: HashMap::new(),
    }
  }

  pub fn take_as_pattern_graph(mut self) -> DynGraph<PatternVertex, PatternEdge> {
    let mut pattern_graph = DynGraph::default();

    // vertices
    for (vid, label) in self.v_labels.drain() {
      let attr = self.v_attrs.remove(&vid);
      let pattern_vertex = PatternVertex {
        vid: vid.clone(),
        label,
        attr,
      };
      pattern_graph.update_v(pattern_vertex, vid);
    }

    // edges
    for (eid, (src_vid, dst_vid)) in self.e_2_vv.drain() {
      let label = self.e_labels.remove(&eid).unwrap();
      let attr = self.e_attrs.remove(&eid);
      let pattern_edge = PatternEdge {
        eid: eid.clone(),
        src_vid,
        dst_vid,
        label,
        attr,
      };
      pattern_graph.update_e(pattern_edge, eid);
    }

    pattern_graph
  }

  pub fn parse(&mut self) {
    let mut lines = self.src.lines().filter(|line| !line.trim().is_empty());
    let cnt_args = lines
      .next()
      .expect("❌  Missing `count_args` line => Count(v, e, v_attr, e_attr).");

    // Count(v, e, v_attr, e_attr)
    let mut cnt_args = cnt_args.split_whitespace();
    let v_cnt = cnt_args
      .next()
      .expect("❌  Missing 'vertex' count.")
      .parse::<usize>()
      .expect("❌  Invalid 'vertex' count.");
    let e_cnt = cnt_args
      .next()
      .expect("❌  Missing 'edge' count.")
      .parse::<usize>()
      .expect("❌  Invalid 'edge' count.");
    let v_attr_cnt = cnt_args
      .next()
      .expect("❌  Missing 'vertex attribute' count.")
      .parse::<usize>()
      .expect("❌  Invalid 'vertex attribute' count.");
    let e_attr_cnt = cnt_args
      .next()
      .expect("❌  Missing 'edge attribute' count.")
      .parse::<usize>()
      .expect("❌  Invalid 'edge attribute' count.");
    self.line += 1;

    // vertices
    for _ in 0..v_cnt {
      let line = lines.next().expect("❌  Missing 'vertex' line.");
      let mut args = line.split_whitespace();

      let vid = args.next().expect("❌  Missing 'vid'.").to_string();
      let label = args.next().expect("❌  Missing 'v_label'.").to_string();

      self.v_labels.insert(vid, label);
    }
    self.line += v_cnt;

    // edges
    for _ in 0..e_cnt {
      let line = lines.next().expect("❌  Missing 'edge' line.");
      let mut args = line.split_whitespace();

      let eid = args.next().expect("❌  Missing 'eid'.").to_string();
      let src_vid = args.next().expect("❌  Missing 'src_vid'.").to_string();
      let dst_vid = args.next().expect("❌  Missing 'dst_vid'.").to_string();
      let label = args.next().expect("❌  Missing 'e_label'.").to_string();

      self.e_2_vv.insert(eid.clone(), (src_vid, dst_vid));
      self.e_labels.insert(eid, label);
    }
    self.line += e_cnt;

    // vertex attributes
    for _ in 0..v_attr_cnt {
      let line = lines.next().expect("❌  Missing 'vertex attribute' line.");
      let mut args = line.split_whitespace();

      let vid = args.next().expect("❌  Missing 'vid'.").to_string();
      let key = args
        .next()
        .expect("❌  Missing 'key' for `v_attr`.")
        .to_string();
      let raw_pred = args.collect_vec().join("");
      if raw_pred.is_empty() {
        panic!("❌  Missing 'predicate' for `v_attr`.");
      }

      let pattern_attr = PatternAttr::parse_from_raw(key, raw_pred);
      self.v_attrs.insert(vid.clone(), pattern_attr);
    }
    self.line += v_attr_cnt;

    // edge attributes
    for _ in 0..e_attr_cnt {
      let line = lines.next().expect("❌  Missing 'edge attribute' line.");
      let mut args = line.split_whitespace();

      let eid = args.next().expect("❌  Missing 'eid'.").to_string();
      let key = args
        .next()
        .expect("❌  Missing 'key' for `e_attr`.")
        .to_string();
      let raw_pred = args.collect_vec().join("");
      if raw_pred.is_empty() {
        panic!("❌  Missing 'predicate' for `e_attr`.");
      }

      let pattern_attr = PatternAttr::parse_from_raw(key, raw_pred);
      self.e_attrs.insert(eid.clone(), pattern_attr);
    }
    self.line += e_attr_cnt;
  }
}
