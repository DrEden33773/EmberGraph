use super::{VidRef, base::Vid};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, strum_macros::Display)]
#[repr(u8)]
pub enum InstructionType {
  #[serde(rename = "init")]
  #[strum(serialize = "Init")]
  Init = 0,
  #[serde(rename = "get_adj")]
  #[strum(serialize = "GetAdj")]
  GetAdj = 1,
  #[serde(rename = "intersect")]
  #[strum(serialize = "Intersect")]
  Intersect = 2,
  #[serde(rename = "foreach")]
  #[strum(serialize = "Foreach")]
  Foreach = 3,
  #[serde(rename = "t_cache")]
  #[strum(serialize = "TCache")]
  TCache = 4,
  #[serde(rename = "report")]
  #[strum(serialize = "Report")]
  Report = 5,
}

impl InstructionType {
  pub fn compare(self, other: Self) -> i8 {
    self as u8 as i8 - other as u8 as i8
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
  pub(crate) vid: Vid,
  #[serde(rename = "type")]
  pub(crate) type_: InstructionType,
  #[serde(rename = "expand_eid_list")]
  pub(crate) expand_eids: Vec<String>,
  pub(crate) single_op: Option<String>,
  pub(crate) multi_ops: Vec<String>,
  pub(crate) target_var: String,
  pub(crate) depend_on: Vec<String>,
}

impl Instruction {
  pub fn to_string_uncolored(&self) -> String {
    match self.type_ {
      InstructionType::Init => format!("{} ({}) -> {}", self.type_, self.vid, self.target_var),
      InstructionType::GetAdj => format!(
        "{} ({})~~{:?} -> {}",
        self.type_,
        self.single_op.as_ref().unwrap_or(&"".to_string()),
        self.expand_eids,
        self.target_var
      ),
      InstructionType::Intersect => match self.single_op {
        Some(ref single_op) => format!(
          "{} ({}, {}) -> {}",
          self.type_, self.vid, single_op, self.target_var,
        ),
        None => format!(
          "{} ({}, {:?}) -> {}",
          self.type_, self.vid, self.multi_ops, self.target_var
        ),
      },
      InstructionType::Foreach => format!(
        "{} ({}) -> {}",
        self.type_,
        self.single_op.as_ref().unwrap_or(&"".to_string()),
        self.target_var
      ),
      InstructionType::Report => format!("{} {:?}", self.type_, self.multi_ops),
      InstructionType::TCache => unimplemented!(),
    }
  }
}

impl Display for Instruction {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self.type_ {
      InstructionType::Init => write!(
        f,
        "{} {}{}{} -> {}",
        self.type_.to_string().purple().bold(),
        "(".purple(),
        self.vid.to_string().yellow(),
        ")".purple(),
        self.target_var.green()
      ),
      InstructionType::GetAdj => write!(
        f,
        "{} {}{}{}~~{:?} -> {}",
        self.type_.to_string().purple().bold(),
        "(".purple(),
        self.single_op.as_ref().unwrap_or(&"".to_string()).yellow(),
        ")".purple(),
        self.expand_eids,
        self.target_var.green()
      ),
      InstructionType::Intersect => match self.single_op {
        Some(ref single_op) => write!(
          f,
          "{} {}{}{}{}{} -> {}",
          self.type_.to_string().purple().bold(),
          "(".purple(),
          self.vid.yellow(),
          ", ".purple(),
          single_op.cyan(),
          ")".purple(),
          self.target_var.green()
        ),
        None => write!(
          f,
          "{} {}{}{}{}{} -> {}",
          self.type_.to_string().purple().bold(),
          "(".purple(),
          self.vid.yellow(),
          ", ".purple(),
          format!("{:?}", self.multi_ops).cyan(),
          ")".purple(),
          self.target_var.green()
        ),
      },
      InstructionType::Foreach => write!(
        f,
        "{} {}{}{} -> {}",
        self.type_.to_string().purple().bold(),
        "(".purple(),
        self.single_op.as_ref().unwrap_or(&"".to_string()).yellow(),
        ")".purple(),
        self.target_var.green()
      ),
      InstructionType::Report => write!(
        f,
        "{} {}",
        self.type_.to_string().purple().bold(),
        format!("{:?}", self.multi_ops).yellow()
      ),
      InstructionType::TCache => unimplemented!(),
    }
  }
}

impl Instruction {
  pub fn is_single_op(&self) -> bool {
    self.single_op.is_some()
  }
}

pub struct InstructionBuilder {
  pub(crate) vid: Vid,
  pub(crate) type_: InstructionType,
  pub(crate) target_var: String,
  pub(crate) expand_eids: Vec<String>,
  pub(crate) single_op: Option<String>,
  pub(crate) multi_ops: Vec<String>,
  pub(crate) depend_on: Vec<String>,
}

impl InstructionBuilder {
  pub fn new(vid: VidRef, type_: InstructionType) -> Self {
    Self {
      vid: vid.to_string(),
      type_,
      target_var: String::new(),
      expand_eids: vec![],
      single_op: None,
      multi_ops: vec![],
      depend_on: vec![],
    }
  }

  pub fn expand_eids(mut self, expand_eids: impl IntoIterator<Item = String>) -> Self {
    self.expand_eids = expand_eids.into_iter().collect();
    self
  }

  pub fn single_op(mut self, single_op: String) -> Self {
    self.single_op = Some(single_op);
    self
  }

  pub fn multi_ops(mut self, multi_ops: impl IntoIterator<Item = String>) -> Self {
    self.multi_ops = multi_ops.into_iter().collect();
    self
  }

  pub fn target_var(mut self, target_var: String) -> Self {
    self.target_var = target_var;
    self
  }

  pub fn depend_on(mut self, depend_on: impl IntoIterator<Item = String>) -> Self {
    self.depend_on = depend_on.into_iter().collect();
    self
  }

  pub fn build(self) -> Instruction {
    Instruction {
      vid: self.vid,
      type_: self.type_,
      expand_eids: self.expand_eids,
      single_op: self.single_op,
      multi_ops: self.multi_ops,
      target_var: self.target_var,
      depend_on: self.depend_on,
    }
  }
}
