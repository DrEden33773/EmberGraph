use crate::{
  matching_ctx::MatchingCtx,
  schemas::{Instruction, InstructionType::*, STR_TUPLE_SPLITTER, VarPrefix},
  storage::StorageAdapter,
};
use foreach::ForeachOperator;
use get_adj::GetAdjOperator;
use init::InitOperator;
use intersect::IntersectOperator;
use report::ReportOperator;
use std::{str::FromStr, sync::Arc};
use tokio::sync::Mutex;

pub mod foreach;
pub mod get_adj;
pub mod init;
pub mod intersect;
pub mod report;

#[inline]
pub(crate) fn resolve_var(target_var: &str) -> (VarPrefix, &str) {
  let mut splitted = target_var.split(STR_TUPLE_SPLITTER);
  let var_type = splitted.next().unwrap();
  let var_name = splitted.next().unwrap();
  (VarPrefix::from_str(var_type).unwrap(), var_name)
}

pub enum InstrOperator<S: StorageAdapter> {
  Init(InitOperator<S>),
  GetAdj(GetAdjOperator<S>),
  Foreach(ForeachOperator),
  Intersect(IntersectOperator<S>),
  Report(ReportOperator),
}

pub struct InstrOperatorFactory;

impl InstrOperatorFactory {
  pub fn create<S: StorageAdapter>(
    instr: Instruction,
    storage_adapter: Arc<Mutex<S>>,
    ctx: Arc<Mutex<MatchingCtx>>,
  ) -> InstrOperator<S> {
    match instr.type_ {
      Init => InstrOperator::Init(InitOperator {
        storage_adapter: storage_adapter.clone(),
        ctx: ctx.clone(),
      }),
      GetAdj => InstrOperator::GetAdj(GetAdjOperator {
        storage_adapter: storage_adapter.clone(),
        ctx: ctx.clone(),
      }),
      Foreach => InstrOperator::Foreach(ForeachOperator { ctx: ctx.clone() }),
      Intersect => InstrOperator::Intersect(IntersectOperator {
        storage_adapter: storage_adapter.clone(),
        ctx: ctx.clone(),
      }),
      Report => InstrOperator::Report(ReportOperator { ctx: ctx.clone() }),
      TCache => unimplemented!("`TCache` operator is not implemented yet."),
    }
  }
}
