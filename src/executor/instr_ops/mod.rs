use std::str::FromStr;

use crate::{
  schemas::{Instruction, STR_TUPLE_SPLITTER, VarPrefix},
  utils::dyn_graph::DynGraph,
};

pub trait InstrOperator {
  #[inline]
  fn resolve_var(target_var: &str) -> (VarPrefix, &str) {
    let splitted = target_var.split(STR_TUPLE_SPLITTER).collect::<Vec<_>>();
    let var_type = splitted[0];
    let var_name = splitted[1];
    (VarPrefix::from_str(var_type).unwrap(), var_name)
  }

  fn execute(
    self,
    instr: Instruction,
    result: &mut Vec<Vec<DynGraph>>,
  ) -> impl Future<Output = ()> + Send;
}
