from dataclasses import dataclass
from enum import StrEnum
from operator import eq, ge, gt, le, lt, ne


class AttrType(StrEnum):
    Int = "int"
    Float = "float"
    String = "string"


class Op(StrEnum):
    Eq = "="
    Ne = "!="
    Gt = ">"
    Ge = ">="
    Lt = "<"
    Le = "<="


def str_op_to_operator(op: Op):
    match op:
        case Op.Eq:
            return eq
        case Op.Ne:
            return ne
        case Op.Gt:
            return gt
        case Op.Ge:
            return ge
        case Op.Lt:
            return lt
        case Op.Le:
            return le


@dataclass
class PatternAttr:
    key: str
    op: Op
    value: int | float | str
    type: AttrType

    def __hash__(self) -> int:
        return hash(self.key)
