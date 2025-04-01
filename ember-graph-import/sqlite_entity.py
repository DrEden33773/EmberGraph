from typing import Optional

from sqlmodel import Field, Session, SQLModel, create_engine
from sqlmodel.ext.asyncio.session import AsyncSession

type Attr = int | float | str
type AttrDict = dict[str, Attr]


class BaseAttribute(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    key: str = Field(index=True)
    value: str
    type: str

    __table_args__ = {"sqlite_autoincrement": True}

    @property
    def typed_value(self) -> Attr:
        if self.type == "int":
            return int(self.value)
        elif self.type == "float":
            return float(self.value)
        else:
            return self.value


class DB_Vertex(SQLModel, table=True):
    vid: str = Field(primary_key=True)
    label: str = Field(index=True)

    def __init__(self, vid: str, label: str, attrs: AttrDict = {}) -> None:
        super().__init__(vid=vid, label=label)
        self._pending_attrs = attrs

    def load_pending_attrs(self, session: AsyncSession | Session):
        for key, value in self._pending_attrs.items():
            attr = Vertex_Attribute(
                vid=self.vid, key=key, value=str(value), type=type(value).__name__
            )
            session.add(attr)

        self._pending_attrs.clear()


class DB_Edge(SQLModel, table=True):
    eid: str = Field(primary_key=True)
    label: str = Field(index=True)
    src_vid: str = Field(index=True)
    dst_vid: str = Field(index=True)

    def __init__(
        self,
        eid: str,
        src_vid: str,
        dst_vid: str,
        label: str,
        attrs: AttrDict = {},
    ) -> None:
        super().__init__(eid=eid, src_vid=src_vid, dst_vid=dst_vid, label=label)
        self._pending_attrs = attrs

    def load_pending_attrs(self, session: AsyncSession | Session):
        for key, value in self._pending_attrs.items():
            attr = Edge_Attribute(
                eid=self.eid, key=key, value=str(value), type=type(value).__name__
            )
            session.add(attr)

        self._pending_attrs.clear()


class Vertex_Attribute(BaseAttribute, table=True):
    vid: str = Field(index=True)

    def __hash__(self) -> int:
        vid_hashed = hash(self.vid)
        return vid_hashed ^ hash(self.id) if self.id else vid_hashed


class Edge_Attribute(BaseAttribute, table=True):
    eid: str = Field(index=True)

    def __hash__(self) -> int:
        eid_hashed = hash(self.eid)
        return eid_hashed ^ hash(self.id) if self.id else eid_hashed


def init_db(db_url: str, echo: bool = False):
    engine = create_engine(db_url, echo=echo)
    SQLModel.metadata.create_all(engine)
    return engine


def init_db_with_clear(db_url: str, echo: bool = False):
    engine = create_engine(db_url, echo=echo)
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    return engine
