"""
This is to avoid wal2json re-rendering every value as JSON, which is lossy at the edges:
json/jsonb column text is parsed and re-serialized (key order, whitespace, duplicate keys),
numerics ride through JSON numbers, NaN/Infinity are unrepresentable. pgoutput is prefered here
because it carries each value as the source type's own text output, and the sink parses it back
via a cast (%s::text::<type>). Using Postgres's canonical codecs end to end means there is no
intermediate format to leak fidelity on any type, including extensions.
Unlike wal2json, pgoutput is built into postgres and requires no additional extensions.

This is currently hand rolled in Python since the pgoutput format is stable and documented -
I could not find an existing maintained decoder in Python. Since this part is the hot loop of
the stream, it may be preferable to use a native format (PyO3?) for higher througput production
use cases.
"""

import struct
from dataclasses import dataclass

from psycopg import Connection

_U16 = struct.Struct(">H")
_U32 = struct.Struct(">I")


@dataclass
class Column:
    name: str
    ty: str  # sink-side cast target, resolved from the Relation message's type oid
    value: str | None  # the type's text output; None is SQL NULL


@dataclass
class Change:
    table: str
    op: str  # INSERT | UPDATE | DELETE
    cols: list[Column]

    def get(self, name: str) -> str | None:
        for c in self.cols:
            if c.name == name:
                return c.value
        return None

    def get_int(self, name: str) -> int | None:
        value = self.get(name)
        return int(value) if value is not None else None


class Commit:
    """End of a source transaction: the changes since the last Commit apply atomically."""


@dataclass
class _Relation:
    table: str
    columns: list[tuple[str, str]]  # (name, type name)


class Decoder:
    """Relation ('R') messages announce a table's columns and type oids before its first change in
    each decoding session (every poll starts one); Type ('Y') messages announce custom types,
    covering any created after the upfront pg_type load."""

    def __init__(self, source: Connection) -> None:
        # One upfront pg_type load covers every type in the source db, extensions included.
        rows = source.execute("SELECT oid, format_type(oid, NULL) FROM pg_type").fetchall()
        self.types: dict[int, str] = dict(rows)
        self.relations: dict[int, _Relation] = {}

    def decode(self, data: bytes) -> Change | Commit | None:
        """One message -> a row change or a Commit marker; None for the rest
        (begin/relation/origin/type)."""
        b = _Buf(data)
        kind = b.u8()
        if kind == ord("R"):
            rel_id = b.u32()
            b.cstr()  # namespace
            table = b.cstr()
            b.u8()  # replica identity
            columns = []
            for _ in range(b.u16()):
                b.u8()  # flags
                name = b.cstr()
                oid = b.u32()
                b.u32()  # typmod
                columns.append((name, self.types.get(oid, "text")))
            self.relations[rel_id] = _Relation(table, columns)
            return None
        if kind == ord("Y"):
            oid = b.u32()
            namespace = b.cstr()
            type_name = b.cstr()
            self.types[oid] = f'"{namespace}"."{type_name}"'
            return None
        if kind == ord("I"):
            rel = self._relation(b.u32())
            b.u8()  # 'N'
            return Change(rel.table, "INSERT", self._read_tuple(rel, b))
        if kind == ord("U"):
            rel = self._relation(b.u32())
            if b.peek() in (ord("K"), ord("O")):
                b.u8()
                self._read_tuple(rel, b)  # old key/row: consumed, unused
            b.u8()  # 'N'
            return Change(rel.table, "UPDATE", self._read_tuple(rel, b))
        if kind == ord("D"):
            rel = self._relation(b.u32())
            b.u8()  # 'K' (key only, default replica identity) or 'O' (full old row)
            return Change(rel.table, "DELETE", self._read_tuple(rel, b))
        if kind == ord("C"):
            return Commit()
        if kind in b"BOTM":  # begin/origin/truncate/message
            return None
        raise ValueError(f"unknown pgoutput message kind {chr(kind)!r}")

    def _relation(self, rel_id: int) -> _Relation:
        if (rel := self.relations.get(rel_id)) is None:
            raise ValueError(f"change for unannounced relation {rel_id}")
        return rel

    def _read_tuple(self, rel: _Relation, b: "_Buf") -> list[Column]:
        """TupleData: per column 'n' (null), 'u' (unchanged TOAST -- omitted, so the upsert leaves
        the sink's copy of it alone) or 't' (the type's text output, length-prefixed)."""
        cols = []
        for i in range(b.u16()):
            name, ty = rel.columns[i]
            k = b.u8()
            if k == ord("n"):
                cols.append(Column(name, ty, None))
            elif k == ord("u"):
                continue
            elif k == ord("t"):
                cols.append(Column(name, ty, b.bytes(b.u32()).decode()))
            else:
                raise ValueError(f"unsupported tuple data kind {chr(k)!r}")
        return cols


class _Buf:
    """Big-endian reader over one message."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def bytes(self, n: int) -> bytes:
        head = self.data[self.pos : self.pos + n]
        if len(head) < n:
            raise ValueError("truncated pgoutput message")
        self.pos += n
        return head

    def u8(self) -> int:
        return self.bytes(1)[0]

    def peek(self) -> int:
        if self.pos >= len(self.data):
            raise ValueError("truncated pgoutput message")
        return self.data[self.pos]

    def u16(self) -> int:
        return _U16.unpack(self.bytes(2))[0]

    def u32(self) -> int:
        return _U32.unpack(self.bytes(4))[0]

    def cstr(self) -> str:
        end = self.data.index(0, self.pos)
        s = self.data[self.pos : end].decode()
        self.pos = end + 1
        return s
