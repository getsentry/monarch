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
from datetime import datetime, timedelta, timezone
from typing import Literal

from psycopg import Connection

Op = Literal["INSERT", "UPDATE", "DELETE"]

_U16 = struct.Struct(">H")
_U32 = struct.Struct(">I")
_U64 = struct.Struct(">Q")
_PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


@dataclass
class Column:
    name: str
    type_name: str  # sink-side cast target, resolved from the Relation message's type oid
    value: str | None  # the type's text output; None is SQL NULL


@dataclass
class Change:
    table: str
    op: Op
    cols: list[Column]
    partial: bool = False  # an unchanged TOAST column was omitted: cols is not the full row

    def get(self, name: str) -> str | None:
        for c in self.cols:
            if c.name == name:
                return c.value
        return None

    def get_int(self, name: str) -> int | None:
        value = self.get(name)
        return int(value) if value is not None else None


@dataclass
class Commit:
    """End of a source transaction: the changes since the last Commit apply atomically."""

    ts: datetime  # source commit time


@dataclass
class Truncate:
    """A table-level operation. The mover can detect it, but cannot org-scope it."""

    tables: list[str]
    cascade: bool
    restart_identity: bool


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

    def decode(self, data: bytes) -> Change | Commit | Truncate | None:
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
            cols, _ = self._read_tuple(rel, b)  # inserts always carry the full row
            return Change(rel.table, "INSERT", cols)
        if kind == ord("U"):
            rel = self._relation(b.u32())
            if b.peek() in (ord("K"), ord("O")):
                b.u8()
                self._read_tuple(rel, b)  # old key/row: consumed, unused
            b.u8()  # 'N'
            cols, partial = self._read_tuple(rel, b)
            return Change(rel.table, "UPDATE", cols, partial)
        if kind == ord("D"):
            rel = self._relation(b.u32())
            b.u8()  # 'K' (key only, default replica identity) or 'O' (full old row)
            cols, _ = self._read_tuple(rel, b)  # key tuples carry no TOAST to omit
            return Change(rel.table, "DELETE", cols)
        if kind == ord("T"):
            count = b.u32()
            options = b.u8()
            tables = [self._relation(b.u32()).table for _ in range(count)]
            return Truncate(
                tables,
                cascade=bool(options & 0x01),
                restart_identity=bool(options & 0x02),
            )
        if kind == ord("C"):
            b.u8()  # flags
            b.u64()  # commit lsn
            b.u64()  # end lsn
            return Commit(_PG_EPOCH + timedelta(microseconds=b.u64()))
        if kind in b"BOM":  # begin/origin/message
            return None
        raise ValueError(f"unknown pgoutput message kind {chr(kind)!r}")

    def _relation(self, rel_id: int) -> _Relation:
        if (rel := self.relations.get(rel_id)) is None:
            raise ValueError(f"change for unannounced relation {rel_id}")
        return rel

    def _read_tuple(self, rel: _Relation, b: "_Buf") -> tuple[list[Column], bool]:
        """TupleData: per column 'n' (null), 'u' (unchanged TOAST -- omitted, marking the change
        partial: apply must update the sink's row, never fabricate one) or 't' (the type's text
        output, length-prefixed)."""
        cols = []
        partial = False
        for i in range(b.u16()):
            name, type_name = rel.columns[i]
            k = b.u8()
            if k == ord("n"):
                cols.append(Column(name, type_name, None))
            elif k == ord("u"):
                partial = True
            elif k == ord("t"):
                cols.append(Column(name, type_name, b.bytes(b.u32()).decode()))
            else:
                raise ValueError(f"unsupported tuple data kind {chr(k)!r}")
        return cols, partial


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

    def u64(self) -> int:
        return _U64.unpack(self.bytes(8))[0]

    def cstr(self) -> str:
        end = self.data.index(0, self.pos)
        s = self.data[self.pos : end].decode()
        self.pos = end + 1
        return s
