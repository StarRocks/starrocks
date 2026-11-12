#!/usr/bin/env python3
# Generates the files used by FileReaderTest.test_read_variant_shredding_top_level_shapes. Each file has one shredded
# variant column `data`; the expected result of a whole-variant read is given per row.
#
#   variant_shredding_top_level_array.parquet   typed_value is a list of {value, typed_value: string}
#     row 0: ["red","green"]
#     row 1: ["red", variant null, "blue"]            the middle element has value = 0x00
#     row 2: [variant null]                           an element with neither field set reads as a variant null
#     row 3: [1]                                      no typed_value: the fallback `value` holds the array
#     row 4: SQL NULL
#     row 5: []                                       an empty array is not a SQL NULL
#   variant_shredding_nested_array.parquet          a list of lists: [["a","b"],[],["c"]]
#   variant_shredding_array_of_objects.parquet      a list of objects with shredded fields a, b
#   variant_shredding_no_value_column.parquet       no `value` column at all; typed_value is an INT32 (34, 35)
#   variant_shredding_array_no_element_value.parquet  list elements have only typed_value (no `value`): ["red","green"]
# The spec makes `value` optional; the shredded variant corpus of other engines allows a reader either to read such files
# or to reject them. These files pin the behavior of reading them. A group without a `value` column is only recognized
# by the Parquet VARIANT annotation, which pyarrow cannot write; annotate_variant() adds it. Usage: python3 gen_variant_shredding_top_level_shapes.py
import pyarrow as pa
import pyarrow.parquet as pq

# --- VARIANT annotation -------------------------------------------------------------------------------------------
# pyarrow reads but cannot write the Parquet VARIANT logical type. A variant group that has no `value` column is only
# recognized by that annotation, so these files get it by editing the footer: a generic Thrift compact-protocol reader and
# writer (enough for the Parquet footer), and one added field on the SchemaElement of the column.
import struct as _struct

_BOOL_T, _BOOL_F, _BYTE, _I16, _I32, _I64, _DOUBLE, _BINARY, _LIST, _SET, _MAP, _STRUCT = range(1, 13)


class _Reader:
    def __init__(self, data):
        self.d, self.p = data, 0

    def byte(self):
        b = self.d[self.p]
        self.p += 1
        return b

    def varint(self):
        r = s = 0
        while True:
            b = self.byte()
            r |= (b & 0x7F) << s
            if not b & 0x80:
                return r
            s += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def value(self, t):
        if t in (_BOOL_T, _BOOL_F):
            return t == _BOOL_T
        if t == _BYTE:
            return _struct.unpack("b", bytes([self.byte()]))[0]
        if t in (_I16, _I32, _I64):
            return self.zigzag()
        if t == _DOUBLE:
            v = self.d[self.p:self.p + 8]
            self.p += 8
            return v
        if t == _BINARY:
            n = self.varint()
            v = self.d[self.p:self.p + n]
            self.p += n
            return bytes(v)
        if t in (_LIST, _SET):
            h = self.byte()
            n, et = h >> 4, h & 15
            if n == 15:
                n = self.varint()
            return (et, [self.value(et) if et not in (_BOOL_T, _BOOL_F) else bool(self.byte() == 1) for _ in range(n)])
        if t == _STRUCT:
            return self.struct()
        raise ValueError("unsupported thrift type %d" % t)

    def struct(self):
        fields, last = [], 0
        while True:
            h = self.byte()
            if h == 0:
                return fields
            t, delta = h & 15, h >> 4
            fid = last + delta if delta else self.zigzag()
            last = fid
            fields.append((fid, t if t not in (_BOOL_T, _BOOL_F) else _BOOL_T, self.value(t)))


class _Writer:
    def __init__(self):
        self.o = bytearray()

    def varint(self, n):
        while n > 0x7F:
            self.o.append((n & 0x7F) | 0x80)
            n >>= 7
        self.o.append(n)

    def zigzag(self, n):
        self.varint((n << 1) ^ (n >> 63))

    def value(self, t, v):
        if t == _BOOL_T:
            return
        if t == _BYTE:
            self.o += _struct.pack("b", v)
        elif t in (_I16, _I32, _I64):
            self.zigzag(v)
        elif t == _DOUBLE:
            self.o += v
        elif t == _BINARY:
            self.varint(len(v))
            self.o += v
        elif t in (_LIST, _SET):
            et, items = v
            if len(items) < 15:
                self.o.append((len(items) << 4) | et)
            else:
                self.o.append(0xF0 | et)
                self.varint(len(items))
            for it in items:
                if et == _BOOL_T:
                    self.o.append(1 if it else 2)
                else:
                    self.value(et, it)
        elif t == _STRUCT:
            self.struct(v)
        else:
            raise ValueError("unsupported thrift type %d" % t)

    def struct(self, fields):
        last = 0
        for fid, t, v in fields:
            wire = (_BOOL_T if v else _BOOL_F) if t == _BOOL_T else t
            if 0 < fid - last <= 15:
                self.o.append(((fid - last) << 4) | wire)
            else:
                self.o.append(wire)
                self.zigzag(fid)
            last = fid
            self.value(t, v)
        self.o.append(0)


def _footer_bounds(data):
    assert data[-4:] == b"PAR1" and data[:4] == b"PAR1"
    n = _struct.unpack("<I", data[-8:-4])[0]
    return len(data) - 8 - n, len(data) - 8


def rewrite_footer(path, edit):
    data = open(path, "rb").read()
    a, b = _footer_bounds(data)
    footer = _Reader(data[a:b]).struct()
    w = _Writer()
    w.struct(footer)
    assert bytes(w.o) == data[a:b], "the footer does not round-trip; the Thrift codec is incomplete"
    edit(footer)
    w = _Writer()
    w.struct(footer)
    open(path, "wb").write(data[:a] + bytes(w.o) + _struct.pack("<I", len(w.o)) + b"PAR1")


def annotate_variant(path, column_name):
    """Adds the VARIANT logical type (specification version 1) to the schema element named column_name."""
    def edit(footer):
        schema = next(v for fid, _, v in footer if fid == 2)[1]
        for element in schema:
            if any(fid == 4 and v == column_name.encode() for fid, _, v in element):
                variant_type = [(1, _BYTE, 1)]  # VariantType.specification_version
                element.append((10, _STRUCT, [(16, _STRUCT, variant_type)]))  # SchemaElement.logicalType.VARIANT
                element.sort(key=lambda f: f[0])
                return
        raise ValueError("no schema element named " + column_name)
    rewrite_footer(path, edit)


META_EMPTY = b"\x01\x00\x00"  # version 1, empty dictionary
META_AB = b"\x11\x02\x00\x01\x02ab"  # version 1, sorted, names a b


def leaf(typed):
    return pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", typed, nullable=True)])


def variant(typed_value, with_value=True):
    fields = [pa.field("metadata", pa.binary(), nullable=False)]
    if with_value:
        fields.append(pa.field("value", pa.binary(), nullable=True))
    fields.append(pa.field("typed_value", typed_value, nullable=True))
    return pa.struct(fields)


def write(name, variant_type, rows):
    schema = pa.schema([pa.field("data", variant_type, nullable=True)])
    pq.write_table(pa.Table.from_pylist([{"data": r} for r in rows], schema=schema), name, compression="NONE")
    annotate_variant(name, "data")
    print(name, len(rows), "rows")


def element(v):
    return {"value": None, "typed_value": v}


def row(typed, value=None, meta=META_EMPTY):
    return {"metadata": meta, "value": value, "typed_value": typed}


ARRAY_OF_ONE = b"\x03\x01\x00\x02\x0c\x01"  # variant array [int8 1]

write("variant_shredding_top_level_array.parquet", variant(pa.list_(leaf(pa.string()))), [
    row([element("red"), element("green")]),
    row([element("red"), {"value": b"\x00", "typed_value": None}, element("blue")]),
    row([element(None)]),
    row(None, value=ARRAY_OF_ONE),
    None,
    row([]),
])
write("variant_shredding_nested_array.parquet", variant(pa.list_(leaf(pa.list_(leaf(pa.string()))))), [
    row([element([element("a"), element("b")]), element([]), element([element("c")])]),
])
write("variant_shredding_array_of_objects.parquet",
      variant(pa.list_(leaf(pa.struct([pa.field("a", leaf(pa.int32()), nullable=True),
                                       pa.field("b", leaf(pa.string()), nullable=True)])))), [
    row([element({"a": element(1), "b": element("x")}), element({"a": element(2), "b": element("y")})], meta=META_AB),
])
write("variant_shredding_no_value_column.parquet", variant(pa.int32(), with_value=False), [
    {"metadata": META_EMPTY, "typed_value": 34},
    {"metadata": META_EMPTY, "typed_value": 35},
])
write("variant_shredding_array_no_element_value.parquet",
      variant(pa.list_(pa.struct([pa.field("typed_value", pa.string(), nullable=True)]))), [
    row([{"typed_value": "red"}, {"typed_value": "green"}]),
])
