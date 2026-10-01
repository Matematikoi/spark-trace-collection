"""Read a mysqldump of a Hive metastore database into {table: [row dicts]} without MySQL."""
import re
from pathlib import Path

_CREATE = re.compile(r"CREATE TABLE `(\w+)` \((.*?)\n\)[^;]*;", re.S)
_COLUMN = re.compile(r"^\s*`(\w+)`", re.M)
_INSERT = re.compile(r"^INSERT INTO `(\w+)` VALUES ", re.M)
_ESCAPES = {"0": "\0", "b": "\b", "n": "\n", "r": "\r", "t": "\t", "Z": "\x1a"}


def _parse_values(text: str, pos: int):
    """Parse `(v,...),(v,...);` starting at pos. Yields tuples; returns via StopIteration."""
    rows, n = [], len(text)
    while pos < n:
        assert text[pos] == "(", text[pos:pos + 50]
        pos += 1
        row = []
        while True:
            c = text[pos]
            if c == "'":
                pos += 1
                buf = []
                while True:
                    c = text[pos]
                    if c == "\\":
                        nxt = text[pos + 1]
                        buf.append(_ESCAPES.get(nxt, nxt))
                        pos += 2
                    elif c == "'":
                        if text[pos + 1] == "'":
                            buf.append("'")
                            pos += 2
                        else:
                            pos += 1
                            break
                    else:
                        buf.append(c)
                        pos += 1
                row.append("".join(buf))
            else:
                end = pos
                while text[end] not in ",)":
                    end += 1
                tok = text[pos:end]
                if tok == "NULL":
                    row.append(None)
                elif tok.startswith("_binary") or tok.startswith("0x"):
                    row.append(tok)
                else:
                    row.append(int(tok) if re.fullmatch(r"-?\d+", tok) else tok)
                pos = end
            if text[pos] == ",":
                pos += 1
                continue
            assert text[pos] == ")"
            pos += 1
            break
        rows.append(row)
        if text[pos] == ",":
            pos += 1
        elif text[pos] == ";":
            return rows, pos + 1
    return rows, pos


def load(path: str, tables: set[str] | None = None) -> dict[str, list[dict]]:
    text = Path(path).read_text(encoding="utf-8", errors="surrogateescape")
    columns = {m.group(1): _COLUMN.findall(m.group(2)) for m in _CREATE.finditer(text)}
    data: dict[str, list[dict]] = {}
    for m in _INSERT.finditer(text):
        name = m.group(1)
        if tables is not None and name not in tables:
            continue
        rows, _ = _parse_values(text, m.end())
        cols = columns[name]
        data.setdefault(name, []).extend(dict(zip(cols, r)) for r in rows)
    return data
