#!/usr/bin/env python3
"""qc.segments_from_rows 纯函数单测（无需数据库）。

运行：python3 scripts/qc_unit.py
"""
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0] + "/app")
from qc import segments_from_rows  # noqa: E402

T0 = datetime(2026, 9, 1, 0, tzinfo=timezone.utc)


def h(n, minute=0):
    return T0 + timedelta(hours=n, minutes=minute)


def rows(*pairs):
    return [(h(n, m), v) for n, m, v in pairs]


cases = []


def case(name, data, threshold, want):
    cases.append((name, data, threshold, want))


case("相邻整点等值达标", rows((0, 0, 5), (1, 0, 5), (2, 0, 5)), 3,
     [(h(0), h(2), 5, 3)])
case("不足阈值不出段", rows((0, 0, 5), (1, 0, 5)), 3, [])
case("测值变化断段", rows((0, 0, 5), (1, 0, 5), (2, 0, 6), (3, 0, 6), (4, 0, 6)), 3,
     [(h(2), h(4), 6, 3)])
case("缺小时断段", rows((0, 0, 5), (1, 0, 5), (3, 0, 5), (4, 0, 5), (5, 0, 5)), 3,
     [(h(3), h(5), 5, 3)])
case("非整点忽略", rows((0, 0, 5), (1, 30, 5), (2, 0, 5), (3, 0, 5)), 3,
     [])
case("两段均达标", rows(*[(i, 0, 7) for i in range(4)],
                        *[(10 + i, 0, 8) for i in range(4)]), 4,
     [(h(0), h(3), 7, 4), (h(10), h(13), 8, 4)])
case("阈值12", rows(*[(i, 0, 1) for i in range(12)]), 12,
     [(h(0), h(11), 1, 12)])
case("空输入", [], 3, [])

bad = 0
for name, data, threshold, want in cases:
    got = [(s["start_ts"], s["end_ts"], s["value"], s["length"])
           for s in segments_from_rows(data, threshold)]
    ok = got == want
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}"
          + ("" if ok else f"\n      got={got}\n     want={want}"))
    bad += 0 if ok else 1

if bad:
    print(f"\n{bad} 个用例失败")
    sys.exit(1)
print("\n全部单测通过 ✓")
