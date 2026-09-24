import unittest

from trendalert.candles import MIN, Bar, SymbolSeries
from trendalert.config import SignalConfig
from trendalert.signals import Detector, evaluate, streak

T0 = 1_790_000_100_000 // (15 * MIN) * (15 * MIN)  # 对齐到 15m 整点


def bar(i: int, o: float, c: float, v: float = 10, h: float | None = None, l: float | None = None) -> Bar:
    return Bar(T0 + i * MIN, o, h if h is not None else max(o, c), l if l is not None else min(o, c), c, v, "rest", True)


def feed(s: SymbolSeries, closes: list[float], vols: list[float] | None = None) -> list:
    out = []
    prev = closes[0]
    start = 0 if s.finalized_ts is None else (s.finalized_ts - T0) // MIN + 1
    if s.finalized_ts is None:
        s.finalized_ts = T0 - MIN
    for k, c in enumerate(closes):
        out.append(s.finalize(bar(start + k, prev, c, vols[k] if vols else 10)))
        prev = c
    return out


class TestAggregate(unittest.TestCase):
    def test_3m_5m_15m_alignment_and_values(self):
        s = SymbolSeries("X", [1, 3, 5, 15])
        closes = [100 + i for i in range(15)]
        closed = feed(s, closes, vols=[1] * 15)
        self.assertEqual([tf for tf, _ in closed[2]], [1, 3])
        self.assertEqual([tf for tf, _ in closed[4]], [1, 5])
        self.assertEqual([tf for tf, _ in closed[14]], [1, 3, 5, 15])
        b15 = s.bars[15][-1]
        self.assertEqual((b15.ts, b15.o, b15.c, b15.v), (T0, 100, 114, 15))
        self.assertEqual(len(s.bars[3]), 5)
        self.assertEqual(len(s.bars[5]), 3)

    def test_incomplete_first_bucket_dropped(self):
        s = SymbolSeries("X", [1, 3])
        s.finalized_ts = T0  # 从第 2 分钟开始，第一个 3m 桶不完整
        feed(s, [1, 2, 3, 4, 5])
        self.assertEqual([b.ts for b in s.bars[3]], [T0 + 3 * MIN])

    def test_correct_recomputes_aggregate(self):
        s = SymbolSeries("X", [1, 3])
        feed(s, [1, 2, 3])
        fixed = Bar(T0 + 2 * MIN, 2, 9, 2, 8, 10, "rest", True)
        self.assertTrue(s.correct(fixed))
        self.assertEqual((s.bars[3][-1].c, s.bars[3][-1].h), (8, 9))
        self.assertFalse(s.correct(fixed))

    def test_ws_does_not_override_finalized_or_rest(self):
        s = SymbolSeries("X", [1])
        feed(s, [1, 2])
        s.on_ws(Bar(T0 + MIN, 0, 0, 0, 0, 0, "ws"))
        self.assertNotIn(T0 + MIN, s.raw)
        s.put_trusted(Bar(T0 + 2 * MIN, 1, 1, 1, 1, 1, "rest", True))
        s.on_ws(Bar(T0 + 2 * MIN, 5, 5, 5, 5, 5, "ws"))
        self.assertEqual(s.raw[T0 + 2 * MIN].src, "rest")

    def test_down_interval_overlap(self):
        s = SymbolSeries("X", [1])
        s.mark_down(T0 + 30_000)
        self.assertFalse(s.ws_trusted_for(T0))
        s.mark_up(T0 + 90_000)
        self.assertFalse(s.ws_trusted_for(T0 + MIN))
        self.assertTrue(s.ws_trusted_for(T0 + 2 * MIN))
        self.assertTrue(s.ws_trusted_for(T0 - MIN))


class TestSignals(unittest.TestCase):
    def cfg(self, **kw):
        c = SignalConfig(min_move_pct={1: 0, 3: 0, 5: 0, 15: 0})
        c.volume.enabled = False
        c.body.enabled = False
        for k, v in kw.items():
            setattr(c, k, v)
        return c

    def test_streak(self):
        mk = lambda cs: [bar(i, cs[i], cs[i]) for i in range(len(cs))]
        self.assertEqual(streak(mk([1, 2, 3, 4, 5])), (1, 4))
        self.assertEqual(streak(mk([5, 4, 3, 2, 1])), (-1, 4))
        self.assertEqual(streak(mk([1, 2, 2, 3])), (1, 1))
        self.assertEqual(streak(mk([3, 3])), (0, 0))

    def test_four_comparisons_need_five_bars(self):
        s = SymbolSeries("X", [1])
        feed(s, [1, 2, 3, 4])
        self.assertIsNone(evaluate(s.bars[1], 1, self.cfg()))
        feed(s, [5])
        ev = evaluate(s.bars[1], 1, self.cfg())
        self.assertEqual((ev.direction, ev.length, ev.passed), (1, 4, True))

    def test_min_move_volume_body_filters(self):
        cfg = self.cfg(min_move_pct={1: 5})
        s = SymbolSeries("X", [1])
        feed(s, [100, 101, 102, 103, 104])
        self.assertFalse(evaluate(s.bars[1], 1, cfg).passed)

        cfg = self.cfg()
        cfg.volume.enabled = True
        cfg.volume.lookback = 3
        s = SymbolSeries("X", [1])
        feed(s, [9, 9, 9, 1, 2, 3, 4, 5], vols=[10, 10, 10, 10, 10, 10, 10, 10])
        self.assertFalse(evaluate(s.bars[1], 1, cfg).passed)
        s = SymbolSeries("X", [1])
        feed(s, [9, 9, 9, 1, 2, 3, 4, 5], vols=[10, 10, 10, 10, 30, 30, 30, 30])
        ev = evaluate(s.bars[1], 1, cfg)
        self.assertTrue(ev.passed)
        self.assertAlmostEqual(ev.vol_ratio, 3.0)

        cfg = self.cfg()
        cfg.body.enabled = True
        s = SymbolSeries("X", [1])
        s.finalized_ts = T0 - MIN
        # 收盘价在涨，但最后一根是阴线（开盘高于收盘）
        for i, (o, c) in enumerate([(1, 1), (1, 2), (2, 3), (3, 4), (6, 5)]):
            s.finalize(bar(i, o, c))
        self.assertIn("实体不达标", evaluate(s.bars[1], 1, cfg).reasons)

    def test_detector_dedup_and_resonance(self):
        cfg = self.cfg()
        det = Detector(cfg)
        s = SymbolSeries("X", [1, 3])
        s.finalized_ts = T0 - MIN
        hits_all = []
        for i, c in enumerate(range(100, 116)):
            closed = s.finalize(bar(i, c - 1, c))
            hits, _, _ = det.check("X", s.bars, closed, T0 + (i + 1) * MIN)
            hits_all.append(hits)
        # 第 5 根（4 次比较）首次触发 1m，之后连涨延长不再重复报
        self.assertEqual([h.evaluations[0].tf for h in hits_all[4]], [1])
        self.assertTrue(all(not h for h in hits_all[5:14]))
        # 第 15 根时 3m 也达到 4 次比较，共振 1m+3m
        self.assertEqual([e.tf for e in hits_all[14][0].evaluations], [3])
        self.assertEqual(hits_all[14][0].resonance, [1, 3])
        self.assertEqual(hits_all[14][0].level, "medium")

    def test_detector_stale_and_silent(self):
        det = Detector(self.cfg())
        s = SymbolSeries("X", [1])
        closed = feed(s, [1, 2, 3, 4, 5])[-1]
        end = T0 + 5 * MIN
        hits, stale, _ = det.check("X", s.bars, closed, end + 2 * MIN)
        self.assertEqual((len(hits), len(stale)), (0, 1))

        det = Detector(self.cfg())
        hits, stale, _ = det.check("X", s.bars, closed, end, silent_until=end)
        self.assertEqual((hits, stale), ([], []))
        # 静默期内出现的连涨已记入去重，预热结束后不会再报
        hits, _, _ = det.check("X", s.bars, closed, end)
        self.assertEqual(hits, [])


if __name__ == "__main__":
    unittest.main()
