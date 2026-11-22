"""归因判定真值表单测（纯 stdlib）：python3 -m unittest tests.test_verdict -v"""
import unittest

from app.verdict import (
    FORCE_STOPPED,
    NORMAL_EXIT,
    OOM_KILL,
    UNKNOWN,
    decide_verdict,
)


def v(**kw):
    return decide_verdict(**kw)[0]


class CleanExitTest(unittest.TestCase):
    def test_exit0_no_delta(self):
        self.assertEqual(
            v(returncode=0, oom_kill_before=3, oom_kill_after=3, stop_requested=False),
            NORMAL_EXIT,
        )

    def test_exit0_counter_missing(self):
        # 退出码 0 是决定性证据，计数缺失不影响
        self.assertEqual(
            v(returncode=0, oom_kill_before=None, oom_kill_after=None, stop_requested=False),
            NORMAL_EXIT,
        )

    def test_exit_nonzero_self_exit(self):
        self.assertEqual(
            v(returncode=1, oom_kill_before=0, oom_kill_after=0, stop_requested=False),
            NORMAL_EXIT,
        )

    def test_exit0_late_stop_request(self):
        # 强停请求在退出后才到达：仍是正常退出（理由中注明未生效）
        self.assertEqual(
            v(returncode=0, oom_kill_before=0, oom_kill_after=0, stop_requested=True),
            NORMAL_EXIT,
        )

    def test_exit0_but_oom_delta_conflict(self):
        self.assertEqual(
            v(returncode=0, oom_kill_before=1, oom_kill_after=2, stop_requested=False),
            UNKNOWN,
        )


class SigkillTest(unittest.TestCase):
    def test_oom(self):
        self.assertEqual(
            v(returncode=-9, oom_kill_before=5, oom_kill_after=6, stop_requested=False),
            OOM_KILL,
        )

    def test_force_stopped(self):
        self.assertEqual(
            v(returncode=-9, oom_kill_before=5, oom_kill_after=5, stop_requested=True),
            FORCE_STOPPED,
        )

    def test_oom_and_stop_conflict(self):
        # 计数涨 + 有强停请求：无法确定致命一击
        self.assertEqual(
            v(returncode=-9, oom_kill_before=5, oom_kill_after=6, stop_requested=True),
            UNKNOWN,
        )

    def test_sigkill_no_evidence(self):
        self.assertEqual(
            v(returncode=-9, oom_kill_before=5, oom_kill_after=5, stop_requested=False),
            UNKNOWN,
        )

    def test_sigkill_counter_missing(self):
        self.assertEqual(
            v(returncode=-9, oom_kill_before=None, oom_kill_after=None, stop_requested=True),
            UNKNOWN,
        )
        self.assertEqual(
            v(returncode=-9, oom_kill_before=None, oom_kill_after=None, stop_requested=False),
            UNKNOWN,
        )


class OtherCasesTest(unittest.TestCase):
    def test_other_signal(self):
        self.assertEqual(
            v(returncode=-15, oom_kill_before=0, oom_kill_after=0, stop_requested=False),
            UNKNOWN,
        )

    def test_join_cgroup_failed(self):
        self.assertEqual(
            v(returncode=69, oom_kill_before=0, oom_kill_after=0, stop_requested=False),
            UNKNOWN,
        )

    def test_returncode_missing(self):
        self.assertEqual(
            v(returncode=None, oom_kill_before=0, oom_kill_after=0, stop_requested=False),
            UNKNOWN,
        )


if __name__ == "__main__":
    unittest.main()
