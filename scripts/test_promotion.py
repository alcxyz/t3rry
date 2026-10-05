import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("promotion", Path(__file__).with_name("check-promotion.py"))
promotion = importlib.util.module_from_spec(spec)
spec.loader.exec_module(promotion)


def event(head="dev", repo="alcxyz/t3rry", base="main"):
    return {"pull_request": {"head": {"ref": head, "repo": {"full_name": repo}},
                             "base": {"ref": base}}}


class PromotionTests(unittest.TestCase):
    def test_new_version_from_dev_is_accepted(self):
        promotion.check(event(), "0.10.0", "0.9.2", False)

    def test_unreleased_or_reused_versions_are_rejected(self):
        for current, base, tag in [("0.9.2", "0.9.2", False),
                                   ("0.9.1", "0.9.2", False),
                                   ("0.10.0", "0.9.2", True)]:
            with self.assertRaises(ValueError):
                promotion.check(event(), current, base, tag)

    def test_main_rejects_feature_branches_and_fork_dev(self):
        for pr in [event(head="feature"), event(repo="someone/t3rry")]:
            with self.assertRaises(ValueError):
                promotion.check(pr, "0.10.0", "0.9.2", False)

    def test_development_prs_do_not_require_a_release(self):
        promotion.check(event(head="feature", base="dev"), "0.9.2-dev", "0.9.2", True)

    def test_first_release_is_accepted(self):
        promotion.check(event(), "0.1.0", "0.0.0", False)

    def test_version_at_reads_history(self):
        # A throwaway repository, so the test also runs in CI's shallow clone.
        with tempfile.TemporaryDirectory() as repo:
            def git(*args):
                return subprocess.check_output(
                    ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid",
                     "-c", "commit.gpgsign=false", *args], cwd=repo, text=True).strip()
            git("init", "-q")
            Path(repo, "README").write_text("x\n")
            git("add", "README")
            git("commit", "-q", "-m", "initial")
            first = git("rev-parse", "HEAD")
            Path(repo, "VERSION").write_text("0.1.0\n")
            git("add", "VERSION")
            git("commit", "-q", "-m", "version")
            second = git("rev-parse", "HEAD")
            cwd = os.getcwd()
            os.chdir(repo)
            self.addCleanup(os.chdir, cwd)
            self.assertEqual(promotion.version_at(first), "0.0.0")
            self.assertEqual(promotion.version_at(second), "0.1.0")
            with self.assertRaises(ValueError):
                promotion.version_at("0" * 40)

    def test_versions_are_compared_numerically(self):
        promotion.check(event(), "0.10.0", "0.9.2", False)
        for value in ["v1.0.0", "01.0.0", "1.0.0-rc1"]:
            with self.assertRaises(ValueError):
                promotion.semver(value)
