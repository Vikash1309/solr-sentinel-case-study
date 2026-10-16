import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
os.environ.setdefault("LLM_PROVIDER", "none")

from generate_synthetic_logs import Sim, write_all  # noqa: E402

from app.config import Settings  # noqa: E402
from app.storage.repository import Repository  # noqa: E402


@pytest.fixture(scope="session")
def synthetic():
    return Sim(42).generate()


def make_settings(tmp_path: Path, **kw) -> Settings:
    return Settings(**{"log_dir": str(tmp_path / "logs"), "upload_dir": str(tmp_path / "uploads"),
                       "db_path": str(tmp_path / "t.db"), "llm_provider": "none", **kw})


def age_files(root: Path):
    old = time.time() - 120
    for f in root.rglob("*.log"):
        os.utime(f, (old, old))


@pytest.fixture()
def env(tmp_path, synthetic):
    s = make_settings(tmp_path)
    write_all(s.log_dir, synthetic)
    age_files(Path(s.log_dir))
    return s, Repository(s.db_path)


@pytest.fixture()
def analysed(env):
    from app.pipeline import run_analysis
    s, repo = env
    run_analysis(repo, s, None)
    return s, repo
