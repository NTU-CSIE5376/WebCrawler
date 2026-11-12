"""Exercise the approved deployment configuration through service loaders."""
from configparser import ConfigParser
from pathlib import Path
from types import SimpleNamespace

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONTROL = ROOT / "containers/scheduler_control/config/control.yaml"
SITEMAP = ROOT / "containers/sitemap_patroller/config/sitemap.yaml"


def test_supervisor_workers_match_scorer_configuration():
    cfg = yaml.safe_load(CONTROL.read_text())["golden_discovery_ranker_v1"]
    supervisor = ConfigParser(interpolation=None)
    supervisor.read(ROOT / "containers/scheduler_control/supervisord.conf")
    program = supervisor["program:golden_discovery_ranker_v1"]

    # A mismatch starts workers outside main.py's accepted worker-id range.
    assert program.getint("numprocs_start") == 0
    assert program.getint("numprocs") == cfg["num_workers"] == 16


def test_scorer_starts_approved_model_without_private_model_overrides(monkeypatch):
    from containers.scheduler_control.scorer import main

    for name in list(main.os.environ):
        if name.startswith(main.ENV_PREFIX + "_"):
            monkeypatch.delenv(name)
    # Compose enables the service; model selection comes from the tracked YAML.
    monkeypatch.setenv(main.ENV_PREFIX + "_ENABLED", "true")
    monkeypatch.setattr(main, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(main, "create_engine", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "sessionmaker", lambda **kwargs: None)
    monkeypatch.setattr(main, "Path", lambda value: SimpleNamespace(exists=lambda: True))

    loaded = {}
    v1 = SimpleNamespace(score_many=lambda urls: [])
    v2 = SimpleNamespace(heads=["general"], metadata={})

    def load_v1(path):
        loaded["v1"] = path
        return v1

    def load_v2(path, *, v1_scorer):
        loaded["v2"] = path
        loaded["fallback"] = v1_scorer
        return v2

    monkeypatch.setattr(main.GoldenDiscoveryRuntimeScorer, "load", load_v1)
    monkeypatch.setattr(main.GoldenDiscoveryRuntimeScorerV2, "load", load_v2)

    class Service:
        def __init__(self, *, cfg, Session, scorer):
            loaded["config"] = cfg
            loaded["scorer"] = scorer

        def run_once(self):
            loaded["ran"] = True

    monkeypatch.setattr(main, "GoldenDiscoveryRankerService", Service)
    monkeypatch.setattr(
        "sys.argv", ["scorer", "--config", str(CONTROL), "--worker-id", "15", "--once"]
    )
    main.main()

    assert loaded["v2"] == "/app/artifacts/golden_discovery_ranker_v2.joblib"
    assert loaded["v1"] == "/app/artifacts/golden_discovery_ranker_v1.joblib"
    assert loaded["fallback"] is v1.score_many
    assert loaded["scorer"] is v2
    assert loaded["config"].num_workers == 16
    assert loaded["config"].domain_priority_steering_enabled is True
    assert loaded["ran"] is True


def test_patrol_loads_approved_cadence_without_database_access(monkeypatch):
    from containers.sitemap_patroller.patrol import main

    def load_config(path):
        source = Path(path)
        if str(source).startswith("/app/"):
            source = ROOT / source.relative_to("/app")
        return yaml.safe_load(source.read_text())

    monkeypatch.setattr(main, "load_yaml", load_config)
    monkeypatch.setattr(main.psycopg2, "connect", lambda dsn: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(main, "load_sharding_config", lambda path, conn: ({}, set()))

    cfg, loop_interval = main._load_config(str(SITEMAP))

    assert (cfg.batch_limit, cfg.global_delay_sec, loop_interval) == (1000, 0.5, 60)
    assert cfg.per_domain_cooldown_sec == 60
    assert cfg.due_interval_hours == 24
