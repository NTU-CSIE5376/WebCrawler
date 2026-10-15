from click.testing import CliRunner

from containers.scheduler_ingest.api.dispatcher import main


def test_dispatcher_help_documents_config_option():
    result = CliRunner().invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "--config" in result.output
    assert "Dispatch accepted crawl requests" in result.output


def test_dispatcher_requires_existing_config_file(tmp_path):
    runner = CliRunner()
    assert runner.invoke(main, []).exit_code == 2
    result = runner.invoke(main, ["--config", str(tmp_path / "missing.yaml")])
    assert result.exit_code == 2
    assert "does not exist" in result.output
