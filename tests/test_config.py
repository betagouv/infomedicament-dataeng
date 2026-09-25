"""Tests for application configuration."""

import importlib


def test_config_loads_dotenv_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("S3_KEY_ID=dotenv-key\nS3_KEY_SECRET=dotenv-secret\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("S3_KEY_ID", raising=False)
    monkeypatch.delenv("S3_KEY_SECRET", raising=False)

    import infomedicament_dataeng.config as config

    config = importlib.reload(config)

    assert config.get_config().s3.access_key == "dotenv-key"
    assert config.get_config().s3.secret_key == "dotenv-secret"


def test_real_environment_overrides_dotenv_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("S3_KEY_ID=dotenv-key\nS3_KEY_SECRET=dotenv-secret\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("S3_KEY_ID", "real-env-key")
    monkeypatch.setenv("S3_KEY_SECRET", "real-env-secret")

    import infomedicament_dataeng.config as config

    config = importlib.reload(config)

    assert config.get_config().s3.access_key == "real-env-key"
    assert config.get_config().s3.secret_key == "real-env-secret"


def test_config_loads_grist_credentials(monkeypatch):
    monkeypatch.setenv("GRIST_DOC_ID", "doc-id")
    monkeypatch.setenv("GRIST_API_KEY", "api-key")

    import infomedicament_dataeng.config as config

    config = importlib.reload(config)

    assert config.get_config().grist.doc_id == "doc-id"
    assert config.get_config().grist.api_key == "api-key"
