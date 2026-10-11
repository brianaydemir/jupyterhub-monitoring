"""Tests for :mod:`app.cli.credentials`."""

from collections.abc import Callable
from pathlib import Path

import pytest

from app.cli import credentials
from app.cli.credentials import prompt_credentials, read_api_key
from app.core.errors import AppError, DataShapeError


def test_an_unreadable_api_key_file_is_an_expected_failure(tmp_path: Path) -> None:
    """Argument validation only checks that the path exists.

    Pointing ``--es-api-key`` at a secret's mount directory
    rather than at the file inside it therefore gets this far.
    Letting the ``OSError`` escape
    would have ``run_command`` report a fixable mistake as an unexpected error.
    """
    with pytest.raises(AppError) as failure:
        read_api_key(tmp_path, "ELASTICSEARCH_API_KEY")

    message = str(failure.value)
    assert message.startswith("Could not read the API key from ")
    assert str(tmp_path) in message
    assert "Errno" in message


def test_a_key_file_is_stripped_of_its_trailing_newline(tmp_path: Path) -> None:
    """A key mounted from a secret usually carries one."""
    key_file = tmp_path / "key.txt"
    key_file.write_text("secret\n", encoding="utf-8")
    assert read_api_key(key_file, "ELASTICSEARCH_API_KEY") == "secret"


def test_the_environment_variable_is_the_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """It is stripped too, for the same reason."""
    monkeypatch.setenv("ELASTICSEARCH_API_KEY", " secret\n")
    assert read_api_key(None, "ELASTICSEARCH_API_KEY") == "secret"


@pytest.mark.parametrize("value", ["", "   "])
def test_an_empty_environment_variable_supplies_no_key(
    value: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unset variable and an empty one are the same absence of a key."""
    monkeypatch.setenv("ELASTICSEARCH_API_KEY", value)
    with pytest.raises(DataShapeError, match="ELASTICSEARCH_API_KEY is unset or empty"):
        read_api_key(None, "ELASTICSEARCH_API_KEY")


def _answer_password(monkeypatch: pytest.MonkeyPatch, password: str) -> None:
    """Answer the password prompt with *password*."""

    def _getpass(_prompt: str) -> str:
        return password

    monkeypatch.setattr(credentials.getpass, "getpass", _getpass)


def _answer(username: str) -> Callable[..., str]:
    """Return a stand-in for ``input`` that answers with *username*."""

    def _input(_prompt: str = "") -> str:
        return username

    return _input


def test_a_supplied_username_is_not_prompted_for(monkeypatch: pytest.MonkeyPatch) -> None:
    """--es-username is the whole point of the flag."""

    def _refuse(prompt: str = "") -> str:
        raise AssertionError("the username should not have been prompted for")

    monkeypatch.setattr("builtins.input", _refuse)
    _answer_password(monkeypatch, "pw")

    assert prompt_credentials("elastic") == ("elastic", "pw")


def test_an_absent_username_is_prompted_for(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both values are then read interactively."""
    monkeypatch.setattr("builtins.input", _answer("elastic"))
    _answer_password(monkeypatch, "pw")

    assert prompt_credentials(None) == ("elastic", "pw")


@pytest.mark.parametrize("interruption", [EOFError, KeyboardInterrupt])
def test_cancelling_the_username_prompt_returns_nothing(
    interruption: type[BaseException],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ctrl-C and end-of-input are both a cancellation, not a crash."""

    def _cancel(prompt: str = "") -> str:
        raise interruption

    monkeypatch.setattr("builtins.input", _cancel)

    assert prompt_credentials(None) is None
    assert "Operation cancelled." in capsys.readouterr().err


@pytest.mark.parametrize("interruption", [EOFError, KeyboardInterrupt])
def test_cancelling_the_password_prompt_returns_nothing(
    interruption: type[BaseException],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The second prompt is cancelled the same way as the first."""

    def _cancel(prompt: str) -> str:
        raise interruption

    monkeypatch.setattr(credentials.getpass, "getpass", _cancel)

    assert prompt_credentials("elastic") is None
    assert "Operation cancelled." in capsys.readouterr().err


def test_an_empty_password_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Sending an empty credential to Elasticsearch would only fail later."""
    _answer_password(monkeypatch, "")

    assert prompt_credentials("elastic") is None
    assert "Username and password are required." in capsys.readouterr().err


def test_an_empty_username_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An empty answer at the prompt is not a username."""
    monkeypatch.setattr("builtins.input", _answer(""))
    _answer_password(monkeypatch, "pw")

    assert prompt_credentials(None) is None
    assert "Username and password are required." in capsys.readouterr().err
