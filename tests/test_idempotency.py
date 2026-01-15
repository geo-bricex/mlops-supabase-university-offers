from contextlib import contextmanager
from types import SimpleNamespace

from src.etl import ingest


class FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class FakeSession:
    def __init__(self, row):
        self.row = row
        self.updated = False
        self.note = None

    def execute(self, query, params=None):
        sql = str(query)
        if "SELECT file_id, status" in sql:
            return FakeResult(self.row)
        if "UPDATE raw_ingest.files SET notes" in sql:
            self.updated = True
            self.note = params.get("note")
            return FakeResult(None)
        return FakeResult(None)


def test_should_skip_by_checksum_updates_note(monkeypatch):
    row = SimpleNamespace(file_id="file-1", status="success")
    session = FakeSession(row)

    @contextmanager
    def fake_get_db_session():
        yield session

    monkeypatch.setattr(ingest, "get_db_session", fake_get_db_session)
    should_skip, file_id = ingest.should_skip_by_checksum("checksum")
    assert should_skip is True
    assert file_id == "file-1"
    assert session.updated is True
    assert session.note
