from nurtelecom_gras_library.additional_functions import (
    send_sms, send_telegram_msg, _as_recipient_list,
)


class FakeConnector:
    def __init__(self):
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((query, params))


def test_as_recipient_list_normalizes():
    assert _as_recipient_list("a") == ["a"]
    assert _as_recipient_list(["a", "b"]) == ["a", "b"]
    assert _as_recipient_list(("a",)) == ["a"]


def test_send_sms_uses_bind_params_for_each_recipient():
    conn = FakeConnector()
    send_sms("hello", ["996700", "996701"], conn)
    assert len(conn.calls) == 2
    for query, params in conn.calls:
        assert ":msisdn" in query and ":sms_txt" in query
        # The payload must travel as a bind param, never interpolated.
        assert "hello" not in query
        assert params["sms_txt"] == "hello"


def test_send_telegram_msg_bind_params():
    conn = FakeConnector()
    send_telegram_msg("hi'; DROP TABLE x--", "chat1", conn)
    assert len(conn.calls) == 1
    query, params = conn.calls[0]
    assert ":receiver" in query and ":payload" in query
    # Injection attempt stays entirely inside the bound parameter.
    assert "DROP TABLE" not in query
    assert params["payload"] == "hi'; DROP TABLE x--"
