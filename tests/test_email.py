import nurtelecom_gras_library.additional_functions as af
from nurtelecom_gras_library import send_email, send_email_html


class FakeSMTP:
    """Captures the message instead of talking to a real SMTP server."""
    last = {}

    def __init__(self, host, port=25):
        FakeSMTP.last = {"host": host, "port": port, "tls": False, "login": None}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        FakeSMTP.last["tls"] = True

    def login(self, user, password):
        FakeSMTP.last["login"] = (user, password)

    def send_message(self, msg):
        FakeSMTP.last["msg"] = msg


def test_send_email_plain(monkeypatch):
    monkeypatch.setattr(af.smtplib, "SMTP", FakeSMTP)
    send_email(send_to=["a@x.kg", "b@x.kg"], send_from="bot@x.kg",
               subject="Hi", host="smtp", content="hello")
    msg = FakeSMTP.last["msg"]
    assert msg["To"] == "a@x.kg, b@x.kg"
    assert msg["Subject"] == "Hi"
    # Date and Message-ID headers should be present.
    assert msg["Date"] and msg["Message-ID"]
    assert msg.get_content_type() == "text/plain"


def test_send_email_html_multipart(monkeypatch):
    monkeypatch.setattr(af.smtplib, "SMTP", FakeSMTP)
    send_email(send_to="a@x.kg", send_from="bot@x.kg", subject="Hi",
               host="smtp", content="plain", html="<b>hi</b>")
    msg = FakeSMTP.last["msg"]
    assert msg.get_content_type() == "multipart/alternative"
    subtypes = {p.get_content_type() for p in msg.iter_parts()}
    assert "text/plain" in subtypes and "text/html" in subtypes


def test_send_email_tls_and_auth(monkeypatch):
    monkeypatch.setattr(af.smtplib, "SMTP", FakeSMTP)
    send_email(send_to="a@x.kg", send_from="bot@x.kg", subject="Hi",
               host="smtp", content="x", port=587, use_tls=True,
               username="u", password="p")
    assert FakeSMTP.last["port"] == 587
    assert FakeSMTP.last["tls"] is True
    assert FakeSMTP.last["login"] == ("u", "p")


def test_send_email_html_wrapper_delegates(monkeypatch):
    monkeypatch.setattr(af.smtplib, "SMTP", FakeSMTP)
    send_email_html(send_to="a@x.kg", send_from="bot@x.kg", subject="Hi",
                    host="smtp", html="<b>hi</b>", text="plain")
    msg = FakeSMTP.last["msg"]
    assert msg.get_content_type() == "multipart/alternative"
