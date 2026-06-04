from nurtelecom_gras_library.TableauServerManager import TableauServerManager


def _mgr():
    # clean_tableau_url uses no instance state, so bypass __init__ (which would
    # require the optional tableauserverclient dependency).
    return TableauServerManager.__new__(TableauServerManager)


def test_clean_url_upgrades_http_and_strips_iid():
    url, filt = _mgr().clean_tableau_url(
        "http://h/#/views/WB/View?:iid=3")
    assert url == "https://h/#/views/WB/View"
    assert filt is None


def test_clean_url_extracts_reg_ord_filter():
    url, filt = _mgr().clean_tableau_url(
        "https://h/#/views/WB/View?REG_ORD=North&:iid=1")
    assert filt == "REG_ORD=North"
    assert ":iid" not in url
    assert "REG_ORD" not in url
    assert url == "https://h/#/views/WB/View"
