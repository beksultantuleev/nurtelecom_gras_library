import pytest

# Spatial extras are optional; skip the whole module if shapely isn't present.
shapely_wkt = pytest.importorskip("shapely.wkt")
shapely_wkb = pytest.importorskip("shapely.wkb")

from nurtelecom_gras_library.OracleGeoDataImporter import _parse_geometry


class FakeLOB:
    """Mimics an oracledb LOB object."""
    def __init__(self, content):
        self._content = content

    def read(self):
        return self._content


def _wkb_point():
    from shapely.geometry import Point
    return Point(1, 2).wkb


def test_parse_wkt_string():
    geom = _parse_geometry("POINT (1 2)", "auto", shapely_wkt, shapely_wkb)
    assert geom.geom_type == "Point"
    assert (geom.x, geom.y) == (1.0, 2.0)


def test_parse_wkb_bytes_auto():
    geom = _parse_geometry(_wkb_point(), "auto", shapely_wkt, shapely_wkb)
    assert geom.geom_type == "Point"


def test_parse_clob_lob():
    geom = _parse_geometry(FakeLOB("LINESTRING (0 0, 1 1)"), "auto", shapely_wkt, shapely_wkb)
    assert geom.geom_type == "LineString"


def test_parse_blob_lob():
    geom = _parse_geometry(FakeLOB(_wkb_point()), "auto", shapely_wkt, shapely_wkb)
    assert geom.geom_type == "Point"


@pytest.mark.parametrize("value", [None, float("nan"), "", "   "])
def test_parse_null_like_returns_none(value):
    assert _parse_geometry(value, "auto", shapely_wkt, shapely_wkb) is None


def test_forced_wkt_on_bytes():
    geom = _parse_geometry(b"POINT (3 4)", "wkt", shapely_wkt, shapely_wkb)
    assert (geom.x, geom.y) == (3.0, 4.0)
