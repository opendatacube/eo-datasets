from io import StringIO
from pathlib import Path

from eodatasets3 import serialise


def test_dumps_yaml_scientific_notation() -> None:
    stream = StringIO()
    serialise.dumps_yaml(stream, {"response": [7e-06, 7e-06, 8e-06]})
    assert isinstance(stream.getvalue(), str)
    assert stream.getvalue().split()[3] == "7.e-06"
    assert stream.getvalue().split()[5] == "7.e-06"
    assert stream.getvalue().split()[7] == "8.e-06"


def test_dump_yaml_scientific_notation(tmp_path: Path) -> None:
    doc = {"response": [7e-06, 7e-06, 8e-06]}
    output_file = tmp_path / "test.yaml"
    serialise.dump_yaml(output_file, doc)
    text = output_file.read_text()
    assert "7.e-06" in text
    assert "7e-06" not in text
