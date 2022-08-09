from io import StringIO

from eodatasets3 import serialise


def test_dumps_yaml_scientific_notation():
    stream = StringIO()
    serialise.dumps_yaml(stream, {"response": [7e-06, 7e-06, 8e-06]})
    assert type(stream.getvalue()) == str
    assert stream.getvalue().split()[3] == "7.e-06"
    assert stream.getvalue().split()[5] == "7.e-06"
    assert stream.getvalue().split()[7] == "8.e-06"
