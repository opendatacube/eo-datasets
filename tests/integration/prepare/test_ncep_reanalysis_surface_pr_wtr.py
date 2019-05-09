# pylint: disable=E501
from functools import partial
from pprint import pformat
import yaml
from deepdiff import DeepDiff
from pathlib import Path

from .common import run_prepare_cli
from eodatasets.prepare import ncep_reanalysis_surface_pr_wtr

NCEP_PR_WTR_FILE: Path = Path(__file__).parent / 'data' / 'ncep-pr-wtr/pr_wtr.eatm.2018.test.nc'

_diff = partial(DeepDiff, significant_digits=6)


def test_prepare_ncep_reanalysis1_pr_wtr(tmpdir):
    output_path = Path(tmpdir)
    expected_metadata_path = output_path / 'pr_wtr.eatm.2018.test.ga-md.yaml'

    expected_doc = [
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-01T00:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "fb3afcb0430157c5845535a64e3b0c53",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 1,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        },
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-01T06:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "47d52e5bb6aa5cb6888d06c8e4bfa756",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 2,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        },
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-01T12:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "534362923d46579ea58466ab1d374e3a",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 3,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        },
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-01T18:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "a6000c88eeba5c3997e6985add9cb30d",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 4,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        },
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-02T00:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "227d9e888e195db895a082df117d9ad7",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 5,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        },
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-02T06:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "b843d85e2b9556b0855c1b8c750ce07e",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 6,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        },
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-02T12:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "0a0fd994712253f0b7125be34d8afe0c",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 7,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        },
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-02T18:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "7d98f5c1e0765d8d8890f4e8791fc10d",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 8,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        },
        {
            "creation_dt": "2019-04-17T01:14:42.954377+00:00",
            "description": "",
            "extent": {
                "center_dt": "2018-01-03T00:00:00+00:00",
                "coord": {
                    "ll": {
                        "lat": -91.25,
                        "lon": -1.25
                    },
                    "lr": {
                        "lat": -91.25,
                        "lon": 358.75
                    },
                    "ul": {
                        "lat": 91.25,
                        "lon": -1.25
                    },
                    "ur": {
                        "lat": 91.25,
                        "lon": 358.75
                    }
                }
            },
            "format": {
                "name": "NetCDF"
            },
            "grid_spatial": {
                "projection": {
                    "geo_ref_points": {
                        "ll": {
                            "x": -1.25,
                            "y": -91.25
                        },
                        "lr": {
                            "x": 358.75,
                            "y": -91.25
                        },
                        "ul": {
                            "x": -1.25,
                            "y": 91.25
                        },
                        "ur": {
                            "x": 358.75,
                            "y": 91.25
                        }
                    },
                    "spatial_reference": "epsg:4236"
                }
            },
            "id": "b65f15d8e74f57cf92f5779dd33e803e",
            "image": {
                "bands": {
                    "water_vapour": {
                        "band": 9,
                        "layer": "pr_wtr",
                        "path": "pr_wtr.eatm.2018.test.nc"
                    }
                }
            },
            "product_name": "ncep_reanalysis_surface_pr_wtr",
            "product_type": "auxiliary",
            "sources": {}
        }
    ]

    result = run_prepare_cli(
        ncep_reanalysis_surface_pr_wtr.main,
        '--output', str(output_path), str(NCEP_PR_WTR_FILE)
    )

    assert expected_metadata_path.exists()
    docs = list(yaml.safe_load_all(expected_metadata_path.open()))

    for idx in range(len(expected_doc)):
        doc_diff = _diff(expected_doc[idx], docs[idx], exclude_paths="root['creation_dt']")
        assert doc_diff == {}, pformat(doc_diff)
