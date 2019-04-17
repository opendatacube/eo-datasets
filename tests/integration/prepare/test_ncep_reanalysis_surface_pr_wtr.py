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
    expected_metadata_path = output_path / 'pr_wtr.eatm.2018.test-metadata.yaml'

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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "d194b0f7b9885bcda7bd9f2ef0c838d5",
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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "a3bf7157fd85573bb1cb193e145ef5e9",
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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "45eaab4272275972b185bb6ab2e18057",
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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "1d2b45cd6d00514dbfe54fd35aa16de2",
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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "9ab0d685c1545e338909297038ded5d3",
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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "b8f1264cafe55f92866464e7151ff3f0",
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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "4d1a74a1d48f5985bf891a828a381074",
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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "2aa736fe24b25a3ab62d1ab4971dec0e",
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
                "name": "netCDF"
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
                    "spatial_reference": (
                        'GEOGCS["Hu Tzu Shan 1950",DATUM["Hu_Tzu_Shan_1950",SPHEROID'
                        '["International 1924",6378388,297,AUTHORITY["EPSG","7022"]]'
                        ',TOWGS84[-637,-549,-203,0,0,0,0],AUTHORITY["EPSG","6236"]],'
                        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree"'
                        ',0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4236"]]'
                    )
                }
            },
            "id": "e22bea968eb55e7fa770cba574c66dda",
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
