import numpy as np

from eodatasets3 import images


def test_rescale_intensity():

    # Generated via: scipy.ndimage.rotate(np.arange(1000, 8000, 100).reshape((7,10)), 45, cval=-99)
    original_image = np.array(
        [
            [-99, -99, -99, -99, -99, -99, -99, -99, -99, -99, -99, -99],
            [-99, -99, -99, -99, -99, -99, 1852, 2730, -99, -99, -99, -99],
            [-99, -99, -99, -99, -99, 1711, 2570, 3428, 4169, -99, -99, -99],
            [-99, -99, -99, -99, 1568, 2432, 3287, 4009, 4805, 5610, -99, -99],
            [-99, -99, -99, 1427, 2291, 3144, 3871, 4663, 5451, 6181, 7049, -99],
            [-99, -99, 1284, 2149, 3003, 3729, 4521, 5312, 6040, 6889, 7757, -99],
            [-99, 1143, 2011, 2860, 3588, 4379, 5171, 5897, 6751, 7616, -99, -99],
            [-99, 1851, 2719, 3449, 4237, 5029, 5756, 6609, 7473, -99, -99, -99],
            [-99, -99, 3290, 4095, 4891, 5613, 6468, 7332, -99, -99, -99, -99],
            [-99, -99, -99, 4731, 5472, 6330, 7189, -99, -99, -99, -99, -99],
            [-99, -99, -99, -99, 6170, 7048, -99, -99, -99, -99, -99, -99],
            [-99, -99, -99, -99, -99, -99, -99, -99, -99, -99, -99, -99],
        ]
    )
    unmodified = original_image.copy()

    # Note that the nodata values are not scaled (a previous bug!)
    # they're translated to the output nodata value (0).

    non = 0  # (Using a variable so the array is more spaced-out & readable)
    expected_dynamic_rescale = np.array(
        [
            [non, non, non, non, non, non, non, non, non, non, non, non],
            [non, non, non, non, non, non, 22, 58, non, non, non, non],
            [non, non, non, non, non, 17, 51, 86, 116, non, non, non],
            [non, non, non, non, 11, 46, 80, 109, 141, 174, non, non],
            [non, non, non, 5, 40, 74, 104, 136, 167, 197, 232, non],
            [non, non, non, 34, 69, 98, 130, 162, 191, 225, 255, non],
            [non, non, 29, 63, 92, 124, 156, 185, 220, 255, non, non],
            [non, 22, 57, 87, 118, 150, 180, 214, 249, non, non, non],
            [non, non, 80, 113, 145, 174, 208, 243, non, non, non, non],
            [non, non, non, 138, 168, 203, 237, non, non, non, non, non],
            [non, non, non, non, 196, 232, non, non, non, non, non, non],
            [non, non, non, non, non, non, non, non, non, non, non, non],
        ],
        dtype=np.uint8,
    )

    dynamically_rescaled = images.rescale_intensity(original_image, image_nodata=-99)
    print("Dynamically rescaled result: ")
    print(repr(dynamically_rescaled))
    assert np.array_equal(dynamically_rescaled, expected_dynamic_rescale)
    assert np.array_equal(
        original_image, unmodified
    ), "rescale_intensity modified the input image"

    staticly_rescaled = images.rescale_intensity(
        original_image,
        static_range=(4000, 6000),
        out_range=(100, 255),
        image_nodata=-99,
    )
    print("Statically rescaled result: ")
    print(repr(staticly_rescaled))

    # Notice how many will be clipped to the min (100) without falling into nodata.
    expected_static_rescale = np.array(
        [
            [non, non, non, non, non, non, non, non, non, non, non, non],
            [non, non, non, non, non, non, 100, 100, non, non, non, non],
            [non, non, non, non, non, 100, 100, 100, 113, non, non, non],
            [non, non, non, non, 100, 100, 100, 100, 162, 224, non, non],
            [non, non, non, 100, 100, 100, 100, 151, 212, 255, 255, non],
            [non, non, 100, 100, 100, 100, 140, 201, 255, 255, 255, non],
            [non, 100, 100, 100, 100, 129, 190, 247, 255, 255, non, non],
            [non, 100, 100, 100, 118, 179, 236, 255, 255, non, non, non],
            [non, non, 100, 107, 169, 225, 255, 255, non, non, non, non],
            [non, non, non, 156, 214, 255, 255, non, non, non, non, non],
            [non, non, non, non, 255, 255, non, non, non, non, non, non],
            [non, non, non, non, non, non, non, non, non, non, non, non],
        ],
        dtype=np.uint8,
    )
    assert np.array_equal(staticly_rescaled, expected_static_rescale)
