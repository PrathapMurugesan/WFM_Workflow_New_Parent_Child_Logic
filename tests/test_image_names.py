"""silver image_name cells stay within the DB column size (varchar(512)): only whole names, in order."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "silver"))

import config  # noqa: E402
from common.utils import join_image_names  # noqa: E402


def test_long_list_is_cut_to_whole_names():
    names = [f"5386cf6e-35dc-4796-a9dc-a5719c6ba6af_lcms_{i:05d}.jpg" for i in range(200)]
    cell = join_image_names(names)
    assert len(cell) <= config.IMAGE_NAME_MAX_CHARS
    kept = cell.split(config.IMAGE_SEPARATOR)
    assert kept == names[:len(kept)]                 # first names, none cut in the middle
    assert len(cell) + len(config.IMAGE_SEPARATOR) + len(names[len(kept)]) > config.IMAGE_NAME_MAX_CHARS


def test_short_list_unchanged_and_empty_values():
    assert join_image_names(["a.jpg", None, "", float("nan"), "b.jpg"]) == "a.jpg, b.jpg"
    assert join_image_names([]) is None
    assert join_image_names(None) is None
