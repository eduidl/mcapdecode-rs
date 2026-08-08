"""Fast MCAP decoding with PyArrow output."""

from ._mcapdecode import McapDecodeError, TopicInfo, list_topics, read


def _topic_info_from_fields(*args):
    return TopicInfo._from_fields(*args)

__all__ = ["McapDecodeError", "TopicInfo", "list_topics", "read"]
