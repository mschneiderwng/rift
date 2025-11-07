from precisely import assert_that, equal_to

from nexo.snapshots import Bookmark, Snapshot


def test_parse_snapshot():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", creation="123")
    assert_that(Snapshot.parse("source/A@s1 uuid:source/A@s1 123 1"), equal_to(s1))


def test_snapshot_name():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", creation="123")
    assert_that(s1.name, equal_to("s1"))


def test_parse_bookmark():
    b1 = Bookmark(fqn="source/A#s1", guid="uuid:source/A@s1", creation="123")
    assert_that(Bookmark.parse("source/A#s1 uuid:source/A@s1 123 1"), equal_to(b1))


def test_bookmark_name():
    b1 = Bookmark(fqn="source/A#s1", guid="uuid:source/A@s1", creation="123")
    assert_that(b1.name, equal_to("s1"))
