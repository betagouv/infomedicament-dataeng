"""Tests for DB import utilities, converted from infomedicament JS tests."""

from infomedicament_dataeng.db import _insert_content_blocks, get_clean_html


class TestGetCleanHTML:
    def test_no_anchor_tags(self):
        html = '<p class="AmmCorpsTexte">Simple text</p>'
        assert get_clean_html(html) == html

    def test_removes_a_name_keeps_content(self):
        html = '<p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>'
        expected = '<p class="AmmAnnexeTitre">NOTICE</p>'
        assert get_clean_html(html) == expected

    def test_multiple_a_name_tags(self):
        html = '<p><a name="first">Premier</a></p><p><a name="second">Deuxième</a></p>'
        expected = "<p>Premier</p><p>Deuxième</p>"
        assert get_clean_html(html) == expected

    def test_nested_html_in_a_name(self):
        html = '<p><a name="_Toc123"><span class="bold">Titre</span> avec <em>emphase</em></a></p>'
        expected = '<p><span class="bold">Titre</span> avec <em>emphase</em></p>'
        assert get_clean_html(html) == expected

    def test_empty_a_name_tag(self):
        html = '<p><a name=""></a>Some text</p>'
        expected = "<p>Some text</p>"
        assert get_clean_html(html) == expected

    def test_preserves_href_anchors(self):
        html = '<p><a href="https://example.com">Link</a></p>'
        assert get_clean_html(html) == html

    def test_real_world_notice_example(self):
        html = '<p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>'
        expected = '<p class="AmmAnnexeTitre">NOTICE</p>'
        assert get_clean_html(html) == expected

    def test_a_name_at_start_of_string(self):
        html = '<a name="start">Content</a>'
        assert get_clean_html(html) == "Content"


class TestInsertContentBlocks:
    def test_inserts_blocks_and_returns_ids(self, fake_cursor):
        cur = fake_cursor(ids=[1, 2, 3])
        result = _insert_content_blocks(
            cur,
            "notices_content",
            [
                {"content": "Bloc 1"},
                {"content": "Bloc 2"},
                {"content": "Bloc ABCD"},
            ],
        )
        assert result == [1, 2, 3]
        assert len(cur.execute_calls) == 3

    def test_filters_blocks_without_content_children_or_text(self, fake_cursor):
        cur = fake_cursor(ids=[1])
        result = _insert_content_blocks(
            cur,
            "notices_content",
            [
                {"content": "Valid item"},
                {"type": "TypeOnly"},  # no content, children, text → filtered
                {"html": "<p>Test</p>"},  # no content, children, text → filtered
            ],
        )
        assert len(cur.execute_calls) == 1
        assert result == [1]

    def test_returns_empty_when_all_filtered(self, fake_cursor):
        cur = fake_cursor()
        result = _insert_content_blocks(
            cur,
            "notices_content",
            [
                {"type": "empty"},
                {"html": "only html"},
            ],
        )
        assert cur.execute_calls == []
        assert result == []

    def test_cleans_html_for_non_table_blocks(self, fake_cursor):
        cur = fake_cursor(ids=[1])
        _insert_content_blocks(
            cur,
            "notices_content",
            [
                {"content": "text", "html": '<p><a name="test">Content</a></p>'},
            ],
        )
        params = cur.execute_calls[-1]
        assert params[8] == "<p>Content</p>"

    def test_does_not_clean_html_for_table_blocks(self, fake_cursor):
        cur = fake_cursor(ids=[1])
        dirty_html = '<p><a name="test">Content</a></p>'
        # table block uses children to pass the content filter
        _insert_content_blocks(
            cur,
            "notices_content",
            [
                {"type": "table", "html": dirty_html, "children": [{"content": "cell"}]},
            ],
        )
        params = cur.execute_calls[-1]
        assert params[8] == dirty_html  # not cleaned

    def test_table_block_does_not_recurse_children(self, fake_cursor):
        cur = fake_cursor(ids=[1])
        _insert_content_blocks(
            cur,
            "notices_content",
            [
                {"type": "table", "html": "<table/>", "children": [{"content": "cell"}]},
            ],
        )
        # only the table itself is inserted, not the child cell
        assert len(cur.execute_calls) == 1
