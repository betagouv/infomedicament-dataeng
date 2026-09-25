"""Tests for the BDPM presentation price importer."""

from unittest.mock import MagicMock, patch

import pytest

from infomedicament_dataeng.bdpm import (
    fetch_ceps_prices,
    fetch_cnam_agrements,
    import_ceps_prices,
    import_cnam_agrements,
)


def _source_row(
    cip: str,
    medicine: str = "",
    public: str = "",
    fee: str = "",
    reimbursement_rates: str = "65%",
) -> str:
    fields = [
        "60002283",
        "4949729",
        "presentation",
        "Présentation active",
        "Déclaration de commercialisation",
        "16/03/2011",
        cip,
        "oui",
        reimbursement_rates,
        medicine,
        public,
        fee,
        "",
    ]
    return "\t".join(fields) + "\n"


def _mock_download(content: str):
    response = MagicMock()
    response.read.return_value = content.encode()
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    return patch("infomedicament_dataeng.bdpm.urllib.request.urlopen", return_value=response)


def test_fetches_all_published_prices_as_cents():
    content = _source_row("3400949497294", "24,34", "25,36", "1,02") + _source_row(
        "3400930301043", "7,518,58", "7,519,60", "1,02"
    )

    with _mock_download(content):
        rows = fetch_ceps_prices()

    assert rows == [
        ("3400949497294", (65,), 2434, 2536, 102),
        ("3400930301043", (65,), 751858, 751960, 102),
    ]


def test_skips_presentations_without_prices_and_keeps_partial_prices():
    content = _source_row("3400949497294", reimbursement_rates="") + _source_row("3400930280300", "8,993,73", "1,02")

    with _mock_download(content):
        rows = fetch_ceps_prices()

    assert rows == [("3400930280300", (65,), 899373, 102, None)]


def test_parses_multiple_reimbursement_rates():
    content = _source_row("3400949497294", "24,34", "25,36", "1,02").replace("65%", "30 % ; 65%")

    with _mock_download(content):
        rows = fetch_ceps_prices()

    assert rows[0][1] == (30, 65)


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("too\tfew\tcolumns\n", "Unexpected column count"),
        (_source_row("not-a-cip", "1,00", "2,02", "1,02"), "Invalid CIP13"),
        (_source_row("3400949497294", "EUR 24.34", "25,36", "1,02"), "Invalid medicine price"),
        (_source_row("3400949497294", "1,00", "2,02", "1,02").replace("65%", "101%"), "Invalid reimbursement rate"),
    ],
)
def test_rejects_malformed_downloads(content: str, message: str):
    with _mock_download(content), pytest.raises(ValueError, match=message):
        fetch_ceps_prices()


def test_refuses_download_without_any_prices():
    content = _source_row("3400949497294", reimbursement_rates="")
    with _mock_download(content), pytest.raises(ValueError, match="refusing to truncate"):
        fetch_ceps_prices()


def test_fetches_cnam_agrements_as_nullable_booleans():
    content = (
        _source_row("3400949497294").replace("\toui\t65%", "\toui\t65%")
        + _source_row("3400930301043").replace("\toui\t65%", "\tnon\t65%")
        + _source_row("3400930280300").replace("\toui\t65%", "\tinconnu\t65%")
    )

    with _mock_download(content):
        rows = fetch_cnam_agrements()

    assert rows == [
        ("3400949497294", True),
        ("3400930301043", False),
        ("3400930280300", None),
    ]


def test_rejects_unknown_cnam_agrement_value():
    content = _source_row("3400949497294").replace("\toui\t65%", "\tpeut-être\t65%")

    with _mock_download(content), pytest.raises(ValueError, match="Invalid agrément aux collectivités"):
        fetch_cnam_agrements()


def test_truncates_and_copies_prices_in_one_transaction():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__exit__.return_value = False
    rows = [
        ("3400949497294", (65,), 2434, 2536, 102),
        ("3400930280300", (65,), 899373, 102, None),
    ]

    with (
        patch("infomedicament_dataeng.bdpm.fetch_ceps_prices", return_value=rows),
        patch("infomedicament_dataeng.bdpm.get_postgres_engine", return_value=mock_engine),
    ):
        count = import_ceps_prices()

    assert count == 2
    assert "TRUNCATE TABLE ceps_price" in str(mock_conn.execute.call_args.args[0])
    cursor = mock_conn.connection.dbapi_connection.cursor.return_value.__enter__.return_value
    copy_sql, buf = cursor.copy_expert.call_args.args
    assert (
        "COPY ceps_price (cip, reimbursement_rates, medicine_price_cents, public_price_cents, "
        "dispensing_fee_cents)" in copy_sql
    )
    assert buf.getvalue() == "3400949497294,{65},2434,2536,102\n3400930280300,{65},899373,102,\n"


def test_truncates_and_copies_cnam_agrements_in_one_transaction():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__exit__.return_value = False
    rows = [("3400949497294", True), ("3400930301043", False), ("3400930280300", None)]

    with (
        patch("infomedicament_dataeng.bdpm.fetch_cnam_agrements", return_value=rows),
        patch("infomedicament_dataeng.bdpm.get_postgres_engine", return_value=mock_engine),
    ):
        count = import_cnam_agrements()

    assert count == 3
    assert "TRUNCATE TABLE cnam_agrement_collectivite" in str(mock_conn.execute.call_args.args[0])
    cursor = mock_conn.connection.dbapi_connection.cursor.return_value.__enter__.return_value
    copy_sql, buf = cursor.copy_expert.call_args.args
    assert "COPY cnam_agrement_collectivite (cip, agrement_collectivite)" in copy_sql
    assert buf.getvalue() == "3400949497294,True\n3400930301043,False\n3400930280300,\n"
