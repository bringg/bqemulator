"""Pin the ``GET /tables/_bqemu_anonymous/anon<digest>`` synthesis path.

Anonymous query-result tables are not registered in the catalog (it
would pollute every listing endpoint); the tables route's GET
handler intercepts the reserved dataset and synthesises a response
from ``JOB_RESULTS`` / ``JOB_SCHEMAS`` so
``client.get_table(query_job.destination)`` returns a real
``num_rows`` + ``schema`` for ``SELECT``-style queries.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pyarrow as pa
import pytest

from bqemulator.api.routes.tables import _anonymous_result_table_to_rest
from bqemulator.jobs.executor import (
    JOB_RESULTS,
    JOB_SCHEMAS,
    _build_query_configuration,
    anonymous_result_table_id,
    discard_job_results,
    is_anonymous_result_table,
    resolve_anonymous_result_job,
)

pytestmark = pytest.mark.unit

#: Advertise ``(project_id, job_id)``'s anonymous table holding column ``n``; return its id.
_Advertise = Callable[[str, str, list[int]], str]


@pytest.fixture
def advertise() -> Iterator[_Advertise]:
    """Advertise a job's anonymous table as the executor does; drop it afterwards."""
    advertised: list[tuple[str, str]] = []

    def _advertise(project_id: str, job_id: str, values: list[int]) -> str:
        JOB_RESULTS[job_id] = pa.table({"n": pa.array(values)})
        config = _build_query_configuration("SELECT 1", project_id, job_id)
        advertised.append((project_id, job_id))
        table_id: str = config["query"]["destinationTable"]["tableId"]
        return table_id

    yield _advertise
    for project_id, job_id in advertised:
        discard_job_results(project_id, job_id)


class TestAnonymousResultTableToRest:
    def test_with_known_job_returns_arrow_row_count(self, advertise: _Advertise) -> None:
        job_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        table_id = advertise("proj", job_id, [1, 2, 3])
        JOB_SCHEMAS[job_id] = [{"name": "n", "type": "INTEGER", "mode": "NULLABLE"}]
        body = _anonymous_result_table_to_rest("proj", "_bqemu_anonymous", table_id)
        assert body["kind"] == "bigquery#table"
        assert body["tableReference"] == {
            "projectId": "proj",
            "datasetId": "_bqemu_anonymous",
            "tableId": table_id,
        }
        assert body["numRows"] == "3"
        assert body["schema"]["fields"] == [{"name": "n", "type": "INTEGER", "mode": "NULLABLE"}]

    def test_unknown_job_returns_zero_rows_and_empty_schema(self) -> None:
        body = _anonymous_result_table_to_rest("proj", "_bqemu_anonymous", "anon" + "0" * 40)
        assert body["numRows"] == "0"
        assert body["schema"]["fields"] == []

    @pytest.mark.parametrize(
        "job_id",
        [
            "job_qw8pT-LJOk-hmKsSh7ULEGSKTw-e",  # Ruby-client id with hyphens
            "job_C8Fyu7FynTc7UTrfCh1QkZFz32Pj",  # Ruby-client id, 32 chars, no hyphens
            "scripted-job-id",  # arbitrary client-chosen id
        ],
    )
    def test_client_chosen_job_ids_resolve(self, advertise: _Advertise, job_id: str) -> None:
        table_id = advertise("proj", job_id, [1, 2])
        body = _anonymous_result_table_to_rest("proj", "_bqemu_anonymous", table_id)
        assert body["numRows"] == "2"


class TestResolveAnonymousResultJob:
    def test_is_anonymous_result_table(self) -> None:
        assert is_anonymous_result_table("_bqemu_anonymous", "anonabc")
        assert not is_anonymous_result_table("_bqemu_anonymous", "other")
        assert not is_anonymous_result_table("ds", "anonabc")

    def test_table_id_is_a_plain_identifier(self) -> None:
        table_id = anonymous_result_table_id("proj", "job_Ab-cD-ef")
        assert table_id.startswith("anon")
        assert table_id[4:].isalnum()

    def test_hyphen_only_differences_resolve_apart(self, advertise: _Advertise) -> None:
        plain = advertise("proj", "ab", [1])
        hyphenated = advertise("proj", "a-b", [2])
        assert plain != hyphenated
        assert resolve_anonymous_result_job("proj", plain) == "ab"
        assert resolve_anonymous_result_job("proj", hyphenated) == "a-b"

    def test_lookup_is_scoped_to_the_project(self, advertise: _Advertise) -> None:
        table_id = advertise("proj", "job_scoped", [1])
        assert resolve_anonymous_result_job("proj", table_id) == "job_scoped"
        assert resolve_anonymous_result_job("other", table_id) is None

    def test_discard_drops_rows_and_mapping(self, advertise: _Advertise) -> None:
        table_id = advertise("proj", "job_gone", [1, 2])
        dropped = discard_job_results("proj", "job_gone")
        assert dropped is not None
        assert dropped.num_rows == 2
        assert resolve_anonymous_result_job("proj", table_id) is None
        assert "job_gone" not in JOB_RESULTS
        assert discard_job_results("proj", "job_gone") is None

    def test_unknown_table_id_returns_none(self) -> None:
        assert resolve_anonymous_result_job("proj", "anonnosuchjob") is None
