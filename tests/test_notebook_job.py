"""The job that triggers the sync notebook on Databricks: finding the Databricks job, starting a run, waiting for it.

Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.assets import reverse_etl_notebook_job as notebook
from tests.fakes import FakeHttp, error_text, make_config

HOST = notebook.DATABRICKS_HOST


class FakeJobsApi(FakeHttp):
    """The slice of the Databricks Jobs API the trigger uses. `jobs` is what jobs/list returns; the run reports each
    of `run_states` in turn (the last one repeats) and `run_now_status` is the status of jobs/run-now."""

    def __init__(self, calls, jobs=({"job_id": 7},), run_states=("TERMINATED",), result_state="SUCCESS", run_now_status=200):
        super().__init__(calls)
        self.jobs, self.run_states, self.result_state, self.run_now_status = list(jobs), list(run_states), result_state, run_now_status
        self.requests: list[tuple[str, str, dict]] = []

    def _reply(self, payload, ok=True, status=200, text=""):
        response = self._response(payload)
        response.ok, response.status_code, response.text = ok, status, text
        return response

    def get(self, url, timeout=10, **kwargs):
        self.requests.append(("GET", url, kwargs))
        if url.endswith("/jobs/list"):
            return self._reply({"jobs": self.jobs})
        state = self.run_states.pop(0) if len(self.run_states) > 1 else self.run_states[0]
        return self._reply(
            {
                "state": {"life_cycle_state": state, "result_state": self.result_state, "state_message": "cluster failed"},
                "start_time": 1_000,
                "end_time": 13_500,
                "run_page_url": "https://databricks.invalid/run/99",
            }
        )

    def post(self, url, timeout=30, **kwargs):
        self.requests.append(("POST", url, kwargs))
        if self.run_now_status != 200:
            return self._reply({}, ok=False, status=self.run_now_status, text="forbidden")
        return self._reply({"run_id": 99})


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(notebook.time, "sleep", lambda seconds: None)


@pytest.fixture
def sync_config(monkeypatch):
    cfg = make_config()
    monkeypatch.setattr(notebook, "_load_sync_config", lambda label: cfg)
    monkeypatch.setattr(notebook, "get_row_fields", lambda location: [("rid", "int", False)])
    return cfg


@pytest.fixture
def added_columns(monkeypatch):
    added: list = []
    monkeypatch.setattr(notebook, "add_missing_columns", lambda risingwave, registry, cfg, row_fields: added.append(row_fields) or ["x"])
    return added


@pytest.fixture
def trigger(calls, resources):
    def run(label="orders", add_risingwave_columns=True, jobs_api=None, **options):
        res = resources(**options)
        res["http"] = jobs_api or FakeJobsApi(calls)
        config = {"label": label, "add_risingwave_columns": add_risingwave_columns}
        defs = dg.Definitions(jobs=[notebook.reverse_etl_notebook_sync_job], resources=res)
        job = defs.resolve_job_def("reverse_etl_notebook_sync_job")
        run_config = {"ops": {"reverse_etl_trigger_notebook_sync": {"config": config}}}
        return job.execute_in_process(run_config=run_config, raise_on_error=False), res["http"]

    return run


def metadata(result):
    (event,) = [event for event in result.all_events if event.is_successful_output]
    return {name: value for name, value in event.step_output_data.metadata.items()}


class TestTrigger:
    def test_a_successful_run_is_started_with_the_label_and_encoding_and_reported(self, trigger, sync_config, added_columns):
        result, api = trigger()
        assert result.success
        post = [request for request in api.requests if request[0] == "POST"]
        assert [(url, kwargs["json"]) for _, url, kwargs in post] == [
            (f"{HOST}/api/2.1/jobs/run-now", {"job_id": 7, "job_parameters": {"label": "orders", "encoding": sync_config.encoding}})
        ]
        reported = metadata(result)
        assert reported["run_id"].value == 99
        assert reported["result"].text == "SUCCESS"
        assert reported["databricks_cluster_seconds"].value == 12.5
        assert reported["risingwave_columns_added"].data == ["x"]

    def test_the_databricks_job_is_looked_up_by_name(self, trigger, sync_config, added_columns):
        _, api = trigger()
        (_, url, kwargs), *_ = api.requests
        assert (url, kwargs["params"]) == (f"{HOST}/api/2.1/jobs/list", {"name": notebook.DATABRICKS_JOB_NAME})

    def test_risingwave_columns_are_added_before_the_run_starts(self, trigger, sync_config, added_columns, calls):
        trigger()
        assert added_columns == [[("rid", "int", False)]]

    def test_the_columns_step_can_be_switched_off(self, trigger, sync_config, added_columns):
        result, _ = trigger(add_risingwave_columns=False)
        assert result.success and added_columns == []
        assert metadata(result)["risingwave_columns_added"].data == []

    def test_a_failed_run_fails_the_op_with_the_state_message(self, trigger, sync_config, added_columns, calls):
        result, _ = trigger(jobs_api=FakeJobsApi(calls, result_state="FAILED"))
        assert not result.success
        text = error_text(result)
        assert "failed (FAILED)" in text and "cluster failed" in text

    def test_the_run_is_polled_until_it_terminates(self, trigger, sync_config, added_columns, calls):
        result, api = trigger(jobs_api=FakeJobsApi(calls, run_states=("PENDING", "RUNNING", "TERMINATED")))
        assert result.success
        assert len([request for request in api.requests if request[1].endswith("/runs/get")]) == 3

    @pytest.mark.parametrize("label", ["Orders", "1orders", "or ders", "orders; drop", ""])
    def test_a_label_that_could_not_name_a_table_is_refused_before_anything_is_called(self, trigger, sync_config, label, calls):
        result, api = trigger(label=label)
        assert not result.success
        assert "Invalid label" in error_text(result)
        assert api.requests == []


class TestFindingTheJob:
    def test_no_such_job_names_the_permission_it_may_lack(self, trigger, sync_config, added_columns, calls):
        result, _ = trigger(jobs_api=FakeJobsApi(calls, jobs=()))
        assert not result.success
        assert "Can Manage Run" in error_text(result)

    def test_two_jobs_with_the_name_are_refused(self, trigger, sync_config, added_columns, calls):
        result, api = trigger(jobs_api=FakeJobsApi(calls, jobs=({"job_id": 1}, {"job_id": 2})))
        assert not result.success
        assert "2 Databricks jobs" in error_text(result)
        assert not [request for request in api.requests if request[0] == "POST"]

    def test_a_job_that_cannot_be_found_stops_before_risingwave_is_changed(self, trigger, sync_config, added_columns, calls):
        trigger(jobs_api=FakeJobsApi(calls, jobs=()))
        assert added_columns == []


class TestStartingTheRun:
    def test_a_rejected_run_now_reports_the_status_and_body(self, trigger, sync_config, added_columns, calls):
        result, _ = trigger(jobs_api=FakeJobsApi(calls, run_now_status=403))
        assert not result.success
        assert "(403): forbidden" in error_text(result)


class TestWaiting:
    def test_a_run_that_never_finishes_times_out(self, calls):
        with pytest.raises(TimeoutError, match="did not finish within 0s"):
            notebook._wait_for_run(FakeJobsApi(calls, run_states=("RUNNING",)), "token", 99, max_wait=0)

    @pytest.mark.parametrize("state", ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"])
    def test_these_states_end_the_wait(self, calls, state):
        data = notebook._wait_for_run(FakeJobsApi(calls, run_states=(state,)), "token", 99)
        assert data["state"]["life_cycle_state"] == state


class TestLoadingTheSyncConfig:
    def test_a_label_is_resolved_from_its_defs_yaml(self):
        assert notebook._load_sync_config("cdf").sync_name == "reverse_etl_cdf"

    def test_an_unknown_label_lists_the_known_ones(self):
        with pytest.raises(ValueError, match="No ReverseEtlCdfDemo instance named 'nope'.*known labels: .*'cdf'"):
            notebook._load_sync_config("nope")
