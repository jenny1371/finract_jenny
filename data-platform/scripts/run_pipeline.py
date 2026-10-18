"""Runs the reconcile_daily job once, in process, and prints what happened: the run status, how many data-quality checks
were evaluated, which failed, and why a failed run failed. Exits non-zero when the run failed.

    cd data-platform/orchestration
    PYTHONPATH=. python ../scripts/run_pipeline.py

For the UI, schedules and run history use `dagster dev -m payments_pipeline.definitions` instead.
"""
import sys

from payments_pipeline.definitions import defs


def main() -> int:
    result = defs.resolve_job_def("reconcile_daily").execute_in_process(raise_on_error=False)
    checks, failed_checks, failures = 0, [], []
    for event in result.all_events:
        if event.event_type_value == "ASSET_CHECK_EVALUATION":
            data = event.event_specific_data
            checks += 1
            if not data.passed:
                failed_checks.append(f"{data.check_name}: {data.description or ''}".strip())
        elif event.is_failure:
            error = getattr(event.event_specific_data, "error", None)
            while error is not None and error.cause:
                error = error.cause
            failures.append((error.message if error else event.event_type_value)[:300].replace("\n", " "))
    print("RUN", "SUCCESS" if result.success else "FAILED", "| checks evaluated:", checks)
    for line in failed_checks:
        print("  failed check:", line)
    for line in dict.fromkeys(failures):
        print("  failure:", line)
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
