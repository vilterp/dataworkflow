"""
Example distributed workflow.

This workflow demonstrates stage definitions that will be executed
by the distributed runner. Workflows are simple Python files with
decorated functions - no need to instantiate runners or manage
control plane connections.
"""

from sdk.decorators import stage
from sdk.context import StageContext


@stage
def extract_data(ctx: StageContext):
    """Extract data from source."""
    print("Extracting data from source...")
    return {"records": 100, "source": "database"}


@stage
def transform_data(ctx: StageContext, extraction_result):
    """Transform the extracted data."""
    print(f"Transforming {extraction_result['records']} records...")
    return {
        "records": extraction_result["records"],
        "transformed": True,
        "operations": ["clean", "normalize", "enrich"]
    }


@stage
def load_data(ctx: StageContext, transformation_result):
    """Load transformed data to destination."""
    print(f"Loading {transformation_result['records']} transformed records...")
    return {
        "records": transformation_result["records"],
        "loaded": True,
        "destination": "data_warehouse"
    }


@stage
def main(ctx: StageContext):
    """Main workflow orchestration."""
    print("Starting workflow execution...")

    # Each of these calls will be executed by the distributed runner
    data = extract_data()
    print(f"Extraction complete: {data}")

    transformed = transform_data(data)
    print(f"Transformation complete: {transformed}")

    loaded = load_data(transformed)
    print(f"Load complete: {loaded}")

    return {
        "status": "success",
        "records_processed": loaded["records"]
    }
