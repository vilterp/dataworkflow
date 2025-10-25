"""
Example workflow demonstrating distributed execution.

This workflow shows how each stage invocation can run in a separate process.
When one stage calls another, it:
1. Sends a request to the control plane to create a call
2. Gets back an invocation ID
3. Polls for completion
4. Returns the result
"""
from sdk.decorators import stage, set_control_plane_url

# Configure the control plane URL (required for distributed mode)
set_control_plane_url('http://localhost:5001')


@stage
def main():
    """
    Main entry point for the workflow.

    This will execute in its own process. When it calls other stages,
    each of those will also execute in separate processes (potentially).
    """
    print("Main: Starting data pipeline...")

    # Each of these calls will:
    # 1. POST to /api/call to create the invocation
    # 2. Poll /api/call/<id> until complete
    # 3. Return the result
    data = extract_data()
    print(f"Main: Extracted {len(data)} items")

    transformed = transform_data(data)
    print(f"Main: Transformed data")

    result = load_data(transformed)
    print(f"Main: Loaded {result} records")

    return {
        "status": "success",
        "records_processed": result,
        "pipeline": "ETL complete"
    }


@stage
def extract_data():
    """
    Extract data from a source.

    This runs in its own process/worker. The parent (main) will block
    waiting for this to complete.
    """
    print("Extract: Fetching data from source...")
    # Simulate data extraction
    data = [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": 30},
        {"id": 4, "value": 40},
        {"id": 5, "value": 50},
    ]
    print(f"Extract: Found {len(data)} records")
    return data


@stage
def transform_data(data):
    """
    Transform the extracted data.

    Args:
        data: List of dictionaries to transform

    Returns:
        Transformed data
    """
    print(f"Transform: Processing {len(data)} records...")
    # Simulate transformation
    transformed = []
    for item in data:
        transformed.append({
            "id": item["id"],
            "value": item["value"] * 2,
            "processed": True
        })
    print(f"Transform: Transformed {len(transformed)} records")
    return transformed


@stage
def load_data(data):
    """
    Load transformed data into destination.

    Args:
        data: List of transformed records

    Returns:
        Number of records loaded
    """
    print(f"Load: Saving {len(data)} records...")
    # Simulate loading data
    count = len(data)
    print(f"Load: Successfully saved {count} records")
    return count


# For standalone testing (without control plane)
if __name__ == '__main__':
    # To run in standalone mode (no distributed execution):
    # from sdk.decorators import stage
    # Don't call set_control_plane_url()

    # To run in distributed mode:
    # 1. Start control plane: python src/app.py
    # 2. Start workers: python -m sdk.worker --server-url http://localhost:5001 \
    #                    --repo-name test-repo --workflow-file examples/distributed_workflow.py \
    #                    --commit-hash HEAD
    # 3. Run this script: python examples/distributed_workflow.py

    print("=" * 60)
    print("Starting distributed workflow execution...")
    print("=" * 60)

    result = main()

    print("=" * 60)
    print("Workflow completed!")
    print(f"Result: {result}")
    print("=" * 60)
