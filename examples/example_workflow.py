"""
Example DataWorkflow workflow with distributed execution.

This workflow demonstrates the distributed execution model where each
stage invocation can potentially run in a separate Python process.
"""

from sdk.decorators import WorkflowRunner

# Create a workflow runner connected to the control plane
runner = WorkflowRunner(
    control_plane_url='http://localhost:5001',
    repo_name='test-repo',
    commit_hash='8bf3a47b0c439a18d9d5eae4f342033509eb0cd8',
    workflow_file='examples/example_workflow.py'
)


@runner.stage
def main():
    """
    Main workflow function - this is the entry point for all workflows.

    When executed, this will create a call invocation in the control plane,
    and a worker will pick it up and execute it.
    """
    print("Hello from the example workflow!")

    # Each of these function calls will go through the control plane
    # and potentially execute on different workers
    data = extract_data()
    transformed_data = transform_data(data)
    result = load_data(transformed_data)

    return {"status": "success", "rows_loaded": result}


@runner.stage
def extract_data():
    """Extract data from a source."""
    print("Extracting data...")
    # In a real workflow, you might read from a database, API, or file
    return [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 3, "name": "Charlie", "value": 300},
    ]


@runner.stage
def transform_data(data):
    """Transform the extracted data."""
    print(f"Transforming {len(data)} rows...")
    # In a real workflow, you might clean, filter, or aggregate data
    return [
        {**row, "value_doubled": row["value"] * 2}
        for row in data
    ]


@runner.stage
def load_data(data):
    """Load the transformed data to a destination."""
    print(f"Loading {len(data)} rows...")
    # In a real workflow, you might write to a database, file, or API
    for row in data:
        print(f"  Loaded: {row}")

    return len(data)


if __name__ == '__main__':
    # To run this workflow in distributed mode:
    # 1. Start the control plane: python src/app.py
    # 2. Start one or more workers:
    #    python -m sdk.worker \
    #      --server-url http://localhost:5001 \
    #      --repo-name my-repo \
    #      --workflow-file examples/example_workflow.py \
    #      --commit-hash HEAD
    # 3. Run this script: python examples/example_workflow.py

    print("Starting workflow execution...")
    result = runner.run(main)
    print(f"Workflow completed with result: {result}")
