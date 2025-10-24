"""
Example DataWorkflow workflow.

This workflow demonstrates the basic structure of a DataWorkflow workflow.
Workflows must define a main() function that serves as the entry point.
The @stage decorator is used to track stage execution in the workflow engine.
"""

from sdk.decorators import stage


@stage
def main():
    """
    Main workflow function - this is the entry point for all workflows.

    The workflow runner will call this function when executing the workflow.
    """
    print("Hello from the example workflow!")

    # You can define and call other functions from main
    data = extract_data()
    transformed_data = transform_data(data)
    result = load_data(transformed_data)

    return {"status": "success", "rows_loaded": result}


@stage
def extract_data():
    """Extract data from a source."""
    print("Extracting data...")
    # In a real workflow, you might read from a database, API, or file
    return [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 3, "name": "Charlie", "value": 300},
    ]


@stage
def transform_data(data):
    """Transform the extracted data."""
    print(f"Transforming {len(data)} rows...")
    # In a real workflow, you might clean, filter, or aggregate data
    return [
        {**row, "value_doubled": row["value"] * 2}
        for row in data
    ]


@stage
def load_data(data):
    """Load the transformed data to a destination."""
    print(f"Loading {len(data)} rows...")
    # In a real workflow, you might write to a database, file, or API
    for row in data:
        print(f"  Loaded: {row}")

    return len(data)
