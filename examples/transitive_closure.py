"""
Transitive Closure Workflow Example.

This workflow demonstrates file I/O capabilities in DataWorkflow:
1. Read an input file (edges.csv) from the repository
2. Compute the transitive closure of the graph
3. Write the result to an output file

The transitive closure of a graph is the set of all pairs (A, C) such that
there is a path from A to C in the original graph.
"""

from sdk.decorators import stage
from sdk.context import StageContext
import csv
from io import StringIO


@stage
def compute_transitive_closure(ctx: StageContext):
    """
    Compute the transitive closure of a graph defined by edges.csv.

    Reads data/edges.csv from the repository, computes transitive closure,
    and writes the result to transitive_closure.csv.
    """
    print("Reading edges from data/edges.csv...")

    # Read the edges file from the repository
    edges_content = ctx.read_file("data/edges.csv")

    # Parse CSV
    edges = []
    csv_reader = csv.DictReader(StringIO(edges_content))
    for row in csv_reader:
        edges.append((row['from'], row['to']))

    print(f"Found {len(edges)} edges: {edges}")

    # Compute transitive closure using Warshall's algorithm
    # First, collect all nodes
    nodes = set()
    for from_node, to_node in edges:
        nodes.add(from_node)
        nodes.add(to_node)

    nodes = sorted(nodes)
    print(f"Nodes in graph: {nodes}")

    # Create adjacency matrix
    n = len(nodes)
    node_to_idx = {node: i for i, node in enumerate(nodes)}

    # Initialize adjacency matrix
    adj = [[False] * n for _ in range(n)]

    # Set direct edges
    for from_node, to_node in edges:
        i = node_to_idx[from_node]
        j = node_to_idx[to_node]
        adj[i][j] = True

    # Add reflexive edges (node to itself)
    for i in range(n):
        adj[i][i] = True

    # Warshall's algorithm for transitive closure
    for k in range(n):
        for i in range(n):
            for j in range(n):
                adj[i][j] = adj[i][j] or (adj[i][k] and adj[k][j])

    # Extract all pairs in transitive closure
    closure = []
    for i in range(n):
        for j in range(n):
            if adj[i][j]:
                closure.append((nodes[i], nodes[j]))

    print(f"Transitive closure has {len(closure)} pairs")

    # Write results to CSV
    output = StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(['from', 'to'])
    for from_node, to_node in sorted(closure):
        csv_writer.writerow([from_node, to_node])

    output_content = output.getvalue()

    # Write the output file
    ctx.write_file("transitive_closure.csv", output_content)
    print("Wrote transitive closure to transitive_closure.csv")

    return {
        "original_edges": len(edges),
        "closure_pairs": len(closure),
        "nodes": len(nodes)
    }


@stage
def main(ctx: StageContext):
    """Main workflow entry point."""
    print("Starting transitive closure workflow...")

    result = compute_transitive_closure()
    print(f"Workflow complete: {result}")

    return result
