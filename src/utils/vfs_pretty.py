"""
Pretty printing utilities for the Virtual File System.
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.core.vfs import VirtualTreeNode, BlobNode, StageRunNode


def pretty_print_tree(node: 'VirtualTreeNode', prefix: str = "", is_last: bool = True) -> str:
    """
    Pretty print a virtual tree structure.

    Format:
        foo.py/           # base blob
          main/           # StageRun
            out.txt       # StageFile blob
            second_stage/ # StageRun
              out2.txt    # StageFile

    Args:
        node: Root node to print
        prefix: Current line prefix for indentation
        is_last: Whether this is the last child at this level

    Returns:
        Pretty-printed tree as a string
    """
    from src.core.vfs import BlobNode, StageRunNode

    lines = []

    # Build the current line
    if node.name:  # Skip root
        connector = "└── " if is_last else "├── "
        suffix = ""

        # Add suffix based on node type
        if isinstance(node, (BlobNode, StageRunNode)):
            suffix = "/"  # Directory-like nodes (have potential children)

        # Add type comment
        type_comment = f" # {node.node_type_name}"

        lines.append(f"{prefix}{connector}{node.name}{suffix}{type_comment}")

    # Get children and recurse
    children = node.get_children()
    for i, (child_name, child_node) in enumerate(children):
        is_last_child = (i == len(children) - 1)

        # Calculate new prefix
        if node.name:  # Skip root prefix calculation
            extension = "    " if is_last else "│   "
            child_prefix = prefix + extension
        else:
            child_prefix = ""

        lines.append(pretty_print_tree(child_node, child_prefix, is_last_child))

    return "\n".join(lines)
