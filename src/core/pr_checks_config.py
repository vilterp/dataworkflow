"""
PR Checks Configuration - defines which checks must pass before a PR can be merged.

The configuration is stored as a YAML file (e.g., `.pr-checks.yml`) in the repository.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator
import yaml


class PRCheckConfig(BaseModel):
    """
    Configuration for a single PR check.
    Each check corresponds to a workflow stage that must complete successfully.
    """
    name: str = Field(..., description="Unique name for this check")
    workflow_file: str = Field(..., description="Path to the workflow file (e.g., 'workflows/ci.py')")
    stage_name: str = Field(..., description="Name of the stage to run (e.g., 'test' or 'lint')")
    arguments: Optional[Dict[str, Any]] = Field(default=None, description="Arguments to pass to the stage")
    required: bool = Field(default=True, description="Whether this check must pass for the PR to be mergeable")

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Ensure check name is not empty and doesn't contain special characters."""
        if not v or not v.strip():
            raise ValueError("Check name cannot be empty")
        if any(c in v for c in ['/', '\\', '\n', '\r', '\t']):
            raise ValueError("Check name cannot contain special characters (/, \\, newlines, tabs)")
        return v.strip()

    @field_validator('workflow_file')
    @classmethod
    def validate_workflow_file(cls, v: str) -> str:
        """Ensure workflow file path is valid."""
        if not v or not v.strip():
            raise ValueError("Workflow file cannot be empty")
        # Remove leading/trailing slashes for consistency
        return v.strip().strip('/')

    @field_validator('stage_name')
    @classmethod
    def validate_stage_name(cls, v: str) -> str:
        """Ensure stage name is valid."""
        if not v or not v.strip():
            raise ValueError("Stage name cannot be empty")
        return v.strip()


class PRChecksConfiguration(BaseModel):
    """
    Complete PR checks configuration for a repository.
    Loaded from `.pr-checks.yml` in the repository root.
    """
    version: str = Field(default="1", description="Configuration format version")
    checks: List[PRCheckConfig] = Field(default_factory=list, description="List of checks to run on PRs")

    @field_validator('checks')
    @classmethod
    def validate_unique_names(cls, v: List[PRCheckConfig]) -> List[PRCheckConfig]:
        """Ensure all check names are unique."""
        names = [check.name for check in v]
        if len(names) != len(set(names)):
            duplicates = [name for name in names if names.count(name) > 1]
            raise ValueError(f"Duplicate check names found: {', '.join(set(duplicates))}")
        return v

    @classmethod
    def from_yaml(cls, yaml_content: str) -> "PRChecksConfiguration":
        """Parse PR checks configuration from YAML content."""
        try:
            data = yaml.safe_load(yaml_content)
            if data is None:
                # Empty file
                return cls(checks=[])
            return cls(**data)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML: {e}")

    def to_yaml(self) -> str:
        """Convert configuration to YAML format."""
        data = self.model_dump(exclude_none=True)
        return yaml.dump(data, sort_keys=False, default_flow_style=False)


# Default configuration file name
PR_CHECKS_CONFIG_FILE = ".pr-checks.yml"


def load_pr_checks_config(yaml_content: str) -> PRChecksConfiguration:
    """
    Load and validate PR checks configuration from YAML content.

    Args:
        yaml_content: YAML content as a string

    Returns:
        Validated PRChecksConfiguration object

    Raises:
        ValueError: If the configuration is invalid
    """
    return PRChecksConfiguration.from_yaml(yaml_content)


# Example configuration for documentation:
EXAMPLE_CONFIG = """version: "1"
checks:
  - name: "tests"
    workflow_file: "workflows/ci.py"
    stage_name: "test"
    required: true

  - name: "lint"
    workflow_file: "workflows/ci.py"
    stage_name: "lint"
    required: true

  - name: "build"
    workflow_file: "workflows/build.py"
    stage_name: "build"
    arguments:
      target: "production"
    required: true

  - name: "security-scan"
    workflow_file: "workflows/security.py"
    stage_name: "scan"
    required: false
"""
