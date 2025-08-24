import ast
from pathlib import Path

import pytest

# Extract the validate_sql_readonly function definition from the source file
# without importing its module (which requires heavy optional dependencies).
source_path = Path(__file__).resolve().parent.parent / "flashduck" / "utils.py"
source = source_path.read_text()
module = ast.parse(source)
func_node = next(
    node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "validate_sql_readonly"
)
func_code = ast.get_source_segment(source, func_node)
namespace = {}
exec(func_code, namespace)
validate_sql_readonly = namespace["validate_sql_readonly"]


def test_validate_sql_readonly_allows_select():
    """SELECT statements should be allowed."""
    assert validate_sql_readonly("SELECT 1") is None


def test_validate_sql_readonly_rejects_delete():
    """DELETE statements should raise an error."""
    with pytest.raises(ValueError):
        validate_sql_readonly("DELETE FROM t")
