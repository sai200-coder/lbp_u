import os
import nbformat
from nbformat.v4 import new_notebook, new_code_cell
from dotenv import load_dotenv
from groq import Groq
from models.modify_model import ModifyNotebookModel
from service.helper import get_catalog_name, get_schema_name
from datetime import datetime


def _clean_sql_output(text: str) -> str:
    """Normalize LLM output to plain SQL text.

    - Strips surrounding triple quotes
    - Removes markdown code fences (```sql ... ``` and ``` ... ```)
    - Removes leading descriptor lines like 'Modified SQL from:' if present
    """
    if not text:
        return ""

    cleaned = text.strip()

    # Remove surrounding triple quotes if present
    if cleaned.startswith('"""') and cleaned.endswith('"""'):
        cleaned = cleaned[3:-3].strip()

    # If content contains fenced code blocks, extract the first fenced block
    # Prefer ```sql fenced block; fall back to generic ```
    lines = cleaned.splitlines()
    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        fence = line.strip()
        if fence.startswith("```sql") or fence == "```":
            start_idx = i + 1
            break

    if start_idx is not None:
        for j in range(start_idx, len(lines)):
            if lines[j].strip() == "```":
                end_idx = j
                break
        if end_idx is not None and end_idx > start_idx:
            cleaned = "\n".join(lines[start_idx:end_idx]).strip()
        else:
            # No closing fence; take the rest
            cleaned = "\n".join(lines[start_idx:]).strip()
    else:
        # No fences; remove any leading descriptor line like '# Modified SQL from: ...'
        if lines and lines[0].lstrip().startswith(("# Modified SQL from:", "Modified SQL from:")):
            cleaned = "\n".join(lines[1:]).strip()

    return cleaned


def _split_sql_statements(sql_text: str) -> list:
    """Split SQL text into individual statements, keeping semicolons.

    This is a simple splitter that does not fully parse SQL, but works well for
    typical scripts generated here. It respects basic string literals to avoid
    splitting on semicolons inside single quotes.
    """
    statements = []
    current = []
    in_single_quote = False
    escape_next = False

    for char in sql_text:
        current.append(char)
        if escape_next:
            escape_next = False
            continue
        if char == "\\":
            escape_next = True
            continue
        if char == "'":
            in_single_quote = not in_single_quote
            continue
        if char == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []

    tail = "".join(current).strip()
    if tail:
        statements.append(tail if tail.endswith(";") else f"{tail};")

    return statements


def _classify_statement(statement: str) -> str:
    """Return one of: 'ddl', 'dml', 'select'."""
    stripped = statement.lstrip()
    # Remove leading SQL comments lines for classification
    while stripped.startswith("--"):
        newline_idx = stripped.find("\n")
        if newline_idx == -1:
            break
        stripped = stripped[newline_idx + 1:].lstrip()

    lowered = stripped.lower()
    if lowered.startswith("create ") or lowered.startswith("drop ") or lowered.startswith("alter "):
        return "ddl"
    if lowered.startswith("insert ") or lowered.startswith("update ") or lowered.startswith("merge ") or lowered.startswith("delete "):
        return "dml"
    if lowered.startswith("select ") or lowered.startswith("with "):
        return "select"
    # Default to DML if it's data-changing keywords appear inside; else select
    if any(k in lowered for k in [" insert ", " update ", " delete ", " merge "]):
        return "dml"
    return "select"


def _organize_sql_blocks(clean_sql: str) -> tuple:
    """Return (ddl_block, dml_block, select_block) as strings.

    Preserves original statement indentation/formatting.
    """
    statements = _split_sql_statements(clean_sql)
    ddl_stmts = []
    dml_stmts = []
    select_stmts = []

    for stmt in statements:
        kind = _classify_statement(stmt)
        if kind == "ddl":
            ddl_stmts.append(stmt.strip())
        elif kind == "dml":
            dml_stmts.append(stmt.strip())
        else:
            select_stmts.append(stmt.strip())

    ddl_block = "\n\n".join(ddl_stmts).strip()
    dml_block = "\n\n".join(dml_stmts).strip()
    select_block = "\n\n".join(select_stmts).strip()
    return ddl_block, dml_block, select_block


def _save_modified_sql_to_notebook(modified_sql: str, sql_filename: str, output_dir: str) -> None:

    notebook = new_notebook()
    clean_sql = _clean_sql_output(modified_sql)
    ddl_block, dml_block, select_block = _organize_sql_blocks(clean_sql)

    cells = []
    if ddl_block:
        cells.append(new_code_cell(ddl_block))
    if dml_block:
        cells.append(new_code_cell(dml_block))
    if select_block:
        cells.append(new_code_cell(select_block))

    # Always produce at least one cell to keep notebook valid
    notebook.cells = cells if cells else [new_code_cell("")]
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    notebook_filename = f"modified_{os.path.splitext(sql_filename)[0]}_{current_time}.ipynb"
    notebook_path = os.path.join(output_dir, notebook_filename)

    os.makedirs(output_dir, exist_ok=True)
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbformat.write(notebook, f)

    print(f"Notebook created: {notebook_path}")


def _save_modified_sql_to_file(modified_sql: str, sql_filename: str, output_dir: str) -> str:
    """Save cleaned SQL to a .sql file and return its path."""
    os.makedirs(output_dir, exist_ok=True)
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_out_filename = f"modified_{os.path.splitext(sql_filename)[0]}_{current_time}.sql"
    sql_out_path = os.path.join(output_dir, sql_out_filename)

    cleaned_sql = _clean_sql_output(modified_sql)
    ddl_block, dml_block, select_block = _organize_sql_blocks(cleaned_sql)

    # Join blocks with a clear blank line separation
    blocks = [b for b in [ddl_block, dml_block, select_block] if b]
    final_text = ("\n\n".join(blocks) + "\n") if blocks else "\n"

    with open(sql_out_path, 'w', encoding='utf-8') as f:
        f.write(final_text)

    print(f"SQL file created: {sql_out_path}")
    return sql_out_path


def run_modify_and_create_notebooks(cfg: ModifyNotebookModel) -> None:

    load_dotenv()
    client = Groq()

    catalog_name = get_catalog_name()
    schema_name = get_schema_name(catalog_name)

    transpiled_dir = cfg.transpiled_dir
    output_dir = cfg.output_dir

    if not os.path.exists(transpiled_dir):
        print(f"Directory '{transpiled_dir}' not found!")
        return

    sql_files = [f for f in os.listdir(transpiled_dir) if f.endswith('.sql')]
    if not sql_files:
        print("No SQL files found in transpiled directory.")
        return

    print(f"Found {len(sql_files)} SQL files to process:")
    for sql_file in sql_files:
        print(f"Processing {sql_file}...")
        try:
            file_path = os.path.join(transpiled_dir, sql_file)
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            prompt = f"""
You are given SQL code that was transpiled using LakeBridge.

Task:
- Modify all table references to use the full three-level namespace format: catalog.schema.table
- If the table is referred to as just "table" or "schema.table", expand it to "catalog.schema.table"
- Use the following defaults where not specified:
  - catalog: {catalog_name}
  - schema: {schema_name}
- Do not explain anything or return extra text. Only return the modified SQL script.

Here is the SQL code:
{sql_content}
"""

            completion = client.chat.completions.create(
                model=cfg.llm_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=cfg.temperature,
                max_completion_tokens=4096,
                top_p=0.95,
                stream=False,
                stop=None,
            )

            modified_sql = completion.choices[0].message.content
            # Save as plain .sql file and as a cleaned notebook for optional review
            _save_modified_sql_to_file(modified_sql, sql_file, output_dir)
            _save_modified_sql_to_notebook(modified_sql, sql_file, output_dir)
            print(f"Successfully processed {sql_file}")
            print("-" * 50)
        except Exception as e:
            print(f"Error processing {sql_file}: {str(e)}")
            print("-" * 50)

    print("Processing complete!")


