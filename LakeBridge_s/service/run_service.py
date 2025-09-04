import os
from nbconvert import NotebookExporter, HTMLExporter
from nbconvert.preprocessors import ExecutePreprocessor
import nbformat
from models.run_model import RunModel


class RunService:
    def __init__(self, model: RunModel) -> None:
        self.model = model

    def run(self) -> str:
        notebook_path = self.model.get_notebook_path()
        if not os.path.isfile(notebook_path):
            raise FileNotFoundError(f"Notebook not found: {notebook_path}")

        with open(notebook_path, "r", encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)

        executor = ExecutePreprocessor(timeout=600, kernel_name="python3")
        executor.preprocess(nb, {"metadata": {"path": os.path.dirname(notebook_path) or "."}})

        base, ext = os.path.splitext(notebook_path)
        executed_path = f"{base}_executed{ext}"
        with open(executed_path, "w", encoding="utf-8") as f:
            nbformat.write(nb, f)

        print(f"Notebook executed successfully: {executed_path}")
        return executed_path


