from service.helper import (
    get_source_directory, get_report_file, get_source_tech,
    get_input_source, get_output_folder, get_error_file_path,
    get_catalog_name, get_schema_name, get_source_dialect,
    get_profile_name, get_target,get_validate, get_warehouse, get_override,
    get_open_config
)

from models.analyzer_model import AnalyzerModel
from models.transpile_model import TranspilerModel
from models.reconcile_model import ReconcilerModel
from models.full_config import FullConfigModel
from models.modify_model import ModifyNotebookModel
from models.upload_model import UploadModel
from models.run_model import RunModel


def collect_user_config() -> FullConfigModel:
    analyzer = AnalyzerModel(
        source_directory=get_source_directory(),
        report_file=get_report_file(),
        source_tech=get_source_tech()
    )
    return {"analyzer": analyzer}
    
def create_transpiler_model() ->  FullConfigModel:
    catalog = get_catalog_name()
    return TranspilerModel(
        source_dialect=get_source_dialect(),
        input_source=get_input_source(),
        output_folder=get_output_folder(),
        error_file_path=get_error_file_path(),
        catalog_name=catalog,
        schema_name=get_schema_name(catalog),
        validate=get_validate(),
        warehouse=get_warehouse(),
        override=get_override(),
        open_config=get_open_config()
        )
    
    reconciler = ReconcilerModel(
        profile_name=get_profile_name(),
        target=get_target()
    )

    return FullConfigModel(
        analyzer=analyzer,
        transpiler=transpiler,
        reconciler=reconciler
    )

def create_modify_model() -> ModifyNotebookModel:
    transpiled_dir = input("Enter transpiled SQL directory [default: ./transpiled]: ").strip()
    if not transpiled_dir:
        transpiled_dir = "./transpiled"

    output_dir = input("Enter output directory for notebooks [default: ./transpiled]: ").strip()
    if not output_dir:
        output_dir = "./transpiled"

    llm_model = input("Enter LLM model name [default: llama-3.1-8b-instant]: ").strip()
    if not llm_model:
        llm_model = "llama-3.1-8b-instant"

    temperature_str = input("Enter temperature (0.0 - 1.0) [default: 0.6]: ").strip()
    temperature = 0.6 if not temperature_str else float(temperature_str)

    return ModifyNotebookModel(
        transpiled_dir=transpiled_dir,
        output_dir=output_dir,
        llm_model=llm_model,
        temperature=temperature,
    )

def create_upload_model() -> UploadModel:
    source_notebook_path = input("Enter path to local file or directory to upload: ").strip()
    destination_directory = input("Enter Databricks workspace directory (e.g., /Users/you/project): ").strip()
    return UploadModel(
        source_notebook_path=source_notebook_path,
        destination_directory=destination_directory,
    )

def create_run_model(uploaded_notebook_path: str) -> RunModel:
    return RunModel(notebook_path=uploaded_notebook_path)
