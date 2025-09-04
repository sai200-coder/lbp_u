# from service.helper import get_inputs
from service.config_service import collect_user_config
from service.config_service import create_transpiler_model
from service.config_service import create_modify_model
from service.config_service import create_upload_model
from service.analyzer_service import run_analyzer
from service.transpile_service import run_transpiler
from service.modify_service import run_modify_and_create_notebooks
from service.upload_service import UploadService
from service.run_service import RunService


def main():
    config1 = collect_user_config()
    # config = get_inputs()
    run_analyzer(config1["analyzer"])
    
    config2 = create_transpiler_model()
    
    run_transpiler(config2)
    
    modify_cfg = create_modify_model()
    run_modify_and_create_notebooks(modify_cfg)

    upload_cfg = create_upload_model()
    uploaded_paths = UploadService(upload_cfg).upload()
    # Notebooks are imported to Databricks workspace and optionally executed via CLI
    # when DATABRICKS_CLUSTER_ID is set. Local execution via RunService is skipped.
    
    #config3 = create_reconciler_model()
   # run_reconciler(config3.reconciler)
    #print(config.transpiler)
    #print(config.reconciler)
    
if __name__ == "__main__":
    main()
