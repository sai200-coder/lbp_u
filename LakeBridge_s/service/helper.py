# from models.test_model import TestModel

from models.analyzer_model import AnalyzerModel
from models.transpile_model import TranspilerModel
from models.reconcile_model import ReconcilerModel
import subprocess


def get_source_directory():
    while True:
        source_directory = input("Enter your source directory path (e.g., /path/to/source): ")
        if source_directory:
            return source_directory
        else:
            print("Source directory cannot be empty. Please try again.")

def get_report_file():
    while True:
        report_file = input("Enter your report file name (e.g., analysis_report.xlsx): ")
        if report_file.endswith('.xlsx'):
            return report_file
        else:
            print("Invalid file format. Please enter a valid .xlsx file.")
            
def get_source_tech():
    valid_options = {
        "0": "abinitio",
        "1": "adf",
        "2": "alteryx",
        "3": "athena",
        "4": "bigquery",
        "5": "bods",
        "6": "cloudera-impala",
        "7": "datastage",
        "8": "greenplum",
        "9": "hive",
        "10": "ibmdb2",
        "11": "informatica-bde",         
        "12": "informatica-pc",
        "13": "informatica-cloud",
        "14": "MS SQL Server",
        "15": "netezza",
        "16": "oozie",
        "17": "oracle",
        "18": "odi",                     
        "19": "pentahodi",
        "20": "pig",
        "21": "presto",
        "22": "pyspark",
        "23": "redshift",
        "24": "saphana-calcviews",
        "25": "sas",
        "26": "snowflake",
        "27": "spss",
        "28": "sqoop",
        "29": "ssis",
        "30": "ssrs",
        "31": "synapse",
        "32": "talend",
        "33": "teradata",
        "34": "vertica",
    }

    print("Select the source technology:")
    for k, v in valid_options.items():
        print(f"[{k}] {v}")

    choice = input("Enter a number between 0 and 34: ").strip()
    if choice in valid_options:
        return valid_options[choice]
    else:
        print(f"Invalid choice. Valid numbers are 0–34.")
        return get_source_tech()


# transpiler config
def get_input_source():
    while True:
        val = input("Enter your input source directory path (e.g., ./sql_scripts): ")
        if val:
            return val
        else:
            print("Input source cannot be empty. Please try again.")

def get_output_folder():
    while True:
        val = input("Enter your output folder path (e.g., ./transpiled_code): ")
        if val:
            return val
        else:
            print("Output folder cannot be empty. Please try again.")

def get_error_file_path():
    while True:
        val = input("Enter your error file path (e.g., ./errors.log): ")
        if val:
            return val
        else:
            print("Error file path cannot be empty. Please try again.")

def get_catalog_name():
    while True:
        val = input("Enter your catalog name (e.g., my_catalog): ").strip()
        if not val:
            print("Catalog name cannot be empty. Please try again.")
            continue
            
        # Validate catalog exists in Databricks
        try:
            result = subprocess.run(
                ['databricks', 'catalogs', 'list'],
                capture_output=True, text=True, check=True
            )
            # Parse output: skip header lines and get first column
            catalogs = []
            for line in result.stdout.splitlines():
                if line.startswith('---') or not line.strip():
                    continue
                parts = line.split()
                if parts:
                    catalogs.append(parts[0])
                    
            if val in catalogs:
                return val
            else:
                available = ', '.join(catalogs) if catalogs else 'none'
                print(f"Catalog '{val}' not found. Available catalogs: {available}")
        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to list catalogs. Details: {e.stderr}")

def get_schema_name(catalog_name):
    while True:
        val = input(f"Enter your schema name in catalog '{catalog_name}' (e.g., my_schema): ").strip()
        if not val:
            print("Schema name cannot be empty. Please try again.")
            continue
            
        # Validate schema exists in the catalog
        try:
            result = subprocess.run(
                ['databricks','schemas','list',catalog_name],
                capture_output=True, text=True, check=True
            )
            schemas = [line.split(maxsplit=1)[0].replace(f'{catalog_name}.', '') for line in result.stdout.splitlines()[1:] if line.strip()]
            if val in schemas:
                return val
            else:
                print(f"Schema '{val}' not found in catalog '{catalog_name}'. Please try again. Available are '{schemas}'")
        except subprocess.CalledProcessError:
            print("Error: Failed to list schemas. Check catalog name or Databricks CLI configuration.")
def get_validate():
    while True:
        val = input("Do you want to validate the transpiled code? (true/false) [default:  ]: ").strip().lower()
        if val in ["true", "false", ""]:
            return val if val else "false"
        else:
            print("Invalid input. Please enter 'true' or 'false'.")
def get_warehouse():
    while True:
        val = input("Enter your warehouse ID (e.g., 0123456789abcde0) [default: 1]: ").strip()
        if val == "":
            return "1"
            
        # Validate warehouse exists
        try:
            result = subprocess.run(
                ['databricks','warehouses', 'list'],
                capture_output=True, text=True, check=True
            )
            # Parse output: skip headers and get first column
            warehouses = []
            for line in result.stdout.splitlines():
                if line.startswith('---') or not line.strip() or line.startswith('ID '):
                    continue
                parts = line.split()
                if parts:
                    warehouses.append(parts[0])
                    
            if val in warehouses:
                return val
            else:
                available = ', '.join(warehouses) if warehouses else 'none'
                print(f"Warehouse ID '{val}' not found. Available warehouses: {available}")
        except subprocess.CalledProcessError as e:
            print(f"Error: Failed to list warehouses. Details: {e.stderr}")
def get_override():
    while True:
        val = input("Do you want to override existing files? (yes/no) [default: yes]: ").strip().lower()
        if val in ["yes", "no", ""]:
            return val if val else "yes"
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
def get_open_config():
    while True:
        val = input("Do you want to open the config file after generation? (yes/no) [default: yes]: ").strip().lower()
        if val in ["yes", "no", ""]:
            return val if val else "yes"
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
            

def get_source_dialect():
    dialects = {
        "0": "Set it later",
        "1": "datastage",
        "2": "informatica (desktop edition)",
        "3": "informatica cloud",
        "4": "mssql",
        "5": "netezza",
        "6": "oracle",
        "7": "snowflake",
        "8": "synapse",
        "9": "teradata",
        "10": "tsql"
    }

    while True:
        print("\nSelect the source dialect:")
        for key, value in dialects.items():
            print(f"[{key}] {value}")

        choice = input("Enter a number between 0 and 10: ").strip()

        if choice in dialects:
            return dialects[choice]   
        else:
            print("Invalid choice. Please enter a number between 0 and 10.")






# reconciler config
def get_profile_name():
    while True:
        val = input("Enter your profile name (e.g., my_profile): ")
        if val:
            return val
        else:
            print("Profile name cannot be empty. Please try again.")

def get_target():
    while True:
        val = input("Enter your target (e.g., dev): ")
        if val:
            return val
        else:
            print("Target cannot be empty. Please try again.")