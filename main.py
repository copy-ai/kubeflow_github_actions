import os
import sys
import json
import yaml
import logging
import kfp
import kfp.compiler as compiler
import kfp_server_api
import importlib.util
from typing import Optional
from dotenv import load_dotenv


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# TODO: Remove after version up to kfp=v1.1
_FILTER_OPERATIONS = {"UNKNOWN": 0,
                      "EQUALS": 1,
                      "NOT_EQUALS": 2,
                      "GREATER_THAN": 3,
                      "GREATER_THAN_EQUALS": 5,
                      "LESS_THAN": 6,
                      "LESS_THAN_EQUALS": 7}


# TODO: Remove after version up to kfp=v1.1
def get_pipeline_id(self, name):
    """Find the id of a pipeline by name.

    Args:
        name: Pipeline name.

    Returns:
        Returns the pipeline id if a pipeline with the name exists.
    """
    pipeline_filter = json.dumps({
        "predicates": [
            {
                "op": _FILTER_OPERATIONS["EQUALS"],
                "key": "name",
                "stringValue": name,
            }
        ]
    })
    result = self._pipelines_api.list_pipelines(filter=pipeline_filter)
    if result.pipelines is None:
        return None
    if len(result.pipelines) == 1:
        return result.pipelines[0].id
    elif len(result.pipelines) > 1:
        raise ValueError(
            "Multiple pipelines with the name: {} found, the name needs to be unique".format(name))
    return None


# TODO: Remove after version up to kfp=v1.1
def upload_pipeline_version(
        self,
        pipeline_package_path,
        pipeline_version_name: str,
        pipeline_id: Optional[str] = None,
        pipeline_name: Optional[str] = None):
    """Uploads a new version of the pipeline to the Kubeflow Pipelines cluster.
    Args:
        pipeline_package_path: Local path to the pipeline package.
        pipeline_version_name:  Name of the pipeline version to be shown in the UI.
        pipeline_id: Optional. Id of the pipeline.
        pipeline_name: Optional. Name of the pipeline.
    Returns:
        Server response object containing pipleine id and other information.
    Throws:
        ValueError when none or both of pipeline_id or pipeline_name are specified
        Exception if pipeline id is not found.
    """

    if all([pipeline_id, pipeline_name]) or not any([pipeline_id, pipeline_name]):
        raise ValueError('Either pipeline_id or pipeline_name is required')

    if pipeline_name:
        pipeline_id = self.get_pipeline_id(pipeline_name)

    response = self._upload_api.upload_pipeline_version(
        pipeline_package_path,
        name=pipeline_version_name,
        pipelineid=pipeline_id
    )

    if self._is_ipython():
        import IPython
        html = 'Pipeline link <a href=%s/#/pipelines/details/%s>here</a>' % (
            self._get_url_prefix(), response.id)
        IPython.display.display(IPython.display.HTML(html))
    return response


def load_function(pipeline_function_name: str, full_path_to_pipeline: str) -> object:
    """Function to load python function from filepath and filename

    Arguments:
        pipeline_function_name {str} -- The name of the pipeline function
        full_path_to_pipeline {str} -- The full path name including the filename of the python file that 
                                        describes the pipeline you want to run on Kubeflow

    Returns:
        object -- [description]
    """
    logging.info(
        f"Loading the pipeline function from: {full_path_to_pipeline}")
    logging.info(
        f"The name of the pipeline function is: {pipeline_function_name}")
    spec = importlib.util.spec_from_file_location(pipeline_function_name,
                                                  full_path_to_pipeline)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    pipeline_func = getattr(foo, pipeline_function_name)
    logging.info("Succesfully loaded the pipeline function.")
    return pipeline_func


def pipeline_compile(pipeline_function: object) -> str:
    """Function to compile pipeline. The pipeline is compiled to a zip file. 

    Arguments:
        pipeline_func {object} -- The kubeflow pipeline function

    Returns:
        str -- The name of the compiled kubeflow pipeline
    """
    pipeline_name_zip = pipeline_function.__name__ + ".zip"
    compiler.Compiler().compile(pipeline_function, pipeline_name_zip)
    logging.info("The pipeline function is compiled.")
    return pipeline_name_zip


def upload_pipeline(pipeline_name_zip: str, pipeline_name: str, github_sha: str, client):
    """ Function to upload pipeline to kubeflow.

    Arguments:
        pipeline_name_zip {str} -- The name of the compiled pipeline.ArithmeticError
        pipeline_name {str} -- The name of the pipeline function. This will be the name in the kubeflow UI. 
    """
    pipeline_id = client.get_pipeline_id(pipeline_name)
    if pipeline_id is None:
        pipeline_id = client.upload_pipeline(
            pipeline_package_path=pipeline_name_zip,
            pipeline_name=pipeline_name).to_dict()["id"]

    client.upload_pipeline_version(
        pipeline_package_path=pipeline_name_zip,
        pipeline_version_name=github_sha,
        pipeline_id=pipeline_id)

    return pipeline_id


def generate_random_string(length: int) -> str:
    import random
    import string
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string


def read_pipeline_params(pipeline_parameters_path: str) -> dict:
    # [TODO] add docstring here
    pipeline_params = {}
    with open(pipeline_parameters_path) as f:
        try:
            pipeline_params = yaml.safe_load(f)
            logging.info(f"The pipeline parameters is: {pipeline_params}")
        except yaml.YAMLError as exc:
            logging.info("The yaml parameters could not be loaded correctly.")
            raise ValueError(
                f"The yaml parameters could not be loaded correctly with {exc}.")
        logging.info(f"The paramters are: {pipeline_params}")
    return pipeline_params


def disable_previous_recurring_runs(client: kfp.Client,
                                    experiment_name: str):
    experiment = client.get_experiment(experiment_name=experiment_name)
    
    experiment_id = experiment.to_dict()["id"]
    logging.info(f"experiment_id is {experiment_id}")

    # Get a list of all recurring runs (jobs), filtered for this experiment
    logging.info(f"Querying for jobs associated with experiment_id {experiment_id}")
    jobs_obj = client._job_api.list_jobs(page_size=350,
                                         resource_reference_key_type="EXPERIMENT",
                                         resource_reference_key_id=experiment_id,
                                         )
    jobs = jobs_obj.to_dict()['jobs']
    logging.info(f"There are {len(jobs)} jobs")
    
    # Disable every job in this experiment if it is Enabled
    for job in jobs:
        logging.info(job)
        job_id = job['id']
        job_status = job['status']
        logging.info(f"job_id is {job_id} and status is {job_status}")
        if job['status'] == 'Enabled':
            try:
                logging.info(f"Disabling job {job_id}")
                client._job_api.disable_job(job_id)
            except Exception as e:
                logging.error(f"Failed to disable job {job_id}")
                logging.error(e)
                continue


def terminate_existing_runs(client: kfp.Client,
                            experiment_name: str):
    experiment = client.get_experiment(experiment_name=experiment_name)
    
    experiment_id = experiment.to_dict()["id"]
    logging.info(f"experiment_id is {experiment_id}")
    
    # Get a list of all the runs in the experiment
    logging.info(f"Querying for runs associated with experiment_id {experiment_id}")
    runs_obj = client.list_runs(experiment_id=experiment_id, page_size=350)
    runs = runs_obj.to_dict()['runs']

    # Extract the IDs of the runs
    run_ids = [r["id"] for r in runs]
    logging.info(f"There are {len(run_ids)} runs")

    # Terminate any existing runs
    for run_id in run_ids:
        try:
            logging.info(f"Terminating run {run_id}")
            client._run_api.terminate_run(run_id=run_id)
        except kfp_server_api.exceptions.ApiException as e:
            logging.error(f"Failed to terminate run {run_id}")
            continue
            

def get_experiment_id(client: kfp.Client,
                      experiment_name: str) -> str:
    try:
        experiment_id = client.get_experiment(
            experiment_name=experiment_name
        ).to_dict()["id"]
    except ValueError:
        experiment_id = client.create_experiment(
            name=experiment_name
        ).to_dict()["id"]
    
    return experiment_id


def run_pipeline_func(client: kfp.Client,
                      pipeline_name: str,
                      github_sha: str,
                      pipeline_id: str,
                      pipeline_parameters_path: dict,
                      recurring_flag: bool = False,
                      cron_exp: str = ''):
    pipeline_params = read_pipeline_params(
        pipeline_parameters_path=pipeline_parameters_path)
    pipeline_params = pipeline_params if pipeline_params is not None else {}
    
    experiment_name = os.environ["INPUT_EXPERIMENT_NAME"]
    experiment_id = get_experiment_id(client=client, experiment_name=experiment_name)

    job_name = f"{pipeline_name}_{github_sha}"

    logging.info(f"experiment_id: {experiment_id}, \
                 job_name: {job_name}, \
                 pipeline_params: {pipeline_params}, \
                 pipeline_id: {pipeline_id}, \
                 cron_exp: {cron_exp}")

    if recurring_flag == "True":
        client.create_recurring_run(experiment_id=experiment_id,
                                    job_name=job_name,
                                    params=pipeline_params,
                                    pipeline_id=pipeline_id,
                                    cron_expression=cron_exp)
        logging.info(
            "Successfully started the recurring pipeline, head over to kubeflow to check it out")

    client.run_pipeline(experiment_id=experiment_id,
                        job_name=job_name,
                        params=pipeline_params,
                        pipeline_id=pipeline_id)
    logging.info(
        "Successfully started the pipeline, head over to kubeflow to check it out")


def main():

    logging.info(
        "Started the process to compile and upload the pipeline to kubeflow.")
    logging.info("Authenticating")

    ga_credentials = os.environ["INPUT_GOOGLE_APPLICATION_CREDENTIALS"]
    with open(ga_credentials) as f:
        sa_details = json.load(f)
    logging.info(f"service account email is {sa_details['client_email']}")
    os.system("gcloud auth activate-service-account {} --key-file={} --project={}".format(sa_details['client_email'],
                                                                                          ga_credentials,
                                                                                          sa_details['project_id']))
    logging.info("Logged in!")

    # # TODO: Remove after version up to kfp=v1.1
    # kfp.Client.get_pipeline_id = get_pipeline_id
    # kfp.Client.upload_pipeline_version = upload_pipeline_version

    client = kfp.Client(host=os.environ['INPUT_KUBEFLOW_URL'])

    experiment_name = os.environ["INPUT_EXPERIMENT_NAME"]
    logging.info(f"Experiment_name is {experiment_name}")

    # Disable previous recurring runs (jobs) and terminate existing runs
    disable_previous_recurring_runs(client=client, experiment_name=experiment_name)
    terminate_existing_runs(client=client, experiment_name=experiment_name)

    pipeline_function_name = os.environ['INPUT_PIPELINE_FUNCTION_NAME']
    pipeline_name = os.environ.get('INPUT_PIPELINE_NAME', pipeline_function_name)  # Default to pipeline function name if not found
    pipeline_function = load_function(pipeline_function_name=pipeline_function_name,
                                      full_path_to_pipeline=os.environ['INPUT_PIPELINE_CODE_PATH'])

    github_sha = os.environ.get("GITHUB_SHA", generate_random_string(25))
    logging.info(f"Tagging with pipeline version with {github_sha}")
    if os.environ["INPUT_VERSION_GITHUB_SHA"] == "True":
        logging.info(f"Versioned pipeline components with : {github_sha}")
        pipeline_function = pipeline_function(github_sha=github_sha)



    logging.info(f"Compiling pipeline {pipeline_name}")
    pipeline_name_zip = pipeline_compile(pipeline_function=pipeline_function)
    logging.info(f"Uploading pipeline {pipeline_name}")
    pipeline_id = upload_pipeline(pipeline_name_zip=pipeline_name_zip,
                                  pipeline_name=pipeline_name,
                                  github_sha=github_sha,
                                  client=client)
    logging.info(f"Finished uploading pipeline {pipeline_name}")

    if os.getenv("INPUT_RUN_PIPELINE") == "True":
        logging.info("Started the process to run the pipeline on kubeflow.")
        run_pipeline_func(pipeline_name=pipeline_name,
                          github_sha=github_sha,
                          pipeline_id=pipeline_id,
                          client=client,
                          pipeline_parameters_path=os.environ["INPUT_PIPELINE_PARAMETERS_PATH"],
                          recurring_flag=os.environ['INPUT_RUN_RECURRING_PIPELINE'],
                          cron_exp=os.environ['INPUT_CRON_EXPRESSION'])


if __name__ == "__main__":
    load_dotenv()
    main()
