include .makefile.env

PYTHON = python3
VENV = .venv
VENV_BIN = $(VENV)/bin
PYTHON_VENV= $(VENV_BIN)/python3

DOCKER_COMPOSE = ./docker/docker-compose.yml
DATA_INPUT_PATH=../../data-resample
TESTS_FOLDER = ./tests

REGION=asia-east1
CLOUD_RUN_IMAGE_NAME=cloud-run-batch-job
JOB_NAME=cloud-run-batch-job
TAG=v1.0

SERVICE_ACCOUNT=batch-job@$(PROJECT_ID).iam.gserviceaccount.com

setup_python_env:
	@echo "Create Python venv environment"
	@$(PYTHON) -m venv $(VENV)
	@$(VENV_BIN)/pip install -U pip
	@$(VENV_BIN)/pip install -r requirements.txt
	@echo "Complete"

setup_docker: 
	@docker-compose -f $(DOCKER_COMPOSE) build 
	@docker-compose -f $(DOCKER_COMPOSE) up  --wait

terraform_up: 
	@cd ./terraform; terraform init; terraform apply -var="project=$(PROJECT_ID)"

setup_infras: setup_docker terraform_up

setup: setup_python_env setup_infras

docker_up:
	@docker-compose -f $(DOCKER_COMPOSE) up  --wait

docker_down:
	@docker-compose -f $(DOCKER_COMPOSE) down

terraform_down: 
	@cd ./terraform; terraform destroy -var="project=$(PROJECT_ID)"

teardown: terraform_down
	@docker-compose -f $(DOCKER_COMPOSE) down -v --rmi all

snapshot_user_info: 
	@echo "Snapshoting user_info db onprem"
	@cd ./batch_job/onprem_batch_job; ../../$(PYTHON_VENV) ./snapshot_user_info.py
	@echo "Snapshoting Done"

upload_event:	
	@echo "Upload Event File"
	@cd ./batch_job/onprem_batch_job; ../../$(PYTHON_VENV) ./upload_event.py --input-path=$(DATA_INPUT_PATH)
	@echo "Upload Done"

build_cloud_image:
	@echo "Building Image for project_id: $(PROJECT_ID)"
	@cd ./batch_job/cloud_run_batch_job; \
		gcloud builds submit --config=cloudbuild.yaml \
            --substitutions=_PROJECT_ID=$(PROJECT_ID),_IMAGE_NAME=$(CLOUD_RUN_IMAGE_NAME),_TAG=$(TAG)

create_cloud_run_job: build_cloud_image
	@echo "Updated Cloud Run Job for project_id: $(PROJECT_ID)"
	@cd ./batch_job/cloud_run_batch_job; 
		gcloud run jobs update $(JOB_NAME) \
            --region asia-east1 \
            --image gcr.io/$(PROJECT_ID)/$(CLOUD_RUN_IMAGE_NAME):$(TAG) \
            --memory $(MEMORY) \
            --max-retries 1 \
            --service-account $(SERVICE_ACCOUNT)

trigger_cloud_run_job: 
	@echo "Trigger Cloud Run Job"
	@gcloud run jobs execute cloud-run-batch-job --region=$(REGION)

run: snapshot_user_info upload_event create_cloud_run_job trigger_cloud_run_job

unit_test: 
	@$(PYTHON_VENV) -m pytest $(TESTS_FOLDER)/unit

intergration_test: 
	@$(PYTHON_VENV) -m pytest $(TESTS_FOLDER)/intergration

validation: 
	@$(PYTHON_VENV) -m pytest $(TESTS_FOLDER)/validation

test: unit_test intergration_test validation

reset_environment: terraform_down terraform_up 

delete_cached: 
	@rm -rf $(VENV)
	@echo "Deleting __pycache__"
	@find . -type d -name "__pycache__" -exec rm -r {} +        
	@find . -type d -name ".pytest_cache" -exec rm -r {} +        
	@echo "Deleting terraform files"
	@find terraform -type f -name "terraform.tfstate*" -exec rm -r {} +   
	@find terraform -name ".terraform*" -exec rm -r {} +   


clean: teardown delete_cached
