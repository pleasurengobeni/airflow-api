
# DAG BUILDER | CONFIG | DYNAMIC DAGS:
This Airflow DAG is designed to dynamically generate and manage multiple ETL (Extract, Transform, Load) DAGs based on configuration files stored in a specific folder.
The primary goal of this DAG is to automate the generation of ETL DAGs based on dynamic configurations. Each configuration specifies an indicator_name and indicator_code, and the DAG uses these to create customized ETL DAG files. This setup can be particularly useful for handling large numbers of similar ETL workflows that differ only by the indicator_name and indicator_code, allowing them to be managed more efficiently.


# ETL DAG:
DAG automates the process of fetching data from an API, transforming it according to specific column mappings, and storing the results in a PostgreSQL database. The workflow includes checks for table existence, dynamic schema creation, handling of date ranges for querying, and inserting data efficiently while avoiding duplicates. The usage of Parquet files as an intermediate storage format optimizes for larger data sets and facilitates easy data retrieval later on.

# Main Workflow:
The main workflow in the DAG follows this sequence:
Test the API and DB connections.
Check if the target table exists.
If not, create the table.
Get the maximum date from the previous DAG execution (or use a start date).
Query the API for data based on the date range.
Store the data in a Parquet file.
Upload the Parquet file to the PostgreSQL database (via staging to avoid reinserting same data on to target table).


## Bonus Points:

#[ ] The pipeline can be stopped and resumed. `red:Airflow allows this`
#[ ] The ability to re-run, only fetch the most recent data.`red:incremental loads, using tracking table`
#[ ] The E of ETL can be stopped and resumed without need to restart the data pull from scratch. `red:tasks can continue from where they left off`
#[ ] Any form of tests (unit/functional): `red:Github pipeline test`
#[ ] Additional suggestions if you had more time: `red:I think I over did it`


# FILE STRUCTURE:
```
repo/
├── .github/                               # GitHub-specific configuration for CI/CD workflows
│   └── workflows/                         # Contains GitHub Actions workflows
│       └── test.yaml                      # CI/CD pipeline file to run tests on new commits/pull requests
├── dags/                                  # Main directory for Airflow DAGs and related files
│   ├── config/                            # Holds JSON configuration files that define ETL jobs
│   │   ├── indicator_name_indicator_code1.json  # Configuration file for a specific ETL job
│   │   └── indicator_name_indicator_code2.json  # Another configuration file for an ETL job
│   ├── custom_dags/                       # Custom Airflow DAGs or Python scripts to manage DAGs
│   │   └── dag_builder.py                 # Script to dynamically generate DAGs from the JSON configurations
│   ├── template/                          # Holds template files for generating DAGs
│   │   └── etl_template.py.jinja          # Jinja template for ETL DAG structure
│   ├── etl/                               # Directory for storing generated DAG files (created by `dag_builder.py`)
│   │   ├── indicator_name_indicator_code_ETL_1.py   # Generated DAG file for specific ETL job
│   │   └── indicator_name_indicator_code_ETL_2.py   # Another generated DAG file
│   └── tests/                             # Contains Python test files to validate DAG generation
│       └── test_dag_generation.py         # Test file for ensuring that DAGs are correctly generated from config
├── .env                                   # Environment file to store sensitive or configuration variables
├── requirements.txt                       # Lists the dependencies required to run the project
├── Dockerfile                             # Defines the Docker image for the project
├── docker-compose.yaml                    # Docker Compose file for orchestrating multi-container environments (e.g., Airflow, Postgres)
├── datalake_init.sh                       # Shell script to initialize your data lake or related infrastructure
└── README.md                              # Project documentation and instructions
```

# DEVOPS | CICD | TESTING:
Version Control: Code and configurations are versioned and stored in GitHub.
CI/CD Pipeline: GitHub Actions automates testing, linting, and deployment to Airflow. I have a crontab job in the server that does git pulls every minute
Testing: Automated tests ensure that generated DAG files are correct and functional. simple tests to demonstrate potential testing via Github
Deployment: Once the code passes the CI pipeline, changes are deployed to the Airflow instance. Changes automatically reflect 


# DEMO APPLICATIONS
- Apache Airflow: http://44.208.179.89:8080/home | username: admin | password: admin.123

- PgAdmin (Target DB): http://44.208.179.89:8081/ | username: ngobeni.pleasure@gmail.com | password: admin.123

- Metabase: http://44.208.179.89:3010/ | username: ngobeni.pleasure@gmail.com | password: admin.123

- Github Repository: https://github.com/pleasurengobeni/airflow-api

# LINUX UBUNTU SERVER 24.04 INSTALLATION



```
#Make key unreadable
	chmod 400 /path/to/airflow-api.pem

#SSH into the server
	ssh -i "/path/to/airflow-api.pem" ubuntu@ec2-44-208-179-89.compute-1.amazonaws.com

#Create AIrflow Dir in the server
	mkdir airflow
	cd airflow

#Create SSH key for GitHub (Press enter to save when prompted)
	ssh-keygen -t ed25519 -C "ngobeni.pleasure@gmail.com"

#start ssh agent 
	eval "$(ssh-agent -s)"

#Add key to ssh agent
	ssh-add ~/.ssh/id_ed25519

#Copy key to clipboard
	cat ~/.ssh/id_ed25519.pub

#Add the SSH Key to GitHub
	Go to GitHub's SSH and GPG keys settings page.
	Click New SSH key.
	In the "Title" field, add a name for the key (e.g., "My Work Laptop").
	Paste your public key into the "Key" field.
	Click Add SSH key.

#Test SSH connection
	ssh -T git@github.com

	Expected output
	Hi username! You've successfully authenticated, but GitHub does not provide shell access.


#Clone Project (must be inside airflow directory)
	git clone -b dev git@github.com:pleasurengobeni/airflow-api.git .


#Install Docker

	sudo apt-get update
	sudo apt-get install apt-transport-https ca-certificates curl software-properties-common
	sudo apt  install docker-compose
	curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
	sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
	sudo apt-get update
	sudo apt-get install docker-ce
	sudo usermod -aG docker ubuntu
	sudo systemctl enable docker
	sudo systemctl start docker
	sudo usermod -aG docker $USER
	newgrp docker

#If docker fails to start
	sudo systemctl restart docker.service
	systemctl status docker.service


#See directory list (airflow)
	ls -l

#Setup your environmental variables (save the following to ~/.bashrc)

	nano ~/.bashrc #use your preferred editor Vim etc

	copy and paste on the last line on bashrc

#----start bashrc-------
#Airflow Database
export AIRFLOW_DB_USER=airflow
export AIRFLOW_DB_PASSWORD=airflow
export AIRFLOW_DB=airflow

#Datalake Database
export DATALAKE_DB_USER=admin
export DATALAKE_DB_PASSWORD=admin.12345
export DATALAKE_DB=central_db
export DATALAKE_PORT=5433

#Airflow Configuration
export FERNET_KEY="Fgwo9x0FeuuOebVEddo19SCDBW5xsQDo-Kpzj7ywwNg="
export AIRFLOW_WEB_USER=admin
export AIRFLOW_WEB_PWD=admin.123
export AIRFLOW_VAR_ENVIRONMENT=dev
export AIRFLOW__CORE__MAX_LOG_FILE_SIZE=104857600  # 100 MB


#Host User for File Permissions
export HOST_UID=1000
export HOST_GID=0

#Connections
export AIRFLOW_CONN_DATA_LAKE=postgresql://admin:admin.12345@datalake_postgres:5432/central_db

#PgAdmin
export PGADMIN_DEFAULT_EMAIL=example@gmail.com
export PGADMIN_DEFAULT_PASSWORD=admin.123

#----end bashrc-----------


#save your bashrc 
	control + X , enter y and press enter

#Apply changes to the system
	source ~/.bashrc

#Solve permission issues

	sudo chown -R $(id -u):0 ~/airflow
	sudo chmod -R g+rwx ~/airflow/
	git config core.fileMode false




#Starting the ETL airflow application (Next steps may be long due to resolving errors, which will be caused by permissions, follow each error and resource
#servers are different and have different settings)
#This will launch 4 containers
	#postgres for airflow
	#postgres for our data storage
	#airflow init (this will run initialise evrything needed then it stops)
	#airflow scheduler (depends on airflow init when it exit without errors)
	#airflow webserver (depends on airflow init when it exit without errors)

#Launch docker compose
	docker compose up --build #(use this the first time you start the applications) or 
	docker compose up -d #(use this to run docker in the background)

#If docker compose does not start run  (it allows running docker without sudo command)
	sudo usermod -aG docker $USER
	newgrp docker

	docker compose up --build #(use this the first time you start the applications) or 
	docker compose up -d #(use this to run docker in the background)


When docker is running successfully, you will start seeing a lot of logs output 
containers will throw errors if any take note of them and try using ChatGPT to resolve them or contact me for assistance

#when you resolve errors, it is best to stop the containers and starting them to apply new changes
	docker compose down #stop containers
	docker compose up #start containers

#If you feel you want to remove everything and start from scratch
#this will delete all the containers and docker images

 	docker stop $(docker ps -aq)
 	docker rm $(docker ps -aq)
 	docker rmi $(docker images -q)
 	docker volume rm $(docker volume ls -q)


#Cheap CICD :-D
#create a crontab job in the server to keep on doing git pulls every minute

	crontab -e #(firstime running this it will ask which editor you prefer)

	#copy paste this on the last line
	* * * * * cd ~/airflow && git pull


This is extra but to allow inbound connection I set the following
sudo iptables -A INPUT -p tcp --dport 8080 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 8081 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 5432 -j ACCEPT


#Common issues

	#Airflow webserver runs before airflow init finishes, and webserver fails to continue this require stopping and starting docker compose
	#stop when init has finised with exit code 
```

















