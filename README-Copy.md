# Data Assessment  Tool

## Setup development environment with data generated from synthea
1. Clone the repo https://github.com/DigitalHealthIntegration/hapi-fhir-jpaserver-starter `git clone https://github.com/DigitalHealthIntegration/hapi-fhir-jpaserver-starter`
2. Checkout the branch ss/synthea `git checkout -b ss/synthea`
3. Follow the [README](https://github.com/DigitalHealthIntegration/hapi-fhir-jpaserver-starter/blob/ss/synthea/README.md)

## Steps to run v1 of the data assessment tool in your env.

1. Clone the repo https://github.com/DigitalHealthIntegration/ETL-German-FHIR-Core
2. Download [OMOP vocabulary](https://www.dropbox.com/s/2f1xg20yjbiup27/2023-01-01-vocabulary_download_v5_%7Bd496576d-0027-4564-b598-491e7f1ac26f%7D_1672620434076.zip?dl=1) and unzip in ETL-German-FHIR-Core folder.
3. Change directory to deploy. eg: cd deploy
4. Rename sample.env to .env
5. Modify the following properties in .env file as per your setup
    ````
    DATA_FHIRSERVER_BASEURL=http://localhost:8080/fhir 
    DATA_FHIRSERVER_USERNAME=username
    DATA_FHIRSERVER_PASSWORD=password
    DATA_FHIRSERVER_TOKEN=token
    
    // Use begin date and end date to restrict the amount of data fetched. It's currently set to a default value of 
    DATA_BEGINDATE=1800-01-01
    DATA_ENDDATE=2099-12-31
    ````
6. Run `docker network create cloudbuild` . The important part here is to make sure the omop conversion application is on the same network as the fhir server to detect it.
7. Run `docker compose up -d`
8. Open a browser and login to http://localhost:8888
8. Open the jupyter notebook named query/omop_queries.ipynb. The token can be found in docker-compose.yml. You can change it as per your need.
9. Run the notebook after data is uploaded to omop from fhir.