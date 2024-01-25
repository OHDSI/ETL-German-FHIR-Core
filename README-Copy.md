# Data Assessment  Tool

## Steps to run v1 of the data assessment tool in your env.

1. Clone the repo https://github.com/DigitalHealthIntegration/ETL-German-FHIR-Core
2. Download [OMOP vocabulary](https://www.dropbox.com/s/2f1xg20yjbiup27/2023-01-01-vocabulary_download_v5_%7Bd496576d-0027-4564-b598-491e7f1ac26f%7D_1672620434076.zip?dl=1) and unzip in ETL-German-FHIR-Core folder.
3. Change directory to deploy. eg: cd deploy
4. Rename sample.env to .env
5. Modify the following properties in .env file
    ````
    DATA_FHIRSERVER_BASEURL=http://localhost:8080/fhir 
    DATA_FHIRSERVER_USERNAME=username
    DATA_FHIRSERVER_PASSWORD=password
    DATA_FHIRSERVER_TOKEN=token
    
    // Use begin date and end date to restrict the amount of data fetched. It's currently set to a default value of 
    DATA_BEGINDATE=1800-01-01
    DATA_ENDDATE=2099-12-31
    ````
6. Run docker compose up -d
   How does he know that the data upload to  omop is done ?
   Also in the PDF file there is an assumption of an existing omop database.. Do they need to install it or is it a container with ?
7. Open a browser and login to http://domain:8888
8. Open the jupyter notebook named ...
9. Run the notebook after data is uploaded to omop from fhir.