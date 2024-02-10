# AirQuality-Dataproc-Epheremal-Cluster-Composer

Probe el concepto de Cluster Efimero de Google DataProc orquestrado por Google Cloud Composer 
Utilizando el Conjunto de Datos Sofia air quality dataset. https://www.kaggle.com/datasets/hmavrodiev/sofia-air-quality-dataset

Me base en contenido del siguiente libro

https://www.packtpub.com/product/data-engineering-with-google-cloud-platform/9781800561328?utm_source=github&utm_medium=repository&utm_campaign=9781800561328

Para publicar el DAG de Airflow ejecutar el siguiente código en Google Cloud Shell

gcloud composer environments storage dags import --environment [TU_COMPOSER] --location [TU_REGION] --source gs://[TU_BUCKET]/Launch_Epheremal_Cluster_ETL_Job.py