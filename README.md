# AirQuality-Dataproc-Epheremal-Cluster-Composer

Probe el concepto de Cluster Efimero de Google DataProc orquestrado por Google Cloud Composer 
Utilizando el Conjunto de Datos Sofia air quality dataset. https://www.kaggle.com/datasets/hmavrodiev/sofia-air-quality-dataset

Me base en contenido del siguiente libro

https://www.packtpub.com/product/data-engineering-with-google-cloud-platform/9781800561328?utm_source=github&utm_medium=repository&utm_campaign=9781800561328

Para publicar el DAG de Airflow ejecutar el siguiente código en Google Cloud Shell

gcloud composer environments storage dags import --environment [TU_COMPOSER] --location [TU_REGION] --source gs://[TU_BUCKET]/Launch_Epheremal_Cluster_ETL_Job.py

## Imagenes de la Ejecución del Proyecto

### Lanzamiento del Trabajo ETL en PySpark

![Lanzamiento del Trabajo ETL en PySpark](https://images4.imagebam.com/84/2e/79/MERYKVG_o.JPG)

### Tablero de Información del Rendimiento del Trabajo ETL

![Tablero de Información del Rendimiento del Trabajo ETL](https://images4.imagebam.com/fb/79/3e/MERYKVH_o.JPG)

### DAG Publicado en Cloud Composer

![DAG Publicado en Cloud Composer](https://images4.imagebam.com/64/5c/4d/MERYKVI_o.JPG)

### Lanzamiento de Cluster Efimero por Cloud Composer

![Lanzamiento de Cluster Efimero por Cloud Composer](https://images4.imagebam.com/d0/29/d0/MERYKVJ_o.JPG)

### Ejecucion Completa del DAG que Lanza el Cluster Efimero y el Trabajo ETL en PySpark

![Ejecucion Completa del DAG que Lanza el Cluster Efimero y el Trabajo ETL en PySpark](https://images4.imagebam.com/4d/b6/c5/MERYKVK_o.JPG)

### Consulta SQL a los Datos Cargados por el Trabajo ETL en PySpark

![Consulta SQL a los Datos Cargados por el Trabajo ETL en PySpark](https://images4.imagebam.com/dd/06/7f/MERYKVL_o.JPG)