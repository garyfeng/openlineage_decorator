# openlineage_decorator
Python decorator class for Open Lineage client

## Problem and Solutions

[OpenLineage](https://github.com/OpenLineage/OpenLineage) is an open standard and an open-source implementation supported by the Linux Foundation for data observability and data lineage tracking. It defines a standard for data lineage events (see schema at https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json) that data processing pipelines send to a centralized RESTful service, backed by a Postgres database. This data can be queried and visualized, e.g., using the [open-source Marquez UI](https://github.com/MarquezProject/marquez). The goal is to log every data transformation and transaction, with versioning history. 

The challenge is to make this process unintrusive and ubiquitous. In our use case (Spark/pySpark, Delta Lake on Databricks), there are two approaches to integrate OpenLineage:

1. Using SparkListener, see https://openlineage.io/blog/openlineage-spark/. This involves adding a custom SparkListner to the Spark/Databricks environment, which automatically reports low-level Spark operations and file I/O. This is demonstrated in a separate notebook [OpenLineage-Spark Demo.ipynb](/notebooks/OpenLineage-Spark%20Demo.ipynb).
    - The advantage is that once set up, it tracks everything it can track without any additional work.
    - The disadvantage is also the lack of control -- you can't refine what you want to track; e.g., instead of your pySpark code, it only has access to the low-level Spark execution plans. 
2. Using Python and the RESTful API. 
    - Advantage is that you can log anything you want.
    - The drawback is that you have to specify what you want to log.
    
The goal here is to simplify the Python/API approach, by using python decorators. The idea is to pack all OpenLineage functions in a decorator function, so that the user only need to do the following to transformations that requires loggin (conceptual model):

```python
from openlineage-decorator import OpenLineageDecor

# set up OL config
ol_config = {
    # set up your OL URL, namespace, password, etc.
}
# get your instance of OL
my_ol = OpenLineageDecor(ol_config)
# defining the transformation using the decorator
@my_ol.track
def my_transformation(df1, df2):
    # ...
    return output_df

# calling this function will generate OL events
df = my_transformation(df1, df2)
```

And the decorator `@my_ol.track` will take care all the logging, with the custom OL configurations, such as your OL URL and namespace.

## Getting Started

This demo uses `docker-compose` to run a set of connected services:
- the Marquez OpenLineage API
- the Marquez web server/viz UI
- the Postgres database as the backend
- a Jupyter notebook service with Spark 3.1+

To start, clone `https://github.com/garyfeng/openlineage_decorator`, and then start `docker-composse`.

```sh
git clong https://github.com/garyfeng/openlineage_decorator.git
cd openlineage_decorator
docker-compose up
```

Start Jupyter at http://127.0.0.1:8888. You may need to look into the server logs to find the passcode to access the notebooks. Run the [OpenLineage Python Integration.ipynb](/notebooks/OpenLineage%20Python%20Integration.ipynb) notebook. If everything goes right, it will send a bunch of OpenLineage events to your Marquez server. 

Then open Marquez web UI at http://127.0.0.1:3000/. Look for the `namespace` of interest, and explore the `jobs` and `datasests`. 

