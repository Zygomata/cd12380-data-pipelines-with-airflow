## TO-DO: run the follwing command and observe the JSON output: 
## airflow connections get redshift -o json
## 
## [{"id": "3", 
## "conn_id": "redshift", 
## "conn_type": "redshift", 
## "description": "", 
## "host": "default.859321506295.us-east-1.redshift-serverless.amazonaws.com", 
## "schema": "dev", 
## "login": "awsuser", 
## "password": "R3dsh1ft", 
## "port": "5439", 
## "is_encrypted": "False", 
## "is_extra_encrypted": "False", 
## "extra_dejson": {}, 
## "get_uri": "redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
#
## Copy the value after "get_uri":
#
## For example: redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev
#
