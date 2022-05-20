import numpy as np
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = 'dataanalyst-188909'
DATA_SET_ID = 'ga4_ecommerce_sample'

client = bigquery.Client(project=PROJECT_ID)
dataset_ref = client.dataset(DATA_SET_ID)


# create training data
with open('sql/training_data.sql', 'r') as file:
    training_data_sql = file.read().rstrip()

training_data_table = bigquery.Table(dataset_ref.table('training_data'))
training_data_table.view_query = training_data_sql
training_data_view = client.create_table(training_data_table)

# function to test different K model
def kmeans_model(n_cluster, model_name):
    """
        training with different cluster num model
    """
    sql =f'''
    CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATA_SET_ID}.{model_name}` 
        OPTIONS(model_type='kmeans',
        kmeans_init_method = 'KMEANS++',
        num_clusters={n_cluster}
    ) AS (
        SELECT 
            -- * except(user_pseudo_id)
            view_item_cnt,
            add_payment_info_cnt,
            page_view_cnt,
            scroll_cnt,
            user_engagement_cnt,
            add_shipping_info_cnt,
            begin_checkout_cnt,
            purchase_item_cnt,
            total_spending
        FROM `{PROJECT_ID}.{DATA_SET_ID}.training_data`
    )
    '''
    job_config = bigquery.QueryJobConfig()
    result = client.query(sql, job_config=job_config)
    return result

low_k = 3
high_k = 15
model_prefix_name = 'kmeans_clusters_'

for k in range(low_k, high_k+1):
    print(k)
    model_name = model_prefix_name + str(k)
    model_result = kmeans_model(k, model_name)
    print(f"Model started: {model_name}")

# check model clear
model_result.result()


# evaluate model with different K
df_list = []
models = client.list_models(DATA_SET_ID) 
for model in models:
    full_model_id = f"{model.dataset_id}.{model.model_id}"
    sql =f'''
        SELECT
            '{model.model_id}' as model_name, 
            davies_bouldin_index,
            mean_squared_distance 
        FROM ML.EVALUATE(MODEL `{full_model_id}`)
    '''
    job_config = bigquery.QueryJobConfig()
    query_job = client.query(sql, job_config=job_config)
    df_temp = query_job.to_dataframe()
    df_list.append(df_temp)

df_evaluate = pd.concat(df_list)
df_evaluate['n_clusters'] = df_evaluate['model_name'].str.split('_').map(lambda x: x[2])
df_evaluate.to_clipboard()


## remove model
models = client.list_models(DATA_SET_ID) 
for model in models:
    model_id = DATA_SET_ID+"."+model.model_id
    client.delete_model(model_id)  # Make an API request.
    print(f"Deleted model '{model_id}'")
