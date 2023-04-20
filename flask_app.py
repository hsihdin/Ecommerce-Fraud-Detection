#importing libs for bq and flask
from flask import Flask, render_template, request
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from google.cloud import bigquery
import pandas as pd
import pickle
import db_dtypes

#import libraries ML Libraries
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.pipeline import make_pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer


#passing gcp service account keys
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './service_account_key.json'

SERVICE_ACCOUNT_JSON_PATH = './service_account_key.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON_PATH)


app = Flask(__name__,template_folder='template')

@app.route('/')

def home():
    return render_template('form.html')

@app.route('/submit', methods=['POST'])

def submit():
    # Get form data
    # data = request.form['sqlQuery'] 
    data = request.form['sqlQuery']

    # Retrieve values from the form using request.form
    transaction_id = request.form['transactionId']
    batch_id = request.form['batchId']
    account_id = request.form['accountId']
    subscription_id = request.form['subscriptionId']
    customer_id = request.form['customerId']
    currency_code = request.form['currencyCode']
    country_code = request.form['countryCode']
    provider_id = request.form['providerId']
    product_id = request.form['productId']
    product_category = request.form['productCategory']
    channel_id = request.form['channelId']
    amount = request.form['amount']
    value = request.form['value']
    transaction_start_time = request.form['transactionStartTime']
    pricing_strategy = request.form['pricingStrategy']
    query = f"INSERT INTO ecommerce-fraud-detection.Test.Test_page (TransactionId, BatchId, AccountId, SubscriptionId, CustomerId, CurrencyCode, CountryCode, ProviderId, ProductId, ProductCategory, ChannelId, Amount, Value, TransactionStartTime, PricingStrategy) VALUES ('{transaction_id}', '{batch_id}', '{account_id}', '{subscription_id}', '{customer_id}', '{currency_code}', SAFE_CAST('{country_code}' as INTEGER), '{provider_id}', '{product_id}', '{product_category}', '{channel_id}', SAFE_CAST('{amount}' as INTEGER), SAFE_CAST('{value}' AS INTEGER), SAFE_CAST('{transaction_start_time}' AS TIMESTAMP), SAFE_CAST('{pricing_strategy}' AS INTEGER));"
    

    print("data received from Form is",query) 
    # Insert data into BigQuery
    # Replace 'your_table_name' with the name of your BigQuery table
        # Create a BigQuery client
    client = bigquery.Client()
    # Execute the query
    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete
    
    # Check for errors
    if query_job.errors:
        print("Error inserting data: {}".format(query_job.errors))
    else:
        print("Data successfully inserted.")


    # Create a dictionary to store the variables
    data = {
    'TransactionId': [transaction_id],
    'BatchId': [batch_id],
    'AccountId': [account_id],
    'SubscriptionId': [subscription_id],
    'CustomerId': [customer_id],
    'CurrencyCode': [currency_code],
    'CountryCode': [country_code],
    'ProviderId': [provider_id],
    'ProductId': [product_id],
    'ProductCategory': [product_category],
    'ChannelId': [channel_id],
    'Amount': [amount],
    'Value': [value],
    'TransactionStartTime': [transaction_start_time],
    'PricingStrategy': [pricing_strategy],
    }

    # Create a DataFrame from the dictionary
    df_from_form = pd.DataFrame(data)
    print(df_from_form)
    print(df_from_form.info())


    # table_ref = client.dataset('Test').table('test')
    # table = client.get_table(table_ref)
    # row = [(data,)]
    # client.insert_rows(table, row)

    # Convert the 'date_column' to datetime type
    # df_from_form['TransactionStartTime'] = pd.to_datetime(df_from_form['TransactionStartTime'], format='%Y-%m-%d %H:%M:%S%z')

    df_cleaned = dataCleaner(df_from_form)
    df_cleaned = df_cleaned.drop(['TransactionId', 'CurrencyCode', 'CountryCode', 'TransactionStartTime'], axis=1)

    # Select columns with numerical features
    num_columns = ['Amount', 'Value', 'PricingStrategy', 'Year',	'Month',	'Day',	'Hour',	'Minute',	'Seconds',	'week',	'weekday']
    cat_columns = ['BatchId',	'AccountId',	'SubscriptionId',	'CustomerId', 'ProviderId', 'ProductId',	'ProductCategory', 'ChannelId',]
    # Convert numerical columns to numeric data type
    df_cleaned[num_columns] = df_cleaned[num_columns].apply(pd.to_numeric, errors='coerce')

    # Convert categorical columns to categorical data type
    df_cleaned[cat_columns] = df_cleaned[cat_columns].astype('category')

    # Convert the 'date_column' to datetime type
    #df_cleaned['TransactionStartTime'] = pd.to_datetime(df_cleaned['TransactionStartTime'], format='%Y-%m-%d %H:%M:%S%z')

    # Initialize the imputer with the most frequent strategy
    num_pipeline = make_pipeline(SimpleImputer(strategy = "mean"), StandardScaler())
    cat_pipeline = make_pipeline(SimpleImputer(strategy = "most_frequent"), OneHotEncoder(handle_unknown="ignore"))
    preprocessing = ColumnTransformer([("num", num_pipeline, num_columns), ("cat", cat_pipeline, cat_columns)])
    # y = df_cleaned.FraudResult
    #X = preprocessing.fit_transform(df_cleaned)


    ##################################BQ LOAD############################
    # Create a client object and authenticate
    client = bigquery.Client.from_service_account_json('./service_account_key.json')

    #Fetching train data from BigQuery
    query_train = """
    SELECT *
    FROM `ecommerce-fraud-detection.Train.training`
    """
    # Execute the query and return the results as a pandas dataframe
    df_train_bq = client.query(query_train).to_dataframe()
    print(df_train_bq.head())
    # df_train_bq = df_train_bq.append(df_from_form, ignore_index=True)
    # Concatenate df_train_bq and df_from_form vertically
    
    # Execute the query and return the results as a pandas dataframe
    df_bq_cleaned = dataCleaner(df_train_bq)
    df_bq_cleaned = df_bq_cleaned.drop(['TransactionId', 'CurrencyCode', 'CountryCode', 'TransactionStartTime'], axis=1)
    
    df_train_bq = pd.concat([df_train_bq, df_from_form], ignore_index=True)
    print('----------- tail -------------')
    print(df_train_bq.tail())

    df_train_bq_processed=preprocessing.fit_transform(df_bq_cleaned)
    print(df_train_bq_processed.shape)
    df_cleaned = df_train_bq_processed[-1]


    filename = 'finalized_model.sav'
    loaded_model = pickle.load(open(filename, 'rb'))
    result = loaded_model.predict(df_cleaned)
    print("Classification result is ",result)


    return 'Data inserted to Bigquery successfully!'+"   \nClassification result is "+str(result)
    # return render_template('valid_transaction.html')

def splitTimeStamp(df):
    df['TransactionStartTime'] = pd.to_datetime(df['TransactionStartTime'], infer_datetime_format=True) 
    df['Year'] = df['TransactionStartTime'].dt.year
    df['Month'] = df['TransactionStartTime'].dt.month
    df['Day'] = df['TransactionStartTime'].dt.day
    df['Hour'] = df['TransactionStartTime'].dt.hour
    df['Minute'] = df['TransactionStartTime'].dt.minute
    df['Seconds'] = df['TransactionStartTime'].dt.second
    #  df['week'] = df['TransactionStartTime'].dt.week
    #  df['weekday'] = df['TransactionStartTime'].dt.weekday

    df['week'] = 5
    df['weekday'] =3
    return df

def correctTimeStamp(df):
  df['TransactionStartTime'] = df['TransactionStartTime'].astype(str).str.replace('T', ' ')
  df['TransactionStartTime'] = df['TransactionStartTime'].astype(str).str.replace('Z', '')
  return splitTimeStamp(df)

def dataCleaner(df):
  # Check for missing values
  print(df.isnull().sum())

  # Drop rows with missing values
  df = df.dropna()
  
  # Check for duplicates
  print(df.duplicated().sum())

  # Drop duplicates
  df = df.drop_duplicates()

  return correctTimeStamp(df)




if __name__ == '__main__':
    app.run(debug=True)

