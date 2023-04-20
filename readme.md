## E-COMMERCE FRAUD DETECTION 

E-commerce is growing at a rate never previously witnessed, making it a prime target for fraudsters. Due to the potential for financial loss, damage to one's reputation, and dissatisfied consumers, fraudulent transactions are a major risk for e-commerce enterprises. To combat this issue, many companies are investing in fraud detection and prevention systems that use cutting-edge machine learning algorithms to identify patterns of fraudulent behavior. 

An effective and user-friendly fraud detection solution is what this project seeks to give. The technology will instantly examine transaction data to spot any suspicious patterns and, if necessary, alert the appropriate parties. While simultaneously giving clients a nice and hassle-free experience, the technology will be able to recognize fraudulent transactions and prevent them. 

Data Discovery: 

We have opted for a Google Cloud Platform based implementation of the project. Data will be fetched in batches from the ecommerce server and loaded to cloud storage bucket. From the cloud storage bucket, data is made available to Bigquery. This process is automated by setting up a Batch ETL Pipeline. With the help of Bigquery, data flows to the compute engine, where our machine learning model is deployed. 

The team inspected incoming data features, did EDA using Pandas Profiling and plotted histograms to understand the relationship between features and target classes. We gained a thorough understanding of the data and knew what was needed for data preparation.

Data Preparation: 

The team checked for missing values and duplicate records in the dataset. Future missing values will be handled by the preprocessing pipeline. Duplicate records were removed. A preprocessing pipeline was established for numerical and categorical features, which includes imputing numerical features with the mean strategy, imputing categorical features with the most frequent strategy, scaling the numerical features, and encoding the categorical features.

Modeling :

Feature Engineering

Algorithm Selection: 
We have trained models using the following 6 algorithms to identify the best performing model: 
Logistic Regression 
Decision Tree 
Gaussian Naive Bayes 
Random Forest 
Gradient Boost 
AdaBoost Classifier 

Model Evaluation:  

Classification reports of each model were generated to understand the performance of each model. Being a fraud detection project, failure to identify fraudulent transactions as fraudulent can be costly for our client. Hence our objective was to maximize recall.  
kFold Cross-validation was used to check whether the model had overfitting issues. 


Hyperparameter tuning: Hyper parameter tuning was done to optimize the model’s performance. Tuning based on Random Search was employed for this purpose. 

File Structure:
 Ecommerce_Fraud_detection_with_bigquery_and_model.ipynb  --> Jupyter Notebook
