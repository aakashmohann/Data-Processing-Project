# Data-Processing-Project
Capstone Project: Data Transformation and Migration

**Overview**

This project aims to perform various data transformation tasks on census data and migrate the transformed data to MongoDB and PostgreSQL databases. The project 
is implemented in Python using libraries such as pandas, pymongo, psycopg2, and streamlit.

**Installation**

**To run the project locally, follow these steps:**

->Clone the repository to your local machine.

->Install the required Python packages using pip:

Copy code

$ pip install pandas streamlit pymongo psycopg2 python-docx

->Ensure you have MongoDB and PostgreSQL installed and running on your local machine.

->Update the connection strings and database credentials in the Python code according to your local setup.


**Usage**


->Navigate to the project directory.

->Run the Streamlit app:

Copy code

$ streamlit run app.py

->Access the Streamlit app in your web browser.


**Project Structure**


->project1.py: Main Streamlit application file containing the user interface and data processing logic.

->census_2011.csv: Input CSV file containing the census data.

->Telangana.docx: Document file used for data transformation.

->README.md: README file providing an overview of the project and instructions for setup and usage.


**Data Transformation Tasks**


->Column Renaming: Rename specific columns in the CSV file.

->State Name Formatting: Format the state names in the dataset.

->Changing State Names: Modify state names based on specific requirements.

->Missing Value Handling: Fill missing values in the dataset using appropriate methods.

->Data Conversion and Migration: Convert the dataset to the desired format and migrate it to MongoDB and PostgreSQL databases.

->Database Creation and Table Setup: Create the necessary databases and tables in PostgreSQL.


**Features**


->Interactive Streamlit interface for data transformation tasks and database interaction.

->Support for various data analysis queries on the migrated data in PostgreSQL.

->Display of query results in tabular format within the Streamlit app.

**Future Enhancements**


->Error handling and validation for database connections and data migration processes.

->Improved user interface design and navigation within the Streamlit app.

->Additional data analysis and visualization features for exploring census data.
