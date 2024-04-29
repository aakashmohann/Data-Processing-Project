import pandas as pd
import streamlit as st
import docx
import pymongo
import psycopg2
from psycopg2 import extensions

# Function to rename columns
def rename_columns(df):
    column_mapping = {
        'State name': 'State or UT',
        'District name': 'District',
        'Male_Literate': 'Literate_Male',
        'Female_Literate': 'Literate_Female',
        'Rural_Households': 'Households_Rural',
        'Urban_Households': 'Households_Urban',
        'Age_Group_0_29': 'Young_and_Adult',
        'Age_Group_30_49': 'Middle_Aged',
        'Age_Group_50': 'Senior_Citizen',
        'Age not stated': 'Age_Not_Stated'
    }
    df.rename(columns=lambda x: column_mapping[x] if x in column_mapping else x, inplace=True)
    return df

#Fuction for formating the state_names
def format_state_name(name):
    words = name.split()
    formatted_words = []
    for word in words:
        if word.lower() == 'and':
            formatted_words.append(word.lower())
        else:
            formatted_words.append(word.capitalize())
    return ' '.join(formatted_words)

#Fuction for read doc file
def read_docx(file_path):
    doc = docx.Document(file_path)
    text = []
    for paragraph in doc.paragraphs:
        # Split each paragraph into words
        words = paragraph.text.split()
        text.extend(words)
    return text

#Fuction for performing task3
def changing_state_name(df,file_path):
    
    Telangana_districts = read_docx(file_path)
    df.loc[df['District'].isin(Telangana_districts), 'State or UT'] = 'Telangana'
    df['District'] = df['District'].replace({'Leh(Ladakh)': 'Leh'})
    Ladakh=['Leh','Kargil']
    df.loc[df['District'].isin(Ladakh), 'State or UT'] = 'Ladakh'
    return df

#Function for missing values

def fill_missing_values(df):
    df['Population'].fillna(df['Male'] + df['Female'], inplace=True)
    df['Population'].fillna(df['Hindus']+df['Muslims']+df['Christians']+df['Sikhs']+df['Buddhists']+df['Jains']+df['Others_Religions']+df['Religion_Not_Stated'],inplace=True)
    df['Population'].fillna(df['Age_Not_Stated']+df['Senior_Citizen']+df['Middle_Aged']+df['Young_and_Adult'],inplace=True)
    df['Population'].fillna(df['Workers']+df['Non_Workers'], inplace=True)
    df['Male'].fillna(df['Population'] - df['Female'],inplace=True)
    df['Female'].fillna(df['Population'] - df['Male'],inplace=True)

    df['Literate'].fillna(df['Literate_Male']+df['Literate_Male'],inplace=True)
    df['Literate_Male'].fillna(df['Literate'] - df['Literate_Female'],inplace=True)
    df['Literate_Female'].fillna(df['Literate']-df['Literate_Male'],inplace=True)

    df['SC'].fillna(df['Male_SC'] + df['Female_SC'],inplace=True)
    df['Male_SC'].fillna(df['SC'] - df['Female_SC'],inplace=True)
    df['Female_SC'].fillna(df['SC'] - df['Male_SC'],inplace=True)

    df['ST'].fillna(df['Male_ST'] + df['Female_ST'],inplace=True)
    df['Male_ST'].fillna(df['ST'] - df['Female_ST'],inplace=True)
    df['Female_ST'].fillna(df['ST'] - df['Male_ST'],inplace=True)

    df['Workers'].fillna(df['Male_Workers'] + df['Female_Workers'],inplace=True)
    df['Male_Workers'].fillna(df['Workers'] - df['Female_Workers'],inplace=True)
    df['Female_Workers'].fillna(df['Workers'] - df['Male_Workers'],inplace=True)

    df['Households_Rural'].fillna(df['Households'] - df['Households_Urban'],inplace=True)
    df['Households'].fillna(df['Households_Urban'] + df['Households_Rural'],inplace=True)
    df['Households_Urban'].fillna(df['Households'] - df['Households_Rural'],inplace=True)

    df['Total_Education'].fillna(df['Literate_Education'] + df['Illiterate_Education'],inplace=True)
    df['Illiterate_Education'].fillna(df['Total_Education'] - df['Literate_Education'],inplace=True)
    df['Literate_Education'].fillna(df['Total_Education'] - df['Illiterate_Education'],inplace=True)

    return df

def push_df_to_mongoDB(df):
    connection_string = "mongodb+srv://aakash:Amar*9786@cluster0.tqxoclr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = pymongo.MongoClient(connection_string)

    census=df.to_dict(orient="records")
    db=client["Capstone"]
    collection = db['census'] 
    collection.delete_many({})
    collection.insert_many(census)

def create_database():
    postgres_connection = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "root",
        "database": "capstone"  
    }
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        user="postgres",
        password="root")
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor=conn.cursor()
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'capstone'")
    exists = cursor.fetchone()

    # If database doesn't exist, create it
    if not exists:
        cursor.execute("CREATE DATABASE capstone")
    cursor.close()
    conn.close()

def execute_sql_query(query):
        conn = psycopg2.connect(
        host="localhost",
        port="5432",
        user="postgres",
        password="root",
        database="capstone"
        )
        
        df = pd.read_sql_query(query, conn)
        return df




# Streamlit app
def main():
    st.header("Capstone Project for data transformation and migration")
    st.markdown("<h7 style='color:black;'>Census CSV File Column Renaming....</h7>",unsafe_allow_html=True)
    df = pd.read_csv("census_2011.csv")

    # Perform column renaming
    df = rename_columns(df)

    # Display completion message
    st.markdown("<h7 style='color:black;'>Renaming the columns in the CSV File Completed which is task1</h7>",unsafe_allow_html=True)
    #task2
    df['State or UT'] = df['State or UT'].apply(format_state_name)
    st.markdown("<h7 style='color:black;'>Formating state column values completed which is task 2</h7>",unsafe_allow_html=True)
    #task3
    file_path = 'Telangana.docx'
    df=changing_state_name(df,file_path)
    st.markdown("<h7 style='color:black;'>Changing state name after partition completed which is task 3</h7>",unsafe_allow_html=True)

    missing_percentage_before_filling = (df.isnull().sum() / len(df)) * 100
    #task4
    df = fill_missing_values(df)

    # Display completion message for missing value filling
    st.markdown("<h7 style='color:black;'>Filling missing values completed which is task 4</h7>", unsafe_allow_html=True)
    missing_percentage_after_filling = (df.isnull().sum() / len(df)) * 100
    
    st.button("Reset", type="primary")
    if st.button('Show Missing Value Percentages'):
        for column_name in df.columns:
            missing_percentage_before = missing_percentage_before_filling.get(column_name, 0)
            missing_percentage_after = missing_percentage_after_filling.get(column_name, 0)
            st.write(f"Column: {column_name}, Before Filling: {missing_percentage_before:.2f}%, After Filling: {missing_percentage_after:.2f}%")
    else:
        st.write('Show Missing Value Percentages')
    #Task5

    # Replace NaN values with a placeholder value
    df=df.fillna(value="placeholder")

    # Replace the placeholder value with None
    df.replace("placeholder", None, inplace=True)

    #Save the processed data to mongoDB with a collection named “census” .
    push_df_to_mongoDB(df)
    st.markdown("<h7 style='color:black;'>Pushed the df to mongoDB</h7>", unsafe_allow_html=True)


    #Task 6: Database connection and data upload
    create_database()
    st.markdown("<h7 style='color:black;'>Database Created in PG Database</h7>", unsafe_allow_html=True)



    # MongoDB connection details
    mongo_connection_string = "mongodb+srv://aakash:Amar*9786@cluster0.tqxoclr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    mongo_client = pymongo.MongoClient(mongo_connection_string)
    mongo_db = mongo_client["Capstone"]
    mongo_collection = mongo_db["census"]

    # PostgreSQL connection details
    postgres_connection = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "root",
        "database": "capstone"  # Replace "your_database_name" with your actual database name
    }

    def create_postgres_table(collection):
        # Get the first document from the collection
        first_document = collection.find_one()

        # Remove "_id" key from the first document and replace spaces in keys with underscores
        modified_document = {key.replace(" ", "_"): value for key, value in first_document.items() if key != "_id"}
        
        # Infer data types for each key
        column_datatypes = {}
        for key, value in modified_document.items():
            if isinstance(value, str):
                column_datatypes[key] = "VARCHAR"
            elif isinstance(value, int):
                column_datatypes[key] = "INTEGER"
            elif value is None:
                column_datatypes[key] = "INTEGER"  # Assuming None values should be stored as INTEGER
            else:
                column_datatypes[key] = "INTEGER"  # Default to VARCHAR for unknown types
        
        # Create PostgreSQL table
        with psycopg2.connect(**postgres_connection) as conn:
            with conn.cursor() as cursor:
                # Construct SQL query to create table
                #create_table_query = f"CREATE TABLE IF NOT EXISTS census ({', '.join([f'{column} {datatype}' for column, datatype in column_datatypes.items()])})"
                create_table_query = f"CREATE TABLE IF NOT EXISTS census ({list(modified_document.keys())[0]} SERIAL PRIMARY KEY, {', '.join([f'{column} {datatype}' for column, datatype in column_datatypes.items() if column != list(modified_document.keys())[0]])})"
                cursor.execute(create_table_query)
                conn.commit()

    def insert_into_postgres(collection):
        # Remove "_id" key from each document and replace spaces in keys with underscores
        modified_documents = []
        for document in collection.find():
            modified_document = {key.replace(" ", "_"): value for key, value in document.items() if key != "_id"}
            modified_documents.append(modified_document)
        
        # Get column names and values from the modified documents
        column_names = list(modified_documents[0].keys())
        values = [[document.get(column, None) for column in column_names] for document in modified_documents]

        # Insert values into PostgreSQL table
        with psycopg2.connect(**postgres_connection) as conn:
            with conn.cursor() as cursor:
                # Construct SQL query to insert values
                insert_query = f"INSERT INTO census ({', '.join(column_names)}) VALUES ({', '.join(['%s' for _ in column_names])})"
                cursor.executemany(insert_query, values)
                conn.commit()

    print("droping table census if exists")
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        user="postgres",
        password="root",
        database="capstone"
    )
    cursor=conn.cursor()
    cursor.execute("Drop table if exists census")
    conn.commit()
    print("completed")


    # Create PostgreSQL table
    print("creating census table again")
    create_postgres_table(mongo_collection)
    print("Table created")

    print("Insertion Started:")
    # Insert data into PostgreSQL table
    insert_into_postgres(mongo_collection)
    print("Insertion completed")
    st.markdown("<h5 style='color:black;'>census table created and data inserted:</h5>", unsafe_allow_html=True)


    #1
    st.markdown("<h5 style='color:black;'>Total Population of India : </h5>", unsafe_allow_html=True)
    # Execute the SQL query and get the results as a DataFrame
    census_df = execute_sql_query("SELECT sum(population)as total_population_of_india from census;")

    # Display the DataFrame in Streamlit
    st.dataframe(census_df)
    #2
    st.markdown("<h5 style='color:black;'>literate male in female :</h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("select district,literate_male,literate_female from census")

    # Display the DataFrame in Streamlit
    st.dataframe(census_df)
    #3
    st.markdown("<h5 style='color:black;'>Percentage of workers in each district</h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""SELECT district,
                CONCAT(
                ROUND(
                CAST((CAST(workers AS NUMERIC) / CAST(population AS NUMERIC)) * 100 AS NUMERIC)
                ),
                '%'
                ) AS percentage 
                FROM census;""")

    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #4
    st.markdown("<h5 style='color:black;'>LPG or PNG as a cooking fuel in each district:</h5>", unsafe_allow_html=True)
    census_df = execute_sql_query("select district,lpg_or_png_households from census")
    st.dataframe(census_df)


    #5
    st.markdown("<h5 style='color:black;'>Religious composition of each district:</h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("select district,hindus,muslims,christians,sikhs,buddhists,jains,others_religions,religion_not_stated from census;")

    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #6
    st.markdown("<h5 style='color:black;'>Households have internet access in each district:</h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("select district,households_with_internet from census;")

    # Display the DataFrame in Streamlit
    st.dataframe(census_df)
    #7
    st.markdown("<h5 style='color:black;'>Educational attainment distribution: </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("select district,below_primary_education,primary_education,middle_education,secondary_education,higher_education,graduate_education,other_education from census;")

    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #8
    st.markdown("<h5 style='color:black;'>Households have access to various modes of transportation in each district: </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                    select district,"households_with_bicycle",
                                    "households_with_car_jeep_van",
                                    "households_with_scooter_motorcycle_moped"
                                    from census
                                    """)

    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #9
    st.markdown("<h5 style='color:black;'>Condition of occupied census houses : </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                select district,"condition_of_occupied_census_houses_dilapidated_households",
                "households_with_separate_kitchen_cooking_inside_house",
                "having_bathing_facility_total_households",
                "having_latrine_facility_within_the_premises_total_households",
                "ownership_owned_households",
                "ownership_rented_households",
                "type_of_bathing_facility_enclosure_without_roof_households",
                "type_of_fuel_used_for_cooking_any_other_households",
                "type_of_latrine_facility_pit_latrine_households",
                "type_of_latrine_facility_other_latrine_households",
                "type_of_latrine_facility_night_soil_disposed_into_open_drain_ho",
                "type_of_latrine_facility_flush_pour_flush_latrine_connected_to_",
                "not_having_bathing_facility_within_the_premises_total_household",
                "not_having_latrine_facility_within_the_premises_alternative_sou"
                from census;
                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #10
    st.markdown("<h5 style='color:black;'>Household size distributed in each district : </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""select district,"household_size_1_person_households",
                "household_size_2_persons_households",
                "household_size_1_to_2_persons",
                "household_size_3_persons_households",
                "household_size_3_to_5_persons_households",
                "household_size_4_persons_households",
                "household_size_5_persons_households",
                "household_size_6_8_persons_households",
                "household_size_9_persons_and_above_households"
                from census;
                
                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #11
    st.markdown("<h5 style='color:black;'>Total number of households in each state : </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                select state_or_ut,sum(households)as total_households from census
                                group by state_or_ut;
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)
    
    #12
    st.markdown("<h5 style='color:black;'>Households have a latrine facility within the premises in each state : </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                select state_or_ut,sum("having_latrine_facility_within_the_premises_total_households")as 
                                latrine_facility_within_premise from census
                                group by state_or_ut
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #13
    st.markdown("<h5 style='color:black;'>The average household size in each state : </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                    SELECT 
                                    state_or_ut,
                                    round(AVG("household_size_1_person_households")) AS avg_household_size_1_person_households,
                                    round(AVG("household_size_2_persons_households") )AS avg_household_size_2_persons_households,
                                    round(AVG("household_size_1_to_2_persons") )AS avg_household_size_1_to_2_persons,
                                    round(AVG("household_size_3_persons_households")) AS abg_household_size_3_persons_households,
                                    round(AVG("household_size_3_to_5_persons_households") )AS avg_household_size_3_to_5_persons,
                                    round(AVG("household_size_4_persons_households") )AS avg_household_size_4_persons_households,
                                    round(AVG("household_size_5_persons_households") )AS avg_household_size_5_persons_households,
                                    round(AVG("household_size_6_8_persons_households") )AS avg_330000_to_425000,
                                    round(AVG("household_size_9_persons_and_above_households") )AS avg_household_size_9_person_and_above_households
                                    FROM census
                                    GROUP BY state_or_ut;
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #14

    st.markdown("<h5 style='color:black;'>Households are owned versus rented in each state : </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                select state_or_ut,sum("ownership_owned_households") as ownership_owned_households,
                                sum("ownership_rented_households")as ownership_rented_households from census
                                group by state_or_ut
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #15

    st.markdown("<h5 style='color:black;'>The distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) in each state : </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                select state_or_ut,sum("type_of_latrine_facility_pit_latrine_households")as pit_latrine_households,
                                sum("type_of_latrine_facility_other_latrine_households") as other_latrine_households,
                                sum("type_of_latrine_facility_night_soil_disposed_into_open_drain_ho") as open_drain_households,
                                sum("type_of_latrine_facility_flush_pour_flush_latrine_connected_to_") as flush_pour_latrine_households
                                from census group by state_or_ut
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #16

    st.markdown("<h5 style='color:black;'>Households have access to drinking water sources near the premises in each state: </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                select state_or_ut,sum("location_of_drinking_water_source_near_the_premises_households")as 
                                "location_of_drinking_water_source_near_the_premises_households" from census
                                group by state_or_ut
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #17

    st.markdown("<h5 style='color:black;'>The average household income distribution in each state based on the power parity categories: </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                SELECT 
                                state_or_ut,
                                round(AVG("power_parity_less_than_rs_45000")) AS avg_less_than_45000,
                                round(AVG("power_parity_rs_45000_90000") )AS avg_45000_to_90000,
                                round(AVG("power_parity_rs_90000_150000") )AS avg_90000_to_150000,
                                round(AVG("power_parity_rs_45000_150000")) AS avg_45000_to_150000,
                                round(AVG("power_parity_rs_150000_240000") )AS avg_150000_to_240000,
                                round(AVG("power_parity_rs_240000_330000") )AS avg_240000_to_330000,
                                round(AVG("power_parity_rs_150000_330000") )AS avg_150000_to_330000,
                                round(AVG("power_parity_rs_330000_425000") )AS avg_330000_to_425000,
                                round(AVG("power_parity_rs_425000_545000") )AS avg_425000_to_545000,
                                round(AVG("power_parity_rs_330000_545000")) AS avg_330000_to_545000,
                                round(AVG("power_parity_above_rs_545000")) AS avg_above_545000
                            FROM census
                            GROUP BY state_or_ut;
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

    #18

    st.markdown("<h5 style='color:black;'>The percentage of married couples with different household sizes in each state: </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                               
                select  (household_size_2_persons_households/married_couples)*100 as per_married_couples_in_household_size_2,
                        (household_size_1_to_2_persons/married_couples)*100 as per_married_couples_in_household_size_1_2,
                        (household_size_3_persons_households/married_couples)*100 as per_married_couples_in_household_size_3,
                        (household_size_3_to_5_persons/married_couples)*100 as per_married_couples_in_household_size_3_5,
                        (household_size_4_persons_households/married_couples)*100 as per_married_couples_in_household_size_4,
                        (household_size_5_persons_households/married_couples)*100 as per_married_couples_in_household_size_5,
                        (household_size_6_to_9_persons/married_couples)*100 as per_married_couples_in_household_size_6_9,
                        (household_size_9_person_and_above_households/married_couples)*100 as per_married_couples_in_household_size_9_more
                        
                from

                (select state_or_ut,
                        round(sum("household_size_2_persons_households")) aS household_size_2_persons_households,
                        round(sum("household_size_1_to_2_persons") )AS household_size_1_to_2_persons,
                        round(sum("household_size_3_persons_households")) AS household_size_3_persons_households,
                        round(sum("household_size_3_to_5_persons_households") )AS household_size_3_to_5_persons,
                        round(sum("household_size_4_persons_households") )AS household_size_4_persons_households,
                        round(sum("household_size_5_persons_households") )AS household_size_5_persons_households,
                        round(sum("household_size_6_8_persons_households") )AS household_size_6_to_9_persons,
                        round(sum("household_size_9_persons_and_above_households") )AS household_size_9_person_and_above_households,
                        (SUM(married_couples_1_households) + SUM(married_couples_2_households) + 
                        SUM(married_couples_3_households) + SUM(married_couples_3_or_more_households) + 
                        SUM(married_couples_4_households) + SUM(married_couples_5__households) + 
                        SUM(married_couples_none_households)) AS married_couples  
                        from census group by state_or_ut)a 
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

     #19

    st.markdown("<h5 style='color:black;'>Households fall below the poverty line in each state based on the power parity categories: </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                select state_or_ut,sum(power_parity_less_than_rs_45000) as households_falls_below_the_poverty_line  from census
                                group by state_or_ut;
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)

     #20

    st.markdown("<h5 style='color:black;'>The overall literacy rate (percentage of literate population) in each state: </h5>", unsafe_allow_html=True)

    census_df = execute_sql_query("""
                                SELECT 
                                state_or_ut,
                                SUM(power_parity_less_than_rs_45000) AS below_rs_45000,
                                SUM(power_parity_rs_45000_90000) AS rs_45000_90000,
                                SUM(power_parity_rs_90000_150000) AS rs_90000_150000,
                                SUM(power_parity_rs_45000_150000) AS rs_45000_150000,
                                SUM(power_parity_rs_150000_240000) AS rs_150000_240000,
                                SUM(power_parity_rs_240000_330000) AS rs_240000_330000,
                                SUM(power_parity_rs_150000_330000) AS rs_150000_330000,
                                SUM(power_parity_rs_330000_425000) AS rs_330000_425000,
                                SUM(power_parity_rs_425000_545000) AS rs_425000_545000,
                                SUM(power_parity_rs_330000_545000) AS rs_330000_545000,
                                SUM(power_parity_above_rs_545000) AS above_rs_545000,
                                SUM(total_power_parity) AS total_power_parity
                                FROM census
                                GROUP BY state_or_ut;
                                """)
    # Display the DataFrame in Streamlit
    st.dataframe(census_df)





if __name__ == "__main__":
    main()



