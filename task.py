

import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
import streamlit as st
import os
import mysql.connector
import re

# Task 1: Rename the Columns
def rename_columns(df):
    df = df.rename(columns={
        'State name': 'State/UT',
        'District name': 'District',
        'Male_Literate': 'Literate_Male',
        'Female_Literate': 'Literate_Female',
        'Rural_Households': 'Households_Rural',
        'Urban_Households': 'Households_Urban',
        'Age_Group_0_29': 'Young_and_Adult',
        'Age_Group_30_49': 'Middle_Aged',
        'Age_Group_50': 'Senior_Citizen',
        'Age_not_stated': 'Age_Not_Stated',
    })
    print("Columns renamed successfully.")
    return df

# Task 2: Rename State/UT Names
def rename_state_names(df):
    def format_state_name(state):
        formatted_state = state.lower().title().replace('And ', 'and ')
        return formatted_state

    df['State/UT'] = df['State/UT'].apply(format_state_name)
    print("State names formatted successfully.")
    return df

# Task 3: Update New State/UT Formation
def update_new_states(df, telangana_file):
    with open(telangana_file, 'r') as f:
        telangana_districts = f.read().splitlines()

    print("Telangana_districts")
    print(telangana_districts)
    # Update Telangana
    df.loc[df['District'].isin(telangana_districts) & 
           (df['State/UT'] == 'Andhra Pradesh'), 'State/UT'] = 'Telangana'

    # Update Ladakh (Leh, Kargil)
    df.loc[df['District'].isin(['Leh', 'Kargil']) & 
           (df['State/UT'] == 'Jammu and Kashmir'), 'State/UT'] = 'Ladakh'

    print("New State/UT formation updated.")
    return df

# Task 4: Find and Process Missing Data
def process_missing_data(df):
    # Checking missing data percentage before filling
    missing_data_before = df.isnull().mean() * 100
    
    # Ensure no inplace operations, store result in the same DataFrame variable
    df['Population'] = df['Population'].fillna(df['Male'] + df['Female'])
    df['Literate'] = df['Literate'].fillna(df['Literate_Male'] + df['Literate_Female'])
    df['Population'] = df['Population'].fillna(df['Young_and_Adult'] + df['Middle_Aged'] + df['Senior_Citizen'])
    df['Households'] = df['Households'].fillna(df['Households_Rural'] + df['Households_Urban'])
    
    # Checking missing data percentage after filling
    missing_data_after = df.isnull().mean() * 100
    
    # Display comparison
    missing_comparison = pd.DataFrame({
        'Before': missing_data_before,
        'After': missing_data_after
    })

    print("Missing data processed successfully.")
    return df, missing_comparison

# Task 5: Save to MongoDB
def save_to_mongo(df, mongo_url):
    try:
        client = MongoClient(mongo_url)
        db = client['newdb']
        census_collection = db['census']
        census_collection.insert_many(df.to_dict('records'))
        print("Data saved to MongoDB.")
    except Exception as e:
        print(f"An error occurred while saving to MongoDB Atlas: {e}")

# Task 6
def upload_to_tidb_from_mongo(csv_file, mongo_url, tidb_config):
    # Derive the table name from the CSV file name without extension
    table_name = os.path.splitext(os.path.basename(csv_file))[0]

    try:
        client = MongoClient(mongo_url)
        db = client['newdb']
        census_collection = db['census']
        data = list(census_collection.find({}, {"_id": 0}))  # Exclude MongoDB's internal '_id' field
        df = pd.DataFrame(data)
        df = df.drop(columns="District_code", errors='ignore')  # Drop 'District_code' if it exists
    except Exception as e:
        print(f"An error occurred while fetching data from MongoDB Atlas: {e}")
        exit(1)

    # Check for duplicate columns and handle them
    print("Original columns:")
    # print(df.columns.tolist())

    # Identify duplicate columns and make them unique
    duplicate_columns = df.columns[df.columns.duplicated()].unique()
    # print(f"Duplicate columns found: {duplicate_columns}")

    for col in duplicate_columns:
        df[col] = df.groupby(col).cumcount()  # Add a suffix to duplicates
        df.rename(columns={col: f"{col}_{df[col].astype(str)}"}, inplace=True)

    # Rename long column names if needed (limit to 64 characters)
    long_columns = [col for col in df.columns if len(col) > 64]
    for col in long_columns:
        new_col = re.sub(r'[^A-Za-z0-9_]', '_', col)[:64]  # Create a shorter name, preserving alphanumeric and underscores
        df.rename(columns={col: new_col}, inplace=True)
        print(f"Renamed column '{col}' to '{new_col}'")

    # Replace NaN values with None for MySQL compatibility
    df = df.where(pd.notnull(df), None)

    # Save the transformed DataFrame to a CSV file (for debugging purposes)
    df.to_csv("check_after_renameColumn.csv", index=False)

    # Connect to TiDB using mysql.connector
    try:
        print(f'Entered try block')
        conn = mysql.connector.connect(
            host=tidb_config['host'],
            port=tidb_config['port'],
            user=tidb_config['user'],
            password=tidb_config['password'],
            database=tidb_config['database']
        )
        cursor = conn.cursor()
        print(f"Connection established.")

        # Create the table in TiDB with appropriate column types
        column_definitions = []
        for col in df.columns:
            column_definitions.append(f"`{col}` VARCHAR(100)")

        # Create the SQL table
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}_2` (
                {', '.join(column_definitions)}
            );
        """
        print(f'Create query: {create_table_query}')
        cursor.execute(create_table_query)
        print("Table creation query executed successfully.")
        

        print(f"Final column name : {df.columns.to_list()}")
        # Prepare for data insertion
        insert_query = f"INSERT INTO `{table_name}_2` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['%s' for _ in df.columns])})"

        # Debug: Print the insert query and the first row of data to be inserted
        print("Insert Query:")
        print(insert_query)
        print("First row of data to insert:")
        print(df.values.tolist()[0])
        df.fillna('', inplace=True)
        # Insert data into the table in smaller batches
        batch_size = 500  # Define the batch size
        data_to_insert = df.values.tolist()

        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            conn.commit()  # Commit after each batch
            print(f"Inserted batch {i // batch_size + 1} of {len(data_to_insert) // batch_size + 1}")

        print(f"Data uploaded to TiDB, table name: {table_name}.")

    except mysql.connector.Error as e:
        print(f"An error occurred while uploading data to TiDB: {e}")

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# Task 7: Run Queries and Display with Streamlit
# mysql+mysqlconnector://Wo9LP4kv11iuK9y.root:<PASSWORD>@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/GUVI?ssl_ca=<CA_PATH>&ssl_verify_cert=true&ssl_verify_identity=true

# Task 7: Run Queries and Display with Streamlit
# def display_queries_in_streamlits(tidb_config):
#     # Establish connection to TiDB
#     try:
#         # Create SQLAlchemy engine for easy SQL execution
#         engine = create_engine(
#             f"mysql+mysqlconnector://{tidb_config['user']}:{tidb_config['password']}@{tidb_config['host']}:{tidb_config['port']}/{tidb_config['database']}"
#         )

#         # Streamlit App Setup
#         st.title("Census Data Dashboard")
#         # Sidebar for navigation
#         st.sidebar.title("Navigation")
#         options = st.sidebar.radio("Select a view:", ["Population Overview", "Literate Details", "Worker Percentage"])

#         # Query 1: Total Population by State
#         if options == "Population Overview":
#             query_population = """SELECT `State/UT`, SUM(Population) as Total_Population FROM GUVI.census_2011 GROUP BY `State/UT`;"""
#             df_population = pd.read_sql(query_population, engine)

#             st.subheader("Total Population by State/UT")
#             st.dataframe(df_population)
        
#         # Query 2: Literate Males and Females by District
#         elif options == "Literate Details":
#             query_literacy = """SELECT District, Literate_Male, Literate_Female FROM GUVI.census_2011;"""
#             df_literacy = pd.read_sql(query_literacy, engine)

#             st.subheader("Literate Males and Females by District")
#             st.dataframe(df_literacy)

        # Query 3: Percentage of Workers by District
    #     elif options == ""
    #         query_workers = """SELECT District, (Male_Workers + Female_Workers) / Population * 100 as Worker_Percentage FROM GUVI.census_2011;"""
    #         df_workers = pd.read_sql(query_workers, engine)
    #         st.subheader("Worker Percentage by District")
    #         st.dataframe(df_workers)

    #         # Query 4: Households with LPG/PNG
    #         query_lpg_png = """SELECT District, `LPG_or_PNG_Households` FROM GUVI.census_2011;"""
    #         df_lpg_png = pd.read_sql(query_lpg_png, engine)
    #         st.subheader("Households with LPG or PNG as Cooking Fuel by District")
    #         st.dataframe(df_lpg_png)

    #         # Query 5: Religious Composition
    #         query_religion = """SELECT District, Hindus, Muslims, Christians, Sikhs, Buddhists, Jains, Others_Religions FROM GUVI.census_2011;"""
    #         df_religion = pd.read_sql(query_religion, engine)
    #         st.subheader("Religious Composition by District")
    #         st.dataframe(df_religion)

    #         # Query 6: Households with Internet Access
    #         query_internet_access = """SELECT District, `Households_with_Internet` FROM GUVI.census_2011;"""
    #         df_internet_access = pd.read_sql(query_internet_access, engine)
    #         st.subheader("Households with Internet Access by District")
    #         st.dataframe(df_internet_access)

    #         # Query 7: Educational Attainment Distribution
    #         query_education = """SELECT District, Below_Primary_Education, Primary_Education, Middle_Education, Secondary_Education, Higher_Education, Graduate_Education, Other_Education FROM GUVI.census_2011;"""
    #         df_education = pd.read_sql(query_education, engine)
    #         st.subheader("Educational Attainment Distribution by District")
    #         st.dataframe(df_education)

    #         # Query 8: Households with Modes of Transportation
    #         query_transport = """SELECT District, Households_with_Bicycle, Households_with_Car_Jeep_Van, Households_with_Radio_Transistor, Households_with_Television FROM GUVI.census_2011;"""
    #         df_transport = pd.read_sql(query_transport, engine)
    #         st.subheader("Households with Various Modes of Transportation by District")
    #         st.dataframe(df_transport)

    #         # Query 9: Condition of Occupied Census Houses
    #         query_census_houses = """SELECT District, Condition_of_occupied_census_houses_Dilapidated_Households, Households_with_separate_kitchen_Cooking_inside_house, Having_bathing_facility_Total_Households, Having_latrine_facility_within_the_premises_Total_Households FROM GUVI.census_2011;"""
    #         df_census_houses = pd.read_sql(query_census_houses, engine)
    #         st.subheader("Condition of Occupied Census Houses by District")
    #         st.dataframe(df_census_houses)

    #         # Query 10: Household Size Distribution
    #         query_household_size = """SELECT District, Household_size_1_person_Households, Household_size_2_persons_Households, Household_size_3_to_5_persons_Households, Household_size_6_8_persons_Households, Household_size_9_persons_and_above_Households FROM GUVI.census_2011;"""
    #         df_household_size = pd.read_sql(query_household_size, engine)
    #         st.subheader("Household Size Distribution by District")
    #         st.dataframe(df_household_size)

    #         # Query 11: Total Number of Households by State
    #         query_total_households = """SELECT `State/UT`, SUM(Households) as Total_Households FROM GUVI.census_2011 GROUP BY `State/UT`;"""
    #         df_total_households = pd.read_sql(query_total_households, engine)
    #         st.subheader("Total Number of Households by State/UT")
    #         st.dataframe(df_total_households)

    #         # Query 12: Households with Latrine Facility within Premises by State
    #         query_latrine_facility = """SELECT `State/UT`, SUM(Having_latrine_facility_within_the_premises_Total_Households) as Total_Households_with_Latrine FROM GUVI.census_2011 GROUP BY `State/UT`;"""
    #         df_latrine_facility = pd.read_sql(query_latrine_facility, engine)
    #         st.subheader("Households with Latrine Facility within Premises by State/UT")
    #         st.dataframe(df_latrine_facility)

    #         # Query 13: Average Household Size by State
    #         query_avg_household_size = """SELECT `State/UT`, AVG(Households) as Average_Household_Size FROM GUVI.census_2011 GROUP BY `State/UT`;"""
    #         df_avg_household_size = pd.read_sql(query_avg_household_size, engine)
    #         st.subheader("Average Household Size by State/UT")
    #         st.dataframe(df_avg_household_size)

    #         # Query 14: Owned vs Rented Households by State
    #         query_owned_rented = """SELECT `State/UT`, SUM(Ownership_Owned_Households) as Owned_Households, SUM(Ownership_Rented_Households) as Rented_Households FROM GUVI.census_2011 GROUP BY `State/UT`;"""
    #         df_owned_rented = pd.read_sql(query_owned_rented, engine)
    #         st.subheader("Owned vs Rented Households by State/UT")
    #         st.dataframe(df_owned_rented)

    #         # Query 15: Distribution of Latrine Facilities by State
    #         query_latrine_types = """SELECT `State/UT`, SUM(Type_of_latrine_facility_Pit_latrine_Households) as Pit_Latrine, SUM(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households) as Flush_Latrine FROM GUVI.census_2011 GROUP BY `State/UT`;"""
    #         df_latrine_types = pd.read_sql(query_latrine_types, engine)
    #         st.subheader("Distribution of Different Types of Latrine Facilities by State/UT")
    #         st.dataframe(df_latrine_types)

    #         # Query 16: Households with Drinking Water Sources Near Premises by State
    #         query_water_source = """SELECT `State/UT`, SUM(Location_of_drinking_water_source_Near_the_premises_Households) as Households_with_Nearby_Water FROM GUVI.census_2011 GROUP BY `State/UT`;"""
    #         df_water_source = pd.read_sql(query_water_source, engine)
    #         st.subheader("Households with Drinking Water Sources Near Premises by State/UT")
    #         st.dataframe(df_water_source)

    #         # Query 17: Average Household Income Distribution by State
    #         query_income_distribution = """SELECT `State/UT`, SUM(Power_Parity_Less_than_Rs_45000) as Less_than_45000, SUM(Power_Parity_Rs_45000_150000) as Between_45000_and_150000, SUM(Power_Parity_Above_Rs_545000) as Above_545000 FROM GUVI.census_2011 GROUP BY `State/UT`;"""
    #         df_income_distribution = pd.read_sql(query_income_distribution, engine)
    #         st.subheader("Average Household Income Distribution by State/UT")
    #         st.dataframe(df_income_distribution)

    #         # Query 18: Percentage of Married Couples with Different Household Sizes by State
    #         query_married_couples = """SELECT `State/UT`, SUM(Married_couples_1_Households) as Married_1_Couple, SUM(Married_couples_2_Households) as Married_2_Couples FROM GUVI.census_2011 GROUP BY `State/UT`;"""
    #         df_married_couples = pd.read_sql(query_married_couples, engine)
    #         st.subheader("Percentage of Married Couples with Different Household Sizes by State/UT")
    #         st.dataframe(df_married_couples)

    # except Exception as e:
    #     st.error(f"An error occurred while connecting to TiDB or running the query: {e}")

def display_queries_in_streamlit(tidb_config):
    # Establish connection to TiDB
    try:
        # Create SQLAlchemy engine for easy SQL execution
        engine = create_engine(
            f"mysql+mysqlconnector://{tidb_config['user']}:{tidb_config['password']}@{tidb_config['host']}:{tidb_config['port']}/{tidb_config['database']}"
        )

        # Streamlit App Setup
        st.title("Census Data Dashboard")
        # Sidebar for navigation
        st.sidebar.title("Navigation")
        options = st.sidebar.radio(
            "Select a view:",
            [
                "Population Overview",
                "Literate Details",
                "Worker Percentage",
                "Households with LPG/PNG",
                "Religious Composition",
                "Internet Access",
                "Educational Attainment",
                "Modes of Transportation",
                "Condition of Census Houses",
                "Household Size Distribution",
                "Total Households by State",
                "Latrine Facility by State",
                "Average Household Size by State",
                "Owned vs Rented Households",
                "Types of Latrine Facilities",
                "Drinking Water Sources",
                "Income Distribution",
                "Married Couples and Household Sizes",
                "Households Below Poverty Line",
                "Literacy Rate by State"
            ]
        )

        # Query 1: Total Population by State
        if options == "Population Overview":
            query_population = """SELECT `State/UT`, SUM(Population) as Total_Population FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_population = pd.read_sql(query_population, engine)
            st.subheader("Total Population by State/UT")
            st.dataframe(df_population)

        # Query 2: Literate Males and Females by District
        elif options == "Literate Details":
            query_literacy = """SELECT District, Literate_Male, Literate_Female FROM GUVI.census_2011;"""
            df_literacy = pd.read_sql(query_literacy, engine)
            st.subheader("Literate Males and Females by District")
            st.dataframe(df_literacy)

        # Query 3: Percentage of Workers by District
        elif options == "Worker Percentage":
            query_workers = """SELECT District, (Male_Workers + Female_Workers) / Population * 100 as Worker_Percentage FROM GUVI.census_2011;"""
            df_workers = pd.read_sql(query_workers, engine)
            st.subheader("Worker Percentage by District")
            st.dataframe(df_workers)

        # Query 4: Households with LPG/PNG
        elif options == "Households with LPG/PNG":
            query_lpg_png = """SELECT District, `LPG_or_PNG_Households` FROM GUVI.census_2011;"""
            df_lpg_png = pd.read_sql(query_lpg_png, engine)
            st.subheader("Households with LPG or PNG as Cooking Fuel by District")
            st.dataframe(df_lpg_png)

        # Query 5: Religious Composition
        elif options == "Religious Composition":
            query_religion = """SELECT District, Hindus, Muslims, Christians, Sikhs, Buddhists, Jains, Others_Religions FROM GUVI.census_2011;"""
            df_religion = pd.read_sql(query_religion, engine)
            st.subheader("Religious Composition by District")
            st.dataframe(df_religion)

        # Query 6: Households with Internet Access
        elif options == "Internet Access":
            query_internet_access = """SELECT District, `Households_with_Internet` FROM GUVI.census_2011;"""
            df_internet_access = pd.read_sql(query_internet_access, engine)
            st.subheader("Households with Internet Access by District")
            st.dataframe(df_internet_access)

        # Query 7: Educational Attainment Distribution
        elif options == "Educational Attainment":
            query_education = """SELECT District, Below_Primary_Education, Primary_Education, Middle_Education, Secondary_Education, Higher_Education, Graduate_Education, Other_Education FROM GUVI.census_2011;"""
            df_education = pd.read_sql(query_education, engine)
            st.subheader("Educational Attainment Distribution by District")
            st.dataframe(df_education)

        # Query 8: Households with Modes of Transportation
        elif options == "Modes of Transportation":
            query_transport = """SELECT District, Households_with_Bicycle, Households_with_Car_Jeep_Van, Households_with_Radio_Transistor, Households_with_Television FROM GUVI.census_2011;"""
            df_transport = pd.read_sql(query_transport, engine)
            st.subheader("Households with Various Modes of Transportation by District")
            st.dataframe(df_transport)

        # Query 9: Condition of Occupied Census Houses
        elif options == "Condition of Census Houses":
            query_census_houses = """SELECT District, Condition_of_occupied_census_houses_Dilapidated_Households, Households_with_separate_kitchen_Cooking_inside_house, Having_bathing_facility_Total_Households, Having_latrine_facility_within_the_premises_Total_Households FROM GUVI.census_2011;"""
            df_census_houses = pd.read_sql(query_census_houses, engine)
            st.subheader("Condition of Occupied Census Houses by District")
            st.dataframe(df_census_houses)

        # Query 10: Household Size Distribution
        elif options == "Household Size Distribution":
            query_household_size = """SELECT District, Household_size_1_person_Households, Household_size_2_persons_Households, Household_size_3_to_5_persons_Households, Household_size_6_8_persons_Households, Household_size_9_persons_and_above_Households FROM GUVI.census_2011;"""
            df_household_size = pd.read_sql(query_household_size, engine)
            st.subheader("Household Size Distribution by District")
            st.dataframe(df_household_size)

        # Query 11: Total Number of Households by State
        elif options == "Total Households by State":
            query_total_households = """SELECT `State/UT`, SUM(Households) as Total_Households FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_total_households = pd.read_sql(query_total_households, engine)
            st.subheader("Total Number of Households by State/UT")
            st.dataframe(df_total_households)

        # Query 12: Households with Latrine Facility within Premises by State
        elif options == "Latrine Facility by State":
            query_latrine_facility = """SELECT `State/UT`, SUM(Having_latrine_facility_within_the_premises_Total_Households) as Total_Households_with_Latrine FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_latrine_facility = pd.read_sql(query_latrine_facility, engine)
            st.subheader("Households with Latrine Facility within Premises by State/UT")
            st.dataframe(df_latrine_facility)

        # Query 13: Average Household Size by State
        elif options == "Average Household Size by State":
            query_avg_household_size = """SELECT `State/UT`, AVG(Households) as Average_Household_Size FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_avg_household_size = pd.read_sql(query_avg_household_size, engine)
            st.subheader("Average Household Size by State/UT")
            st.dataframe(df_avg_household_size)

        # Query 14: Owned vs Rented Households by State
        elif options == "Owned vs Rented Households":
            query_owned_rented = """SELECT `State/UT`, SUM(Ownership_Owned_Households) as Owned_Households, SUM(Ownership_Rented_Households) as Rented_Households FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_owned_rented = pd.read_sql(query_owned_rented, engine)
            st.subheader("Owned vs Rented Households by State/UT")
            st.dataframe(df_owned_rented)

        # Query 15: Distribution of Latrine Facilities by State
        elif options == "Types of Latrine Facilities":
            query_latrine_types = """SELECT `State/UT`, SUM(Type_of_latrine_facility_Pit_latrine_Households) as Pit_Latrine, SUM(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households) as Flush_Latrine FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_latrine_types = pd.read_sql(query_latrine_types, engine)
            st.subheader("Distribution of Different Types of Latrine Facilities by State/UT")
            st.dataframe(df_latrine_types)

        # Query 16: Households with Drinking Water Sources Near Premises by State
        elif options == "Drinking Water Sources":
            query_water_source = """SELECT `State/UT`, SUM(Location_of_drinking_water_source_Near_the_premises_Households) as Households_with_Nearby_Water FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_water_source = pd.read_sql(query_water_source, engine)
            st.subheader("Households with Drinking Water Sources Near Premises by State/UT")
            st.dataframe(df_water_source)

        # Query 17: Income Distribution by District
        elif options == "Income Distribution":
            query_income_distribution = """SELECT `State/UT`,
SUM(Power_Parity_Less_than_Rs_45000) as Less_than_45000,
SUM(Power_Parity_Rs_45000_90000) as Between_45000_90000,
SUM(Power_Parity_Rs_90000_150000) as Between_90000_150000,
SUM(Power_Parity_Rs_45000_150000) as Between_45000_150000,
SUM(Power_Parity_Rs_150000_240000) as Between_150000_240000,
SUM(Power_Parity_Rs_240000_330000) as Between_240000_330000,
SUM(Power_Parity_Rs_150000_330000) as Between_150000_330000,
SUM(Power_Parity_Rs_330000_425000) as Between_330000_425000,
SUM(Power_Parity_Rs_330000_545000) as Between_330000_545000,
SUM(Power_Parity_Above_Rs_545000) as Above_Rs_545000
FROM GUVI.census_2011
GROUP BY `State/UT`;"""
            df_income_distribution = pd.read_sql(query_income_distribution, engine)
            st.subheader("Income Distribution by District")
            st.dataframe(df_income_distribution)

        # Query 18: Married Couples and Household Sizes by District
        elif options == "Married Couples and Household Sizes":
            query_married_household = """SELECT `State/UT`,
       SUM(Married_couples_1_Households) / SUM(Households) * 100 as Percentage_1_Couple_Households,
       SUM(Married_couples_2_Households) / SUM(Households) * 100 as Percentage_2_Couple_Households,
       SUM(Married_couples_3_Households) / SUM(Households) * 100 as Percentage_3_Couple_Households,
SUM(Married_couples_3_or_more_Households) / SUM(Households) * 100 as Percentage_3_or_More_Couple_Households,
SUM(Married_couples_4_Households) / SUM(Households) * 100 as Percentage_4_Couple_Households,
SUM(Married_couples_5__Households) / SUM(Households) * 100 as Percentage_5_Couple_Households
FROM GUVI.census_2011
GROUP BY `State/UT`;"""
            df_married_household = pd.read_sql(query_married_household, engine)
            st.subheader("Married Couples and Household Sizes by District")
            st.dataframe(df_married_household)

        # Query 19: Households Below Poverty Line by District
        elif options == "Households Below Poverty Line":
            query_bpl = """SELECT `State/UT`, Sum(Power_Parity_Less_than_Rs_45000) as Below_poverty_line FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_bpl = pd.read_sql(query_bpl, engine)
            st.subheader("Households Below Poverty Line by District")
            st.dataframe(df_bpl)

        # Query 20: Literacy Rate by State
        elif options == "Literacy Rate by State":
            query_literacy_rate = """SELECT `State/UT`, SUM(Literate_Male + Literate_Female) / SUM(Population) * 100 as Literacy_Rate FROM GUVI.census_2011 GROUP BY `State/UT`;"""
            df_literacy_rate = pd.read_sql(query_literacy_rate, engine)
            st.subheader("Literacy Rate by State/UT")
            st.dataframe(df_literacy_rate)

    except Exception as e:
        st.error(f"An error occurred: {e}")



# Main function to run all tasks sequentially
def run_all(csv_file, telangana_file, mongo_url):
    # Load the dataset
    df = pd.read_csv(csv_file)
    
    # Task 1: Rename columns
    df = rename_columns(df)
    
    # Task 2: Rename State names
    df = rename_state_names(df)
    
    # Task 3: Update State/UT formation
    df = update_new_states(df, telangana_file)
    
    # Task 4: Process missing data
    df, missing_comparison = process_missing_data(df)
    print("Missing Data Comparison:")
    print(missing_comparison)
    #df.to_csv("result1.csv")
    
    # Task 5: Save to MongoDB
    save_to_mongo(df, mongo_url)
    
    # Task 6: Upload to Relational Database
    upload_to_tidb_from_mongo(csv_file, mongo_url, tidb_config)
    
    # Task 7: Run queries and display in Streamlit
    # engine = create_engine(db_url)
    display_queries_in_streamlit(tidb_config)


# Example Usage

# Paths to files and databases
csv_file = 'census_2011.csv'
telangana_file = 'Data/Telangana.txt'
#db_url = 'postgresql://user:password@localhost:5432/mydatabase'
mongo_url = 'mongodb+srv://Vinothinia:Vinothiniatlas@cluster0.cdnxy.mongodb.net/'

tidb_config = {
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'port': 4000,
    'user': 'Wo9LP4kv11iuK9y.root',
    'password': 'xIyUnY8XN7Iwtarh',
    'database': 'GUVI'
}

# Run all tasks
run_all(csv_file, telangana_file, mongo_url)
#run_all(csv_file, telangana_file)
