# Census Data Dashboard

This project is a **Streamlit** application that visualizes census data from the **`census_2011`** table in the **`GUVI`** database. The dashboard provides various insights into the census data such as population statistics, literacy rates, household amenities, worker percentages, and more.

## Features

The dashboard offers the following views:

1. **Population Overview**: Displays the total population of each state/union territory.
2. **Literate Details**: Shows the number of literate males and females by district.
3. **Worker Percentage**: Displays the percentage of male and female workers in each district.
4. **Households with LPG/PNG**: Shows the number of households with LPG or PNG as a cooking fuel.
5. **Religious Composition**: Displays the distribution of different religions (Hindus, Muslims, Christians, etc.) by district.
6. **Internet Access**: Shows the number of households with internet access by district.
7. **Educational Attainment**: Displays the educational attainment distribution (below primary, primary, middle, etc.) by district.
8. **Modes of Transportation**: Shows the number of households with access to different modes of transportation (bicycle, car, radio, television, etc.) by district.
9. **Condition of Census Houses**: Displays the condition of occupied census houses (dilapidated, with separate kitchen, with bathing facility, etc.) by district.
10. **Household Size Distribution**: Shows household size distribution by district.
11. **Total Households by State**: Displays the total number of households in each state/UT.
12. **Households with Latrine Facility**: Shows the number of households with latrine facilities within the premises by state/UT.
13. **Average Household Size**: Displays the average household size by state/UT.
14. **Owned vs Rented Households**: Shows the number of owned versus rented households by state/UT.
15. **Types of Latrine Facilities**: Displays the distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) by state/UT.
16. **Drinking Water Sources**: Shows households with access to drinking water sources near the premises by state/UT.
17. **Household Income Distribution**: Displays the average household income distribution in each state based on power parity categories.
18. **Married Couples by Household Size**: Shows the percentage of married couples with different household sizes by state/UT.
19. **Households Below the Poverty Line**: Displays the estimated number of households falling below the poverty line based on power parity categories by state/UT.
20. **Literacy Rate**: Shows the overall literacy rate (percentage of literate population) by state/UT.

## Project Setup

### Prerequisites

- Python 3.7 or later
- TiDB Database credentials
- Install dependencies:

```bash
pip install streamlit pandas mysql-connector-python sqlalchemy

### Running the Application 
1) Clone the repository:
git clone https://github.com/username/census-data-dashboard.git
2) Navigate to the project directory:
cd census-data-dashboard
3) Activate the virtual environment 
cd .venv/Scripts/activate
4) Run the Streamlit app:
streamlit run app.py

Project Structure

census-data-dashboard/
│
├── task.py               # Main Streamlit application code
├── Data
	├── Telangana.txt
└── README.md            # Project documentation (this file)
