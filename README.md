# Movies-ETL
Performing ETL process on data utilizing Python, Pandas and SQL.
## ETL Process Assumptions
- When designing this ETL automated code, one of the main assumptions that I am working under, is that the messages generated are monitored. I operate under this assumption, because this code should be activated manually. This would mean the person who activates the code, to upload new data to the database, would be able to see the generated print results. 
- This is important especially encase the second assumption is violated. My second assumption is that the format of the initial extracted data stays the same. This code was specifically designed to extract Wikipedia as a JSON file and the Kaggle and Ratings data as a csv. If this changes, the code would no longer function and would alert the person activating the code. 
- Another assumption I am working under is that the names for the columns in the extracted data stay the same. Since the code references specific column headers to clean the data, if these column headers change then the data would not be properly cleaned. Along these same lines, I am also assuming that majority of the box office, budget, release date and running time data are strings. This assumption is key to the cleaning process of the Wikipedia data. Try and except blocks are placed throughout the code to notify where the code is malfunctioning. These notifications enable changes like this to be more easily troubleshot and remedied. 
- The last few assumptions I made when designing the code have to do with the desired content of the data after it has been cleaned and uploaded to the SQL database. I’m assuming that two separate tables need to be generated within SQL. One that has the combined movie data from Wikipedia and Kaggle and one that has only ratings data. If separate data needs to be loaded to the database, such as a table with movie data combined with their ratings, then the current code will need to be altered to meet this requirement. 
