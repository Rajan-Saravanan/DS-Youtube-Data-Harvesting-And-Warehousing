# Youtub Data Harvesing and Warehousing

### About the Project
This project is about creating a Streamlit app with ETL functionality using Python programming language.

### Functionalities implemented in Streamlit Web Application
1. Extract Channel, Playlist and Videos in Playlist from youtube website through REST API.
2. Store the extracted data in MongoDB Atlas server.
3. Transform the data stored in MongoDB Atlas server to SQLite Database
4. Perform Analysis on the Transformed Data

### Requirements to use Youtube Data Harvesting and Warehousing Streamlit web application
1. To Run the Streamlit web application in local machine.
    * User should have to install the required libraries mentioned in _requiements.txt_ file though pip install command
    * User should have an API Key generated in Google console
    * User should enable Youtube Data API V3 in Google console for the accout API Key is generated
    * MongoDB used in this project is free tire Atlas. If user face issue with MongoDB fell free to create use MongoDB installed in local machine.
    * Don't forget to modify MongoDB connection in MongoDB class constructor method in app.py
    
### Screenshot of the Streamlit web application
![alt text](image.png)
