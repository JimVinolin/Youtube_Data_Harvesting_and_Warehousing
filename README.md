# Youtube_Data_Harvesting_and_Warehousing
The YouTube Data Harvesting and Warehousing project is a Python-based Streamlit app that allows users to access and analyze data from various YouTube channels. The app has features such as inputting a YouTube channel ID and retrieving all relevant data (channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API. The data can be stored in a MongoDB database as a data warehouse and queried with SQL.

The project is designed to demonstrate how to harvest and warehouse YouTube data using SQL, MongoDB, and Streamlit. The first step is to collect data from YouTube using the YouTube Data API. The API provides access to a wide range of data, including channel information, video statistics, and viewer engagement metrics. The collected data can be stored in a variety of ways. In this project, we will use MongoDB and SQL. MongoDB is a NoSQL database that is well-suited for storing large amounts of unstructured data. SQL is a relational database that is well-suited for querying and analyzing structured data. The data can be stored in a data warehouse, which is a centralized repository for data. Data warehouses are used to store large amounts of data from a variety of sources. They can be used to analyze data, identify trends, and make predictions. The data can be analyzed using a variety of tools. In this project, we will use Streamlit. Streamlit is a Python library that can be used to create interactive web applications. We will use Streamlit to create a dashboard that allows users to visualize and analyze the data.

#Used Toolset for this project:
#YouTube API Calling
The project leverages the YouTube API to fetch essential metadata, statistics, and comments from YouTube videos, channels, and playlists. The API enables seamless interaction with YouTube's vast database, allowing users to access up-to-date and accurate information about the content they are interested in.

#Python
Python serves as the primary programming language for this project. Its versatility and rich ecosystem of libraries make it an ideal choice for tasks ranging from data harvesting to data warehousing and analytics. The project utilizes Python for scripting data retrieval, managing data storage, and implementing the analytics dashboard.

#MongoDB
MongoDB is employed as the NoSQL database for storing harvested YouTube data. Its flexible schema design accommodates the dynamic nature of YouTube data, providing scalability and ease of integration. MongoDB's document-oriented storage enables efficient handling of diverse information such as video details, channel information, and user comments.

#PostgreSQL (PSQL)
PostgreSQL, often referred to as PSQL, is utilized as a relational database management system (RDBMS) to serve as the centralized data warehouse. PSQL excels in handling structured data and supports complex queries, making it an excellent choice for storing and organizing the YouTube data in a structured and efficient manner.

#Streamlit
Streamlit is employed to create a user-friendly and interactive analytics dashboard. With Streamlit, users can explore, analyze, and visualize YouTube data effortlessly through a web-based interface. Its simplicity and ease of use make it an excellent tool for rapidly developing data-driven applications, providing a seamless experience for researchers, content creators, and enthusiasts alike.







