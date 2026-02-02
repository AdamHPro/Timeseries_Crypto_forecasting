# Timeseries_Cryptos_forecasting

Building an ETL with an API and a Frontend to use a timeseries forecasting model to predict cryptocurrency prices.

## Run the project:

1. Clone the repository:

   ```bash
   git clone
      cd Timeseries_Crypto_forecasting
   ```

2. copy the .env.example to .env (and modify the variables if needed):

   ```bash
   cp .env.example .env
   ```

3. Build and run the Docker containers:

   ```bash
   docker-compose up --build
   ```

4. Access the frontend application at `http://localhost:3000`.

## Stack

- Backend: Python
- Pandas
- Numpy
- Matplotlib
- Scikit-learn
- TensorFlow/Keras
- Jupyter Notebook
- Frontend: React.js
- Orchestration: Flask
- Database: PostgreSQL
- Deployment: Docker
- Version Control: Git/GitHub
- Cloud: AWS/GCP

## Learning objectives

- Understand the fundamentals of time series analysis and forecasting.
- Gain hands-on experience with LSTM neural networks for time series prediction.
- Learn to preprocess and visualize time series data.
- Develop skills in building and deploying machine learning models using Python and relevant libraries.
- Explore the application of deep learning techniques in financial markets, specifically cryptocurrency price prediction.
- Advise on best practices for model evaluation and performance tuning in time series forecasting tasks.
- Understand the challenges and considerations when working with cryptocurrency data, such as volatility and market trends.
- Gain experience in integrating machine learning models into web applications for real-time predictions.
- Learn to use Docker for containerization and deployment of machine learning applications.
- Explore cloud platforms for hosting and scaling machine learning applications.

## Project Duration

The project is expected to take approximately 8-12 weeks, depending on the complexity of the model and the depth of analysis required.

## Structure

- `api/`: Contains the FastAPI code for serving the model.
- `etl/`: Contains scripts for data extraction, transformation, and loading.
- `front/`: Contains the React.js frontend code.
- db: Postgresql database for storing historical cryptocurrency data.

## Architecture

The architecture of the project consists of the following components:

1. Data Collection: Using APIs to gather new recent historical cryptocurrency data (comparison with the actual database, and upsert in the db). Store the new data in data_lake parquet. Train the model with the new data from data_lake. (/etl)

2. Put the result in a volume, accesible by /api

3. Model Serving: FastAPI application to serve the trained model and provide predictions via RESTful endpoints. (/api)

4. Frontend Interface: React.js application to provide a user-friendly interface for interacting with the model and visualizing predictions. (/front)

5. Database: PostgreSQL database to store historical cryptocurrency data and model predictions. (db)
