# Import the dependencies.
import datetime as dt
import numpy as np
import pandas as pd

from flask import Flask, jsonify

from sqlalchemy import create_engine, func, desc
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
Base.prepare(engine, reflect=True)
# reflect the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Save references to each table


# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start (replace start with selected date in yyyy-mm-dd format)<br/>"
        f"/api/v1.0/start/end (replace start/end with selected date in yyyy-mm-dd format) <br/>"
     )
     

@uteapp.ro("/api/v1.0/precipitation")
def precipitation():
    # Find the most recent date in the dataset
    last_date_str = session.query(func.max(Measurement.date)).scalar()
    last_date = dt.datetime.strptime(last_date_str, '%Y-%m-%d').date()

    # Calculate the date one year ago from the most recent date
    one_year_ago = last_date - dt.timedelta(days=365)

    # Query to retrieve the last 12 months of precipitation data
    query = (session.query(Measurement.date, Measurement.prcp)
             .filter(Measurement.date >= str(one_year_ago))
             .order_by(Measurement.date))

    data = query.all()

    # Convert the query results to a dictionary
    precipitation_data = [{date: prcp} for date, prcp in data]

    return jsonify(precipitation_data)

@app.route("/api/v1.0/stations")
def active_stations():
    # Query all stations
#    results = session.query(Station.station).all()

    # Design a query to calculate the total number of stations in the dataset
    query_stations_count = session.query(Measurement.station).distinct().count()

    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
    query = (session.query(Measurement.station, func.count(Measurement.id).label('count'))
         .group_by(Measurement.station)
         .order_by(desc('count')))
    
    # Execute the query and fetch all results
    active_stations = query.all()

    active_stations_data = []
    for station, count in active_stations:
        active_stations_data.append({"station": station, "count": count})

    return jsonify(active_stations_data)
    
@app.route("/api/v1.0/tobs")
def tobs():
    # Session query of Measurement table for date and tobs of most active station of the most recent year
    tobs_date = session.query(Measurement.date, Measurement.tobs).all()

    # Found unique station names, counted them, and sort them
    distinct_station_ct = session.query(Measurement.station, func.count(Measurement.station).label("Station Activity")).group_by(Measurement.station)
    sorted_station_list = distinct_station_ct.order_by(desc("Station Activity")).all()
    
    most_active_station = sorted_station_list[0][0]

    # Find most recent date
    recent_date_query = session.query(func.max(Measurement.date)).all()
    max_date = recent_date_query[0][0]

    max_date_list = max_date.split("-")
    starting_date = dt.date(int(max_date_list[0]), int(max_date_list[1]), int(max_date_list[2]))

    one_year = dt.timedelta(days = 366)

    one_year_prior = starting_date - one_year
    
    # Query the most active station name date and tobs
    active_station_query = session.query(Measurement.station, Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station).filter(Measurement.date > one_year_prior)

    # active_station_query_list = [data for data in active_station_query]

    state_date_tobs_list = []
    for station, date, tobs in active_station_query:
        tobs_dates_station_dict = {}
        tobs_dates_station_dict["station"] = station
        tobs_dates_station_dict["date"] = date
        tobs_dates_station_dict["temperature observed"] = tobs
        state_date_tobs_list.append(tobs_dates_station_dict)
    
    return jsonify(state_date_tobs_list)


@app.route("/api/v1.0/<start>")
def start_date(start):
    session = Session(engine)
    
    try:
        # Convert string date to datetime date
        start_date = dt.datetime.strptime(start, "%Y-%m-%d").date()
    except ValueError:
        session.close()
        return jsonify({"error": "Date format must be YYYY-MM-DD"}), 400

    # Query to calculate the lowest, highest, and average temperature from the start date
    temperature_query = (
        session.query(
            func.min(Measurement.tobs).label('min_temp'),
            func.max(Measurement.tobs).label('max_temp'),
            func.avg(Measurement.tobs).label('avg_temp')
        )
        .filter(Measurement.date >= start_date)
    )

    # Execute the query and fetch the results
    temperature_stats = temperature_query.one()

    # Get the most recent date in the dataset
    most_recent_date_set = session.query(func.max(Measurement.date)).first()[0]

    session.close()

    # Create a dictionary from the data
    temps_dict = {
        'start_date': str(start_date),
        'end_date': most_recent_date_set,
        'min_temp': temperature_stats.min_temp,
        'max_temp': temperature_stats.max_temp,
        'avg_temp': temperature_stats.avg_temp
    }

    return jsonify(temps_dict)


@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start, end):
    session = Session(engine)

    try:
        # Parse the start and end dates
        start_date = dt.datetime.strptime(start, "%Y-%m-%d").date()
        end_date = dt.datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        session.close()
        return jsonify({"error": "Date format must be YYYY-MM-DD"}), 400

    # Query to calculate the lowest, highest, and average temperature between the start and end dates
    temperature_query = (
        session.query(
            func.min(Measurement.tobs).label('min_temp'),
            func.max(Measurement.tobs).label('max_temp'),
            func.avg(Measurement.tobs).label('avg_temp')
        )
        .filter(Measurement.date >= start_date)
        .filter(Measurement.date <= end_date)
    )

    # Execute the query and fetch the results
    temperature_stats = temperature_query.one()

    session.close()

    # Create a dictionary from the data
    temps_dict = {
        'start_date': str(start_date),
        'end_date': str(end_date),
        'min_temp': temperature_stats.min_temp,
        'max_temp': temperature_stats.max_temp,
        'avg_temp': temperature_stats.avg_temp
    }

    return jsonify(temps_dict)

if __name__ == '__main__':
    app.run(debug=True)