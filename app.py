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
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
    )

@app.route("/api/v1.0/precipitation")
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
    # Find the most recent date in the dataset
    last_date_str = session.query(func.max(Measurement.date)).scalar()
    last_date = dt.datetime.strptime(last_date_str, '%Y-%m-%d').date()

    # Calculate the date one year ago from the most recent date
    one_year_ago = last_date - dt.timedelta(days=365)

    # Query the most active station
    most_active_station = session.query(Measurement.station)\
        .group_by(Measurement.station)\
        .order_by(func.count(Measurement.station).desc())\
        .first()[0]

    # Query the dates and temperature observations of the most active station for the last year
    results = session.query(Measurement.date, Measurement.tobs)\
        .filter(Measurement.station == most_active_station)\
        .filter(Measurement.date >= one_year_ago)\
        .all()

    # Convert the query results to a list of temperature observations
    tobs_data = list(np.ravel(results))

    return jsonify(tobs_data)

@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def temperature_range(start=None, end=None):
    # Define the selection criteria
    sel = [
        func.min(Measurement.tobs),
        func.avg(Measurement.tobs),
        func.max(Measurement.tobs)
    ]

    if not end:
        # Calculate TMIN, TAVG, and TMAX for all dates >= start
        results = session.query(*sel).filter(Measurement.date >= start).all()
    else:
        # Calculate TMIN, TAVG, and TMAX for dates between start and end
        results = session.query(*sel).filter(Measurement.date >= start).filter(Measurement.date <= end).all()

    # Convert the query results to a list
    temperature_data = list(np.ravel(results))

    return jsonify(temperature_data)

if __name__ == "__main__":
    app.run(debug=True)
