# Import the dependencies.
import numpy as np
import datetime as dt

# * Import Numpy & Datetime
import numpy as np
import datetime as dt

# * Import SQLalchemy
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc

# * Import Flask
from flask import Flask, jsonify
#################################################
# Database Setup
#################################################
path = "/Users/jessicaborbon/sqlalchemy-challenge/Resources/hawaii.sqlite"
engine = create_engine(f"sqlite:///{path}")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# reflect the tables
measurement = Base.classes.measurement
station = Base.classes.station

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

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
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end"
    )

    @app.route("/api/v1.0/precipitation")
def precipitation():
    # Query for the dates and precipitation values
    results = session.query(Measurement.date, Measurement.prcp).all()
    
    # Create a dictionary from the row data and append to a list of all_precipitation
    all_precipitation = []
    for date, prcp in results:
        precip_dict = {}
        precip_dict["date"] = date
        precip_dict["prcp"] = prcp
        all_precipitation.append(precip_dict)

    return jsonify(all_precipitation)

@app.route("/api/v1.0/stations")
def stations():
    # Query for the stations
    results = session.query(Station.station).all()
    
    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    # Query for the dates and temperature observations of the most active station for the last year of data
    most_active_station = session.query(Measurement.station).group_by(Measurement.station).order_by(desc(func.count(Measurement.station))).first()[0]
    last_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    
    results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station).filter(Measurement.date >= last_year).all()
    
    # Create a list of temperature observations (TOBS)
    tobs_list = []
    for date, tobs in results:
        tobs_dict = {}
        tobs_dict["date"] = date
        tobs_dict["tobs"] = tobs
        tobs_list.append(tobs_dict)

    return jsonify(tobs_list)

@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def temperature_range(start, end=None):
    # Select statement
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]

    if not end:
        results = session.query(*sel).filter(Measurement.date >= start).all()
    else:
        results = session.query(*sel).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    
    # Convert list of tuples into normal list
    temps = list(np.ravel(results))
    
    return jsonify(temps)

if __name__ == "__main__":
    app.run(debug=True)