# Import dependencies
import numpy as np
from datetime import datetime, timedelta

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, extract, desc, and_

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///data/hawaii.sqlite")

# reflect an existing database into a new model
Model = automap_base()
# reflect the tables
Model.prepare(engine, reflect=True)

# Save reference to the table
#Measurement = Model.classes.measurement
#Station     = Model.classes.station
class Measurement(Model):
    __tablename__ = 'measurement'

    def __repr__(self):
        return "<{}(station='{}', date='{}', prcp='{}', tobs='{}')>".\
                format(self.__class__.__name__, self.station, self.date, self.prcp, self.tobs)

class Station(Model):
    __tablename__ = 'station'

    def __repr__(self):
        return "<{}(station='{}', name='{}', latitude='{}', longitude='{}', elevation='{}')>".\
                format(self.__class__.__name__, self.station, self.name, self.latitude, self.longitude, self.elevation)

# reflect the tables
Model.prepare(engine, reflect=True)


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f'<a href="/api/v1.0/precipitation">/api/v1.0/precipitation</a><br/>'
        f'<a href="/api/v1.0/stations">/api/v1.0/stations</a><br/>'
        f'<a href="/api/v1.0/tobs">/api/v1.0/tobs</a><br/>'
        f'/api/v1.0/<start> and /api/v1.0/<start>/<end><br/>'
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of dates and percipitation values observed"""
    # Query all dates and percipitation values
    results = session.query(Measurement.date, Measurement.prcp).all()

    session.close()

    # Convert list of tuples into normal list
    #all_names = list(np.ravel(results))

    # Create a dictionary from the row data and append to a list
    response = []
    for date, prcp in results:
        obj = {}
        obj[date] = prcp
        response.append(obj)

    return jsonify(response)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list all stations in the dataset"""
    # Query all stations that are in the measurement table
    results = session.query(Station).\
                      join(Measurement, Measurement.station == Station.station).\
                      group_by(Measurement.station).\
                      all()

    session.close()

    # Create a dictionary from the row data and append to a list
    response = []
    for row in results:
        obj = {}
        obj[row.station] = {
            'name'     : row.name,
            'latitude' : row.latitude,
            'longitude': row.longitude,
            'elevation': row.elevation
        }
        response.append(obj)

    return jsonify(response)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of dates and temperature values observed"""
    # Calculate the date 1 year ago from the last data point in the database
    last_date_reported = session.query(func.max(Measurement.date)).scalar()
    last_year = datetime.strptime(last_date_reported, '%Y-%m-%d') - timedelta(365)

    # Query all dates and temperature values
    station = session.query(Measurement.station, func.count(Measurement.station)).\
                   group_by(Measurement.station).\
                   order_by(desc(func.count(Measurement.station))).\
                   first()[0]

    results = session.query(Measurement.date, Measurement.tobs).\
                      filter(Measurement.station == station).\
                      filter(extract('year', Measurement.date) == extract('year', last_year)).\
                      all()

    session.close()

    # Convert list of tuples into normal list
    #all_names = list(np.ravel(results))

    # Create a dictionary from the row data and append to a list
    response = []
    for date, temp in results:
        obj = {}
        obj[date] = temp
        response.append(obj)

    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)