# Import dependencies
import numpy as np
from datetime import datetime, timedelta

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, extract, desc, and_

# Python micro web framework
from flask import Flask, jsonify, render_template, abort

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
@app.route('/', methods=['GET'])
def home():
    """List all available api routes."""
    return render_template('index.html')


@app.route('/api/v1.0/precipitation',  methods=['GET'])
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of dates and percipitation values observed"""
    # Retrieve last year in the database
    last_year = session.query( extract('year', func.max(Measurement.date)) ).scalar()

    # Query all dates and percipitation values
    results = session.query(Measurement.date, Measurement.prcp).\
                      filter(extract('year', Measurement.date) == last_year).\
                      all()

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

@app.route('/api/v1.0/stations', methods=['GET'])
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

@app.route('/api/v1.0/tobs', methods=['GET'])
def temperatures():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of dates and temperature values observed"""
    # Calculate the date 1 year ago from the last data point in the database
    last_year = session.query( extract('year', func.max(Measurement.date)) ).scalar()

    # Uncomment the two lines below if you want the previous year data
    #last_date_reported = session.query(func.max(Measurement.date)).scalar()
    #last_year = extract('year', datetime.strptime(last_date_reported, '%Y-%m-%d') - timedelta(365))

    # Query all dates and temperature values
    station = session.query(Measurement.station, func.count(Measurement.station)).\
                   group_by(Measurement.station).\
                   order_by(desc(func.count(Measurement.station))).\
                   first()[0]

    results = session.query(Measurement.date, Measurement.tobs).\
                      filter(and_(Measurement.station == station,
                                  extract('year', Measurement.date) == last_year)
                            ).\
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

@app.route('/api/v1.0/<start>',       methods=['GET'])
@app.route('/api/v1.0/<start>/<end>', methods=['GET'])
def by_date(start, end=None):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a JSON list of minimum, maximum, and average temperatures by date range"""

    start_date = datetime.strptime(start, '%Y-%m-%d')

    if (end == None):
        results = session.query(
            Measurement.date,
            func.min(Measurement.tobs), 
            func.avg(Measurement.tobs),
            func.max(Measurement.tobs)
            ).\
            group_by(Measurement.date).\
            filter(Measurement.date >= start_date).\
            all()
    else:
        end_date = datetime.strptime(end, '%Y-%m-%d')
 
        results = session.query(
                Measurement.date,
                func.min(Measurement.tobs), 
                func.avg(Measurement.tobs),
                func.max(Measurement.tobs)
            ).\
            group_by(Measurement.date).\
            filter(Measurement.date.between(start_date, end_date)).\
            all()

    #filter(and_(Measurement.date <= end_date, Measurement.date >= start_date)).\
    session.close()

    if (len(results)):
        response = [ { row[0]: { 'min': row[1], 'avg': row[2], 'max': row[3] }} for row in results ]
    else:
        abort(404)

    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)