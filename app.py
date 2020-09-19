from flask import Flask,render_template,request,redirect
# from flask_sqlalchemy import SQLAlchemy
import get_Sqlite_data
app = Flask(__name__)

# app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///hawaii.sqlite"

@app.route('/homepage')
def index():
    return render_template("index.html")

@app.route('/homepage/api/v1.0/precipitation',methods=['GET','POST'])
def precipitation():
    if request.method=='POST':
        return redirect('/homepage')
    prcpt_data = get_Sqlite_data.precipitation_data()
    prcpt_data['precipitation']= round(prcpt_data['precipitation'],3)
    result = dict(zip(prcpt_data.date,prcpt_data.precipitation))
    return render_template("precipitation.html",result=result)

@app.route('/homepage/api/v1.0/stations',methods=['GET','POST'])
def stations():
    if request.method=='POST':
        return redirect('/homepage')
    station,station_count =get_Sqlite_data.stations()
    stations = station.to_dict('r')
    print("Total Distinct Station count",station_count[0])
    return render_template("stations.html",stations=stations)

@app.route('/homepage/api/v1.0/tobs',methods=['GET','POST'])
def tobs():
    if request.method=='POST':
        return redirect('/homepage')
    most_active_station,Station_name = get_Sqlite_data.tobs()
    return render_template("tobs.html",most_active_station=most_active_station.to_dict('r'),Station_name=Station_name)

@app.route('/homepage/api/v1.0/',methods=['GET','POST'])
def startAndEndDate():
    if request.method=='POST':
        return redirect('/homepage')
    calc_temps, Start_date, End_date = get_Sqlite_data.calc_temps('2012-02-28', '2012-03-05')
    Single_date_calcul,Single_Start_date=get_Sqlite_data.calc_temps_with_oneDate('2012-02-28')
    return render_template("startAndEndDate.html",calc_temps=calc_temps,Start_date=Start_date,End_date=End_date,Single_date_calcul=Single_date_calcul,Single_Start_date=Single_Start_date)



if __name__=="__main__":
    app.run(debug=True)
