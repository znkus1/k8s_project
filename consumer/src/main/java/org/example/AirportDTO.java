package org.example;

import java.time.LocalDateTime;

public class AirportDTO {
    //    id SERIAL PRIMARY KEY,
    public String Year;
    public String Month;
    public String DayofMonth;
    public String DayOfWeek;
    public String DepTime;
    public String CRSDepTime;
    public String ArrTime;
    public String CRSArrTime;
    public String UniqueCarrier;
    public String FlightNum;
    public String TailNum;
    public String  ActualElapsedTime;
    public String CRSElapsedTime;
    public String AirTime;
    public String ArrDelay;
    public String DepDelay;
    public String Origin;
    public String Dest;
    public String Distance;
    public String TaxiIn;
    public String TaxiOut;
    public String Cancelled;
    public String CancellationCode;
    public String Diverted;
    public String CarrierDelay;
    public String WeatherDelay;
    public String NASDelay;
    public String SecurityDelay;
    public String LateAircraftDelay;
    public LocalDateTime insert_dt;

    public AirportDTO(String year, String month, String dayofMonth, String dayOfWeek, String depTime, String CRSDepTime, String arrTime, String CRSArrTime, String uniqueCarrier, String flightNum, String tailNum, String actualElapsedTime, String CRSElapsedTime, String airTime, String arrDelay, String depDelay, String origin, String dest, String distance, String taxiIn, String taxiOut, String cancelled, String cancellationCode, String diverted, String carrierDelay, String weatherDelay, String NASDelay, String securityDelay, String lateAircraftDelay, LocalDateTime insert_dt) {
        this.Year = year;
        Month = month;
        DayofMonth = dayofMonth;
        DayOfWeek = dayOfWeek;
        DepTime = depTime;
        this.CRSDepTime = CRSDepTime;
        ArrTime = arrTime;
        this.CRSArrTime = CRSArrTime;
        UniqueCarrier = uniqueCarrier;
        FlightNum = flightNum;
        TailNum = tailNum;
        ActualElapsedTime = actualElapsedTime;
        this.CRSElapsedTime = CRSElapsedTime;
        AirTime = airTime;
        ArrDelay = arrDelay;
        DepDelay = depDelay;
        Origin = origin;
        Dest = dest;
        Distance = distance;
        TaxiIn = taxiIn;
        TaxiOut = taxiOut;
        Cancelled = cancelled;
        CancellationCode = cancellationCode;
        Diverted = diverted;
        CarrierDelay = carrierDelay;
        WeatherDelay = weatherDelay;
        this.NASDelay = NASDelay;
        SecurityDelay = securityDelay;
        LateAircraftDelay = lateAircraftDelay;
        this.insert_dt = insert_dt;
    }

    @Override
    public String toString() {
        return "AirportDTO{" +
                "Year='" + Year + '\'' +
                ", Month='" + Month + '\'' +
                ", DayofMonth='" + DayofMonth + '\'' +
                ", DayOfWeek='" + DayOfWeek + '\'' +
                ", DepTime='" + DepTime + '\'' +
                ", CRSDepTime='" + CRSDepTime + '\'' +
                ", ArrTime='" + ArrTime + '\'' +
                ", CRSArrTime='" + CRSArrTime + '\'' +
                ", UniqueCarrier='" + UniqueCarrier + '\'' +
                ", FlightNum='" + FlightNum + '\'' +
                ", TailNum='" + TailNum + '\'' +
                ", ActualElapsedTime='" + ActualElapsedTime + '\'' +
                ", CRSElapsedTime='" + CRSElapsedTime + '\'' +
                ", AirTime='" + AirTime + '\'' +
                ", ArrDelay='" + ArrDelay + '\'' +
                ", DepDelay='" + DepDelay + '\'' +
                ", Origin='" + Origin + '\'' +
                ", Dest='" + Dest + '\'' +
                ", Distance='" + Distance + '\'' +
                ", TaxiIn='" + TaxiIn + '\'' +
                ", TaxiOut='" + TaxiOut + '\'' +
                ", Cancelled='" + Cancelled + '\'' +
                ", CancellationCode='" + CancellationCode + '\'' +
                ", Diverted='" + Diverted + '\'' +
                ", CarrierDelay='" + CarrierDelay + '\'' +
                ", WeatherDelay='" + WeatherDelay + '\'' +
                ", NASDelay='" + NASDelay + '\'' +
                ", SecurityDelay='" + SecurityDelay + '\'' +
                ", LateAircraftDelay='" + LateAircraftDelay + '\'' +
                ", insert_dt=" + insert_dt +
                '}';
    }
}
