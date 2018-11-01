a udf (batch) for kapacitor using prophet  

[facebook prophet doc](https://github.com/facebook/prophet)  
[main thread on influxdata community](https://community.influxdata.com/t/prophet-forecasting-udf-unable-to-write-response-to-influxdb/6348)  

it's a unix socket based udf.  
by default it use /tmp/udf_prophet.sock , make sure you give enough read/write right for kapacitor daemon/process to interact with the file.  
you can override the file location as the first argument running this script.  
(i was able to use run it on docker and/or kubernetes)

Inspired by this talk, by the creator of kapacitor.  

[![Nathaniel Cook - Forecasting Time Series Data at scale with the TICK stack](http://img.youtube.com/vi/raEyZEryC0k/0.jpg)](http://www.youtube.com/watch?v=raEyZEryC0k)

InfluxDb support by default holt_winters for forecasting timeseries.  
Note that you need to specify the seasonality pattern to forecast with holt_winters, without the right seasonality pattern your forcasted value may endup being not that usefull.  
[HOLT_WINTERS()](https://docs.influxdata.com/influxdb/v1.6/query_language/functions/#holt-winters)  
Finding the seasonality pattern of a timeseries is not that easy ! :).  
Prophet on the other hand try to find the seasonal pattern it self, and is pretty good a it.  

Using it with a system like kapacitor make sense, since you timeseries and seasonality pattern may change in the future.  
And unless you have enough time/people to track those change, you had rather have something else doing it for you !  


[another nice article about forecasting jobs, if you are curious!](https://eng.uber.com/forecasting-introduction/)  
