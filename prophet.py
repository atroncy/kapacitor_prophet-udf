import sys
import json
from kapacitor.udf.agent import Agent, Handler, Server
from kapacitor.udf import udf_pb2
import signal

from fbprophet import Prophet

import pandas as pd

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()


class ProphetHandler(Handler):

    class state(object):
        def __init__(self):
            self._entries = []

        def reset(self):
            self._entries = []

        def update(self, value, ds):
            self._entries.append((value, ds)) 
	
        def get_entries(self):
            return self._entries
	
    def __init__(self, agent):
        self._agent = agent
        self._field = None
        self._periods = 0
        self._freq = None
        self._changepoint_prior_scale = None
        self._growth = None
        self._cap = None
        self._floor = None
        self._include_history = None
        self._state  = ProphetHandler.state()
        self._begin_response = None


    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.BATCH
        response.info.provides = udf_pb2.BATCH
        response.info.options['field'].valueTypes.append(udf_pb2.STRING)
        response.info.options['periods'].valueTypes.append(udf_pb2.INT)
        response.info.options['freq'].valueTypes.append(udf_pb2.STRING)
        response.info.options['changepoint_prior_scale'].valueTypes.append(udf_pb2.DOUBLE)
        response.info.options['growth'].valueTypes.append(udf_pb2.STRING)
        response.info.options['cap'].valueTypes.append(udf_pb2.DOUBLE)
        response.info.options['floor'].valueTypes.append(udf_pb2.DOUBLE)
        response.info.options['include_history'].valueTypes.append(udf_pb2.BOOL)
        return response

    def init(self, init_req):
        success = True
        msg = ''

        for opt in init_req.options:
            if opt.name == 'field':
                self._field = opt.values[0].stringValue
            elif opt.name == 'periods':	
                self._periods = opt.values[0].intValue
            elif opt.name == 'cap':	
                self._cap = opt.values[0].doubleValue
            elif opt.name == 'floor':
                self._floor = opt.values[0].doubleValue
            elif opt.name == 'growth':	
                self._growth = opt.values[0].stringValue
            elif opt.name == 'freq':	
                self._freq = opt.values[0].stringValue
            elif opt.name == 'changepoint_prior_scale':	
                self._changepoint_prior_scale = opt.values[0].doubleValue
            elif opt.name == 'include_history':	
                self._include_history = opt.values[0].boolValue

        if self._field is None:
            success = False
            msg += ' must supply field'
        if self._periods <= 0:
            success = False
            msg += ' periods must be > to 0'

        response = udf_pb2.Response()
        response.init.success = success
        response.init.error = msg[1:]
        logger.info('init %s', msg)
        return response

    def snapshot(self):
        response = udf_pb2.Response()
        response.snapshot.snapshot = ''
        return response

    def restore(self, restore_req):
        response = udf_pb2.Response()
        response.restore.success = False
        response.restore.error = 'not implemented'
        return response

    def begin_batch(self, begin_req):
        self._state.reset()

        response = udf_pb2.Response()
        response.begin.CopyFrom(begin_req)
        self._begin_response = response
        logger.info('begin batch')

    def point(self, point):
        value = point.fieldsDouble[self._field]
        self._state.update(value, point.time)

    def end_batch(self, end_req):
        entries = self._state.get_entries()
	
        ds = []
        y = []
        for a, b in entries:
            ds.append(b)
            y.append(a)

        d = {'y': y, 'ds': ds}
        df = pd.DataFrame(d)

        if self._cap is not None:
            df['cap'] = self._cap
            if self._floor is not None:
                df['floor'] = self._floor

        m = None
        if self._changepoint_prior_scale is not None and self._growth is not None:
            m = Prophet(changepoint_prior_scale=self._changepoint_prior_scale, growth=self._growth)
        elif self._changepoint_prior_scale is not None:
            m = Prophet(changepoint_prior_scale=self._changepoint_prior_scale)
        elif self._growth is not None:
            m = Prophet(growth=self._growth)
        else:
            m = Prophet()

        logger.info('fit model')
        m.fit(df)

        future = None
        if self._freq is not None and self._include_history is not None:
            future = m.make_future_dataframe(periods=self._periods, include_history=self._include_history, freq=self._freq)
        elif self._freq is not None:
            future = m.make_future_dataframe(periods=self._periods, freq=self._freq)
        elif self._include_history is not None:
            future = m.make_future_dataframe(periods=self._periods, include_history=self._include_history)
        else:
            future = m.make_future_dataframe(periods=self._periods)

        if self._cap is not None:
            future['cap'] = self._cap
            if self._floor is not None:
                future['floor'] = self._floor

        forecast = m.predict(future)
        logger.info('forecasted')
        self._begin_response.begin.size = forecast.size
        self._agent.write_response(self._begin_response)

        response = udf_pb2.Response()
        for index, rows in forecast.iterrows():
            point = {'yhat': rows['yhat'], 'yhat_lower': rows['yhat_lower'], 'yhat_upper': rows['yhat_upper']}
            # TODO this look bad :)
            response.point.time = int(rows['ds'].timestamp()) * 1000000000
            response.point.fieldsDouble['yhat'] = rows['yhat']
            response.point.fieldsDouble['yhat_upper'] = rows['yhat_upper']
            response.point.fieldsDouble['yhat_lower'] = rows['yhat_lower']
            self._agent.write_response(response)

        response.end.CopyFrom(end_req)
        self._agent.write_response(response)
        logger.info('ending batch')

class accepter(object):
    _count = 0
    def accept(self, conn, addr):
        self._count += 1
        a = Agent(conn, conn)
        h = ProphetHandler(a)
        a.handler = h

        logger.info("Starting Agent for connection %d", self._count)
        a.start()
        a.wait()
        logger.info("Agent finished connection %d",self._count)

if __name__ == '__main__':
    path = "/tmp/udf_prophet.sock"
    if len(sys.argv) == 2:
        path = sys.argv[1]
    server = Server(path, accepter())
    logger.info("Started server")
server.serve()
