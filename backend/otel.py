#!/bin/env python3.11

__doc__ = '''Backend for the OTel protocol.'''

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

otel_backend_name='trc2db'
otel_backend_version='0.2'

OTEL_IGNORED_OPS = ('STAR', 'HEADER')

class OraId(IdGenerator):
    '''Custom id generator for traces and spans. Allows setting the trace id to something that was
        passed to the database by the client.'''
    def __init__(self):
        super().__init__()
        self.span_id = None
        self.trace_id = None
    def set_trace_id(self, tid):
        self.trace_id = tid
    def set_span_id(self, sid):
        self.span_id = sid
    def generate_trace_id(self):
        return self.trace_id
    def generate_span_id(self):
        return self.trace_id
        #return super().generate_span_id()

class Backend:
    def __init__(self, trace_id_name):
        self.trace_id_name = trace_id_name
        self._exec_id = 0
        self.id_gen = OraId()
        self.resources = Resource(attributes={
            "service.name": otel_backend_name
        })
        self.provider = TracerProvider(resource=self.resources, id_generator=self.id_gen)
        self.processor = BatchSpanProcessor(OTLPSpanExporter())
        self.provider.add_span_processor(self.processor)
        trace.set_tracer_provider(self.provider)
    def get_exec_id(self):
        self._exec_id += 1
        return self._exec_id
    def add_ops(self, exec_id, sql_id, ops):
        dops = [o.to_dict(exec_id, sql_id) for o in ops if o.op_type not in OTEL_IGNORED_OPS]
        if len(dops) == 0:
            return
        trace_id = dops[0][self.trace_id_name]
        cursor = dops[0]['cursor']
        ts = dops[0]['ts']
        self.id_gen.set_trace_id(int(trace_id))
        self.id_gen.set_span_id(exec_id)
        tracer = trace.get_tracer(otel_backend_name, otel_backend_version)
        sp = tracer.start_span(str(exec_id), start_time=int(ts.timestamp()*1000000000))
        last_ts = int()
        for o in dops:
            if 'ts' in o.keys():
                ts = int(o['ts'].timestamp()*1000000000)
                o['ts'] = ts
                sp.add_event(o['op_type'], o, ts)
                last_ts = ts
            else:
                sp.add_event(o['op_type'], o, last_ts)
        sp.end(last_ts)
    def flush(self):
        pass
