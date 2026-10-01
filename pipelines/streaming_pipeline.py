"""Pub/Sub -> 1-minute windowed revenue by region -> BigQuery (streaming inserts)."""
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, AfterWatermark

from pipelines.transforms import InvalidRecord, parse_json_message, validate

SCHEMA = "window_start:TIMESTAMP,region:STRING,orders:INTEGER,revenue:NUMERIC"


def safe_parse(msg: bytes):
    try:
        yield validate(parse_json_message(msg))
    except InvalidRecord:
        beam.metrics.Metrics.counter("orders", "invalid").inc()


class FormatRow(beam.DoFn):
    def process(self, kv, win=beam.DoFn.WindowParam):
        region, (orders, revenue) = kv
        yield {
            "window_start": win.start.to_utc_datetime().isoformat(),
            "region": region,
            "orders": orders,
            "revenue": round(revenue, 2),
        }


def run(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--input_topic", required=True)
    p.add_argument("--output_table", required=True)
    args, beam_args = p.parse_known_args(argv)
    opts = PipelineOptions(beam_args)
    opts.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=opts) as pipe:
        (
            pipe
            | "ReadPubSub" >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | "Parse" >> beam.FlatMap(safe_parse)
            | "KeyByRegion" >> beam.Map(lambda r: (r["region"], (1, r["amount"])))
            | "Window" >> beam.WindowInto(
                window.FixedWindows(60),
                trigger=AfterWatermark(late=AfterProcessingTime(30)),
                allowed_lateness=300,
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
            | "Sum" >> beam.CombinePerKey(lambda vals: tuple(map(sum, zip(*vals))))
            | "Format" >> beam.ParDo(FormatRow())
            | "WriteBQ" >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=SCHEMA,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )


if __name__ == "__main__":
    run()
