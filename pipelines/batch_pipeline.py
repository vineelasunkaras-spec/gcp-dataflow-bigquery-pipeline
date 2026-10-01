"""GCS CSV -> validated rows -> BigQuery, with a dead-letter output."""
import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from pipelines.transforms import InvalidRecord, parse_csv_line, validate

SCHEMA = "order_id:STRING,customer_id:STRING,region:STRING,amount:NUMERIC,currency:STRING,order_ts:TIMESTAMP"


class ParseAndValidate(beam.DoFn):
    DEAD = "dead_letter"

    def process(self, line):
        try:
            yield validate(parse_csv_line(line))
        except InvalidRecord as e:
            yield beam.pvalue.TaggedOutput(self.DEAD, json.dumps({"raw": line, "error": str(e)}))


def run(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output_table", required=True)
    p.add_argument("--dead_letter", default="dead_letter/orders")
    args, beam_args = p.parse_known_args(argv)

    with beam.Pipeline(options=PipelineOptions(beam_args)) as pipe:
        results = (
            pipe
            | "Read" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "ParseValidate" >> beam.ParDo(ParseAndValidate()).with_outputs(ParseAndValidate.DEAD, main="valid")
        )
        results.valid | "ToBigQuery" >> beam.io.WriteToBigQuery(
            args.output_table,
            schema=SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
        results[ParseAndValidate.DEAD] | "DeadLetter" >> beam.io.WriteToText(args.dead_letter, file_name_suffix=".jsonl")


if __name__ == "__main__":
    run()
