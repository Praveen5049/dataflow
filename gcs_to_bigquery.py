import apache_beam as beam
import argparse
import logging
import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys


class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage."""
        shard_id, name, department = key_value
        filename = self.output_path
        with io.gcsio.GcsIO().open(filename=filename, mode="wb") as f:
            # for shard_id, name,department in key_value:
            f.write(f"{shard_id},{name},{department}\n".encode("utf-8"))


class Split(beam.DoFn):

    def process(self, element):
        Id, Name, Roll_Number, Department, Date = element
        dict_obj = dict({
            'Id': Id,
            'Name': Name,
            'Roll_Number': Roll_Number,
            'Department': Department,
            'Date': Date,
        })
        print(dict_obj)
        # json_obj = json.loads(json.dumps(dict_obj))
        # 149633CM,Marco,10,Accounts,1-01-2019
        yield json.loads(json.dumps(dict_obj))
        # return dict_obj


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='File to read',
                        default='gs://traning-317210.appspot.com/demo_data.txt')
    parser.add_argument('--temp_location',
                        dest='temp_location',
                        help='temp location',
                        default='gs://traning-317210.appspot.com/tmp')
    parser.add_argument('--output',
                        help=(
                            'Output BigQuery table for results specified as:traning-317210:traning.dataflow'),
                        default=('traning-317210:traning.student'))
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    schema = 'Id:STRING,Name:STRING,Roll_Number:INTEGER,Department:STRING,Date:STRING'
    with beam.Pipeline(options=pipeline_options) as p:
        input_rows = p | "Read from GCS file" >> beam.io.ReadFromText(
            known_args.input)
        event = (
            input_rows
            | 'split row' >> beam.Map(lambda record: record.split(','))
            | 'convert json ' >> beam.ParDo(Split())
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(known_args.output,
                                                           schema=schema,
                                                           method="STREAMING_INSERTS",
                                                           )
        )

        p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
