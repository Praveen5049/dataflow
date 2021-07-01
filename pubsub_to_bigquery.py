import apache_beam as beam
import argparse
import logging,json
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from apache_beam import window

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
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/traning-317210/subscriptions/student_information_subscriber'
                        )
    parser.add_argument('--outputTopic',
                        help=(
                            'Output BigQuery table for results specified as:traning-317210:traning.dataflow'),
                        default=('traning-317210:traning.student'))
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    schema = 'Id:STRING,Name:STRING,Roll_Number:INTEGER,Department:STRING,Date:STRING'
    with beam.Pipeline(options=pipeline_options) as p:
        pubsub_data = (
                    p
                    | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= known_args.inputSubscription)
                    | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))
                    | 'Split Row' >> beam.Map(lambda row : row.decode().split(','))
                    | 'convert json ' >> beam.ParDo(Split())
                    | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(known_args.outputTopic,
                                                           schema=schema,
                                                           method="STREAMING_INSERTS",
                                                           )
                )
        p.run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()