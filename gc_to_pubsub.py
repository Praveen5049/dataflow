import apache_beam as beam
import argparse
import logging
import json
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


class Split(beam.DoFn):
    def process(self, element):
        Id, Name, Department = element
        dict_obj = dict({
            'Id': Id,
            'Name': Name,
            'Department': Department,
        })
        print(dict_obj)
        yield json.dumps(dict_obj).encode('utf-8')


def custom_timestamp(elements):
    unix_timestamp = elements[7]
    return beam.window.TimestampedValue(elements, int(unix_timestamp))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='File to read',
                        default='gs://traning-317210.appspot.com/demo_data.txt')
    parser.add_argument('--outputTopic',
                        dest='outputTopic',
                        help='output topic name',
                        default='projects/traning-317210/topics/student_information_topic'
                        )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True
    window_size = 1.0
    with beam.Pipeline(options=pipeline_options) as p:
        ######################## CODE TO SEND GCS FILE TO PUBSUB ################
        input_rows = p | "Read from GCS file" >> beam.io.ReadFromText(
            known_args.input)
        event = (
                input_rows
                | 'split row' >> beam.Map(lambda record: record.split(','))
                | 'Map wanted columns' >> beam.Map(lambda record: (record[0], record[1], record[3]))
                | 'convert json ' >> beam.ParDo(Split())
                | 'Window' >> beam.WindowInto(window.SlidingWindows(30, 10))
                | 'Write to pus sub' >> beam.io.WriteToPubSub(known_args.outputTopic)
        )
        ########################################################
        p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
