import apache_beam as beam
import argparse
import logging
import csv
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='File to read',
                        default='demo_data.txt')
    parser.add_argument('--output',
                        dest='output',
                        help='Outputfilename',
                        default='data/output')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        input_rows = p | "Read from TXT" >> beam.io.ReadFromText(known_args.input)

        filter_accounts = (
                input_rows
                | beam.Map(lambda record: record.split(','))
                # |beam.Filter(filtering)
                | beam.Filter(lambda record: record[3] == 'Accounts')
        )

        #####################################
        ## Get Distinct Employees belonging to Accounts Department
        unique_employees = (
                filter_accounts
                | beam.Map(lambda record: (record[1], 1))
                | beam.CombinePerKey(sum)
                | beam.Map(lambda record: record[0])
        )
        # p1=(unique_employees |"Write to Output">>beam.io.WriteToText(known_args.output))
        unique_department = (
                input_rows
                | 'split records' >> beam.Map(lambda record: record.split(','))
                | 'list department' >> beam.Map(lambda record: (record[3], 1))
                | 'sum departments' >> beam.CombinePerKey(sum)
                | 'Count unique department' >> beam.combiners.Count.PerElement()
        )
        p1 = (unique_department | "Write to Output" >> beam.io.WriteToText(known_args.output))
        #####################################################

        ######################  YOUR CODE HERE ##############
        ## Obtain Number of Employees belonging to each department and save in a separate output file

        ######################################################

        p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()