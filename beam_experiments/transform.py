import argparse
import logging

import apache_beam as beam
import json
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from flatten_json import flatten


class JsonCoder(object):
  """A JSON coder interpreting each line as a JSON string."""

  def encode(self, x):
    return json.dumps(x)

  def decode(self, x):
    return json.loads(x)


class extractMetrics(beam.DoFn):

    def process(self, element):
        """
        Returns a list of tuples containing country and duration
        """
        if 'metrics_conversionMetrics' in element:
           for conversion in element['metrics_conversionMetrics']:
             name = conversion.get('name').lower()
             element[name + 'totalConversions'] = conversion.get('totalConversions')
             element[name + 'sumConversions'] = conversion.get('sumConversions')
             element[name + 'cpc'] = conversion.get('cpc')
             element[name + 'spend'] = conversion.get('spend')
             element[name + 'impression'] = conversion.get('impression')
           del element['metrics_conversionMetrics']
        else:
          None
        return [element]




def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='input',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> ReadFromText(known_args.input, coder=JsonCoder())

  counts = (lines
            | "extract_campaigns" >> beam.ParDo(lambda list : list)
            | 'extract_metrics' >> beam.ParDo(extractMetrics())
            | 'write' >> WriteToText(known_args.output))

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
