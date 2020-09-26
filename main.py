import json
import argparse

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class Flatner(DoFn):
    def process(self, json_object):

        jsoned = json.loads(json_object)
        data = jsoned.get('data')
        
        if data and len(data) > 0:
            for item in data:
                row = {
                    'id': item.get('id'), 
                    'type': item.get('type'), 
                    'createdAt': item.get('attributes', {}).get('createdAt'),
                    'updatedAt': item.get('attributes', {}).get('updatedAt'),
                    'slug': item.get('attributes', {}).get('slug'),
                    'synopsis': item.get('attributes', {}).get('synopsis'),
                    'description': item.get('attributes', {}).get('description'),
                    'canonicalTitle': item.get('attributes', {}).get('canonicalTitle'),
                    'averageRating': item.get('attributes', {}).get('averageRating'),
                    'userCount': item.get('userCount'),
                    'favoritesCount': item.get('favoritesCount'),
                    'startDate': item.get('startDate'),
                    'endDate': item.get('endDate'),
                    'subtype': item.get('subtype'),
                    'status': item.get('status'),
                    'posterImage': poster.get('original') if (poster := item.get('attributes', {}).get('posterImage', {})) is not None else None,
                    'coverImage': cover.get('original') if (cover := item.get('attributes', {}).get('coverImage', {})) is not None else None,
                    'episodeCount': item.get('episodeCount'),
                    'episodeLength': item.get('episodeLength'),
                    'totalLength': item.get('totalLength'),
                    'youtubeVideoId': item.get('youtubeVideoId'),
                    'showType': item.get('showType'),
                    'nsfw': item.get('nsfw')
                }
                
                yield row

table_schema = {
    'fields': [
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'createdAt', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'updatedAt', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'slug', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'synopsis', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'canonicalTitle', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'averageRating', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'userCount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'favoritesCount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'startDate', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'endDate', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'subtype', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'posterImage', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'coverImage', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'episodeCount', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'episodeLength', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'totalLength', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'youtubeVideoId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'showType', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'nsfw', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}
    ]
}

def run(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        help='Input file to process.')
    parser.add_argument(
        '--temp_location',
        dest='temp',
        help='Temp location to store staging files.')
    parser.add_argument(
        '--table_spec',
        dest='table_spec',
        help='Bigquery table path. Ex: project_id:dataset.table')
    parser.add_argument(
        '--project',
        dest='project',
        help='Google Cloud Platform project ID')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=known_args.project,
        temp_location=known_args.temp,
        region='us-central1'
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        flatten_pipe = (
            pipeline
            | "Initializing" >> beam.Create([{}])
            | "Read Json" >> beam.io.ReadFromText(known_args.input)
            | "Process Json" >> beam.ParDo(Flatner())
        )

        output_pipe = (
            flatten_pipe
            | "Write on Bigquery" >> beam.io.WriteToBigQuery(
                known_args.table_spec, 
                schema=table_schema, 
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, 
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=known_args.temp
            )
        )

if __name__ == '__main__':
  run()