from airflow.models.baseoperator import BaseOperator
from tempfile import NamedTemporaryFile
from contextlib import closing


class FileTransferOperator(BaseOperator):

    def __init__(self, provider, consumers, tmp_file_open_mode='t', **kwargs):
        super().__init__(**kwargs)
        self._provider = provider
        self._consumers = consumers
        self._mode = tmp_file_open_mode

    def execute(self, context):
        kwargs = {
            'ds': context.get('ds'),
            'ds_nodash': context.get('ds_nodash'),
            'ts': context.get('ts'),
            'ti': context.get('ti'),

            'tmp_file_open_mode': self._mode,
        }

        if self._mode == 't':
            # text mode
            tmp_file_open_args = {'mode': 'w+t', 'newline': '', 'encoding': 'utf-8'}
        else:
            # bynary mode
            tmp_file_open_args = {'mode': 'w+b'}

        with closing(NamedTemporaryFile(**tmp_file_open_args)) as tmp_file:
            print('tmp_file:', tmp_file.name)

            self._provider(tmp_file, **kwargs)

            for consumer in self._consumers:
                tmp_file.seek(0)
                consumer(tmp_file, **kwargs)
