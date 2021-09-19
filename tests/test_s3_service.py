import io
from unittest import TestCase
from unittest.mock import Mock

from botocore.response import StreamingBody
from botocore.stub import Stubber

from app.exceptions.custom_exceptions import UploadFileException
from app.s3_service import S3Service


def get_response():
    return {'ResponseMetadata': {'RequestId': '1'}}


class TestS3Service(TestCase):

    def setUp(self) -> None:
        self.s3_service = S3Service()

    def test_get_client(self):

        client = self.s3_service.get_client()
        assert client is not None

    def test_execute_put_object_success(self):

        s3_client = self.s3_service.get_client()
        assert s3_client is not None

        # prepara um stubber
        with Stubber(s3_client) as stubber:
            # simula um conteudo qualquer
            content_file = b'sfsd-sdfgfd dgd fgdf-g dgdfgdfgd-d gdfgdfgd'
            content_file = StreamingBody(io.BytesIO(content_file), len(content_file))
            content_type = 'text/plan'
            bucket_test = 'bucket_name'
            key_test = 'key_file_name'
            # recupera a resposta esperada da execucao do metodo s3_client.put_object()
            response = get_response()

            # raw_stream = StreamingBody(io.BytesIO(content_file), len(content_file))
            # esses sao os parametros enviados ao metodo s3_client.put_object()
            expected_params = {'Bucket': bucket_test,
                               'Key': key_test,
                               'Body': content_file,
                               'ContentType': content_type}

            stubber.add_response('put_object', response, expected_params)
            stubber.activate()
            # recebe a resposta do metodo contendo o conteudo e o tamanho do arquivo
            service_response = self.s3_service.execute_put_object(client=s3_client,
                                                                  bucket=bucket_test,
                                                                  key=key_test,
                                                                  body=content_file,
                                                                  content_type=content_type)
            assert service_response == (get_response()['ResponseMetadata']['RequestId'])

    def test_execute_put_object_success_upload_file_exception(self):

        s3_client = self.s3_service.get_client()
        s3_client.put_object = Mock(return_value=None)
        bucket_test = 'bucket_name'
        key_test = 'key_file_name'
        content = 'serwe0-ssfsdfs'

        with self.assertRaises(UploadFileException):
            # recebe a resposta do metodo contendo o conteudo e o tamanho do arquivo
            self.s3_service.execute_put_object(client=s3_client,
                                               bucket=bucket_test,
                                               key=key_test,
                                               body=content,
                                               content_type='text/plan')
