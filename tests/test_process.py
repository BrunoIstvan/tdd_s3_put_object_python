import base64
import io
import json
import unittest
from unittest.mock import MagicMock, Mock

from botocore.response import StreamingBody

from app.exceptions.custom_exceptions import InvalidQueryParametersException, InvalidS3ParametersException, \
    BodyNotFoundException, HeaderNotFoundException
from app.process import Process

event = {
    'queryStringParameters': {
        'bucket': 'bucket_name',
        'key': 'key_file_name'
    },
    'body': b'c2FtcGxlIHRleHQ=',
    'headers': {
        'Content-Type': 'application/json'
    }
}


class TestProcess(unittest.TestCase):

    def setUp(self) -> None:
        self.process = Process()

    def test_execute(self):
        content_file = b'c2FtcGxlIHRleHQ='
        expected_put_object_response = {'ResponseMetadata': {'RequestId': '1'}}
        expected_response = self.process.build_response(expected_put_object_response)
        self.process.validate_query_string_param = Mock(return_value=event['queryStringParameters'])
        self.process.validate_param_values = Mock(return_value=('bucket_name', 'key_file_name'))
        self.process.validate_body = Mock(return_value=content_file)
        self.process.validate_header_content_type = MagicMock(return_value='text/plan')
        self.process.s3_service = MagicMock()
        self.process.s3_service.get_client = MagicMock()
        self.process.s3_service.execute_put_object = MagicMock(return_value=expected_put_object_response)

        content = self.process.execute(event)

        assert content == expected_response
        self.process.validate_query_string_param.assert_called_with(event)
        self.process.validate_param_values.assert_called_with(event['queryStringParameters'])
        self.process.validate_body.assert_called_with(event)
        self.process.validate_header_content_type.assert_called_with(event)
        self.process.s3_service.get_client.assert_called_once()
        self.process.s3_service.execute_put_object.assert_called_with(self.process.s3_service.get_client(),
                                                                      'bucket_name',
                                                                      'key_file_name',
                                                                      b'sample text',  # base64 de b'c2FtcGxlIHRleHQ='
                                                                      'text/plan')

    def test_validate_param_values_success(self):
        bucket, key = self.process.validate_param_values(event['queryStringParameters'])
        assert bucket is not None, 'Parametro bucket esta vazio'
        assert key is not None, 'Parametro key esta vazio'
        assert bucket == 'bucket_name', 'Valor do parametro bucket esta diferente do esperado'
        assert key == 'key_file_name', 'Valor do parametro key esta diferente do esperado'

    def test_validate_param_values_fail(self):
        with self.assertRaises(InvalidS3ParametersException):
            self.process.validate_param_values({})

    def test_validate_query_string_param_success(self):
        params = self.process.validate_query_string_param(event)
        assert params is not None, 'Parametro event esta nulo'
        assert 'bucket' in params, 'Parametro bucket nao informado dentro de queryStringParameters'
        assert 'key' in params, 'Parametro key nao informado dentro de queryStringParameters'
        assert params['bucket'] == 'bucket_name', 'Valor do parametro bucket diferente do esperado'
        assert params['key'] == 'key_file_name', 'Valor do parametro key diferente do esperado'

    def test_validate_query_string_param_fail(self):
        with self.assertRaises(InvalidQueryParametersException):
            self.process.validate_query_string_param({})

    def test_build_response_success(self):
        content = {'ResponseMetadata': {'RequestId': '2'}}
        success_response = {
            'headers': {'Content-Type': 'application/json'},
            'statusCode': 200,
            'body': json.dumps(content)
        }
        response = self.process.build_response('2')
        assert response is not None
        assert response == success_response

    def test_build_response_file_not_found(self):
        error_upload_response = {'statusCode': 500,
                                 'body': json.dumps({'message': 'Erro ao realizar upload de arquivo'})}
        response = self.process.build_response(None)
        assert response is not None
        assert response == error_upload_response

    def test_validate_body_success(self):
        content = b'fsdfsd-sdfs'
        body = self.process.validate_body({'body': content})
        assert body == content

    def test_validate_body_fail(self):
        with self.assertRaises(BodyNotFoundException):
            self.process.validate_body({})

    def test_validate_header_content_type_success(self):
        content = 'application/json'
        header = self.process.validate_header_content_type(event)
        assert content == header

    def test_validate_header_content_type_fail(self):
        with self.assertRaises(HeaderNotFoundException):
            self.process.validate_header_content_type({'Content-Type': None})


if __name__ == '__main__':
    unittest.main()
