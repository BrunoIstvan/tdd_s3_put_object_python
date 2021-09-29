import base64
import json

from app.exceptions.custom_exceptions import InvalidQueryParametersException, InvalidS3ParametersException, \
    BodyNotFoundException, HeaderNotFoundException
from app.s3_service import S3Service


class Process:

    def __init__(self):
        self.s3_service = S3Service()

    def execute(self, event):
        """
        Método que executa todo o processamento
        :param event: Dicionário de dados gerado pelo chamador
        :return: Resposta do padrão API Gateway contendo o conteúdo do arquivo
        """

        body = self.validate_body(event)

        body = base64.b64decode(body)

        content_type = self.validate_header_content_type(event)

        params = self.validate_query_string_param(event)

        bucket, key = self.validate_param_values(params)

        s3_client = self.s3_service.get_client()

        content = self.s3_service.execute_put_object(s3_client, bucket, key, body, content_type)

        return self.build_response(content)

    def validate_param_values(self, params):
        """
        Valida se os campos bucket e key foram informados corretamente
        :param params: Dicionário com os dados vindos do parâmetro event['queryStringParameters']
        :return: Retorna o valor do parametro bucket e do parametro key
        """
        if 'bucket' not in params or 'key' not in params or \
                params['bucket'] == '' or params['key'] == '':
            raise InvalidS3ParametersException('Para o serviço s3 é necessário enviar os parâmetros '
                                               'bucket e key com seus devidos valores')
        else:
            return params['bucket'], params['key']

    def validate_query_string_param(self, event):
        """
        Valida se o parametro queryStringParameters existe dentro do parametro event
        :param event: Dicionário com os dados de entrada originados pelo chamador
        :return: Dicionário contendo todos os dados dentro do parâmetro queryStringParameters
        """
        if 'queryStringParameters' not in event:
            raise InvalidQueryParametersException('O evento informado não contém os parâmetros necessários')

        return event['queryStringParameters']

    def build_response(self, content):
        """
        Método que constrói a resposta da execução desse processo
        :param content: Conteúdo do arquivo em formato binário
        :return: Resposta no padrão do API Gateway
        """

        if content is not None:
            return {
                'headers': {'Content-Type': 'application/json'},
                'statusCode': 200,
                'body': json.dumps({'ResponseMetadata': {'RequestId': content}})
            }

        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro ao realizar upload de arquivo'})
        }

    def validate_body(self, event):
        """
        Método que valida se o conteúdo do arquivo foi enviado no parametro body
        Retorna o conteúdo em caso de sucesso
        :param event: Dicionário com os dados de entrada originados pelo chamador
        :return: Conteúdo do arquivo
        :exception: BodyNotFoundException em caso de parâmetro não informado ou conteúdo vazio
        """
        if 'body' not in event or event['body'] is None:
            raise BodyNotFoundException('O conteúdo do arquivo não foi encontrado na requisição')

        return event['body']

    def validate_header_content_type(self, event):
        """
            Método que valida se o conteúdo do header foi enviado no parametro Header
            Retorna o conteúdo em caso de sucesso
            :param event: Dicionário com os dados de entrada originados pelo chamador
            :return: Valor do parâmetro Content-Type
            :exception: BodyNotFoundException em caso de parâmetro não informado ou conteúdo vazio
            """
        if 'headers' not in event or event['headers'] is None or 'Content-Type' not in event['headers']:
            raise HeaderNotFoundException('O conteúdo do cabeçalho não foi encontrado na requisição')

        return event['headers']['Content-Type']
