import boto3

from app.exceptions.custom_exceptions import UploadFileException


class S3Service:

    def __init__(self):
        pass

    def get_client(self):
        """
        Método que retorna um client do S3
        :return: Client do S3
        """

        return boto3.client('s3')

    def execute_put_object(self, client, bucket, key, body, content_type):
        """
        Método que grava o conteúdo em um arquivo dentro do S3
        :param client: Client do S3
        :param bucket: Nome do bucket
        :param key: Nome do arquivo
        :param body: Conteúdo do arquivo
        :param content_type: Tipo de Conteúdo
        :return: Versão do arquivo
        """

        result = client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

        if result is None:
            raise UploadFileException('Upload de arquivo não executado com sucesso')

        # retorna o conteudo e o tamanho do arquivo
        return result['ResponseMetadata']['RequestId']

