class CustomException(Exception):
    pass


class InvalidQueryParametersException(CustomException):
    pass


class InvalidS3ParametersException(CustomException):
    pass


class UploadFileException(Exception):
    pass


class BodyNotFoundException(Exception):
    pass


class HeaderNotFoundException(Exception):
    pass
