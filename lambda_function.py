from app.process import Process


def lambda_handler(event, context):

    return Process().execute(event)

