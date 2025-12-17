import json
import openai
import pydantic

def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps("OpenAI and Pydantic imported successfully")
    }