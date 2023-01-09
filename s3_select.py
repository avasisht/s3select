import boto3

# Create an S3 client
session = boto3.Session(profile_name='abhi')
s3 = session.client('s3')

# Set the name of the bucket and object to retrieve
bucket = 'awss3selectexample'
key = 'Top1000IMDBmovies.csv'

# Set the SQL expression to use for processing the object
expression = "SELECT MovieName FROM s3object s WHERE s.MovieRating > '9.0'"

# Set the response format for the data
response_format = 'CSV'

# Set the encoding for the data
encoding = 'UTF-8'

# Set the desired output serialization for the data
output_serialization = {
    'CSV': {
        'QuoteFields': 'ASNEEDED',
        'RecordDelimiter': '\n',
        'FieldDelimiter': ',',
        'QuoteEscapeCharacter': '"'
    }
}

# Set the input serialization for the data
input_serialization = {
    'CSV': {
        'FileHeaderInfo': 'Use',
        'RecordDelimiter': '\n',
        'FieldDelimiter': ','

    }
}

# Use the S3 client to retrieve the object and process it using S3 Select
response = s3.select_object_content(
    Bucket=bucket,
    Key=key,
    Expression=expression,
    ExpressionType='SQL',
    InputSerialization=input_serialization,
    OutputSerialization=output_serialization
)

# Get the records from the response
records = response['Payload']

# Iterate over the records and print each one
for record in records:
    if 'Records' in record:
        payload = record['Records']['Payload'].decode(encoding)
        print(payload)
    elif 'Stats' in record:
        print(record['Stats'])
    elif 'End' in record:
        print(record['End'])
