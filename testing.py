import boto3
import csv

def main():
    s3 = boto3.resource('s3',
    aws_access_key_id='AKIAYYZP7VKXT2BSGELD',
    aws_secret_access_key='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'
    )

    try:
        s3.create_bucket(Bucket='test-qiancao', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'})
    except:
        print ("this may already exist")

    bucket = s3.Bucket("test-qiancao")
    bucket.Acl().put(ACL='public-read')

    body = open(r"C:\Users\53402\OneDrive\Documents\MiloCao\CS1660\HW\New Text Document.txt", 'rb')
    o = s3.Object('test-qiancao', 'test').put(Body=body )
    s3.Object('test-qiancao', 'test').Acl().put(ACL='public-read')

    dyndb = boto3.resource('dynamodb',
    region_name='us-west-2',
    aws_access_key_id='AKIAYYZP7VKXT2BSGELD',
    aws_secret_access_key='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'
    )

    try:
        table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
        {
        'AttributeName': 'PartitionKey',
        'KeyType': 'HASH'
        },
        {
        'AttributeName': 'RowKey',
        'KeyType': 'RANGE'
        }
        ],
        AttributeDefinitions=[
        {
        'AttributeName': 'PartitionKey',
        'AttributeType': 'S'
        },
        {
        'AttributeName': 'RowKey',
        'AttributeType': 'S'
        },
        ],
        ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
        }
        )
    except:
        #if there is an exception, the table may already exist. if so...
        table = dyndb.Table("DataTable")

    table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

    print(table.item_count)

    with open(r"C:\\Users\\53402\\OneDrive\\Documents\\MiloCao\\CS1660\\HW\\test1.csv", 'rt') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
        next(csvf)
        for item in csvf:
            print (item)
            body = open(r"C:\\Users\\53402\\OneDrive\\Documents\\MiloCao\\CS1660\\HW\\"+item[4], 'rb')
            s3.Object('test-qiancao', item[4]).put(Body=body )
            md = s3.Object('test-qiancao', item[4]).Acl().put(ACL='public-read')

            url = " https://s3-us-west-2.amazonaws.com/test-qiancao/"+item[4]
            metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
            'description' : item[4], 'date' : item[2], 'url':url}
            try:
                table.put_item(Item=metadata_item)
            except:
                print ("item may already be there or another failure")

    

if __name__ == "__main__":
    main()