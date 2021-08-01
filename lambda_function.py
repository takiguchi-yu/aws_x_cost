import os
import json
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
# import japanize_matplotlib
import boto3

ce = boto3.client('ce')
s3 = boto3.client('s3')

def main():
    # 現在日付を取得
    now = dt.datetime.now()
    start = (now - dt.timedelta(days=8)).strftime('%Y-%m-%d')
    end = now.strftime('%Y-%m-%d')

    # AWS Cost Explorer 実行
    response = ce.get_cost_and_usage (
        TimePeriod = {
            'Start': start,
            'End': end,
        },
        Granularity='DAILY',  # DAILY or MONTHLY
        Metrics = [
            'AmortizedCost'
        ],
        GroupBy = [
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
        ]
    )

    # 日ごとにループ
    dataframe = {}
    for result in response['ResultsByTime']:
        print(result)
        total = 0
        start_date = result['TimePeriod']['Start']
        date = dt.datetime.strptime(start_date, '%Y-%m-%d').strftime('%-m/%-d')
    
        # 集計対象を初期化
        service_amount = {
            'Amazon Relational Database Service': 0,
            'Amazon Virtual Private Cloud': 0,
            'AWS Database Migration Service': 0,
            'EC2 - Other': 0,
            'AWS Support (Developer)': 0,
            'Tax': 0,
        }
    
        # サービスごとにループ
        sub_total = 0
        for group in result['Groups']:
            amount = round(float(group['Metrics']['AmortizedCost']['Amount']), 1) # 料金は小数第一位で四捨五入
            service = group['Keys'][0]
            if service in service_amount:
                service_amount[service] = amount
                sub_total += amount

            total += amount

        service_amount['Others'] = round(total - sub_total, 1) # 集計対象以外の料金を算出
        dataframe[date] = service_amount

    dataset = pd.DataFrame(dataframe)
    
    
    # 積み上げ棒グラフ内にデータラベルを表示する
    fig, ax = plt.subplots(figsize=(15, 8))
    for i in range(len(dataset)):
        ax.bar(dataset.columns, dataset.iloc[i], bottom=dataset.iloc[:i].sum())
        for j in range(len(dataset.columns)): 
            plt.text(x=j, 
                y=dataset.iloc[:i, j].sum() + (dataset.iloc[i, j] / 2), 
                s=dataset.iloc[i, j], 
                ha='center', 
                va='bottom'
            )
    ax.set(xlabel='', ylabel='Cost ($)')
    ax.legend(dataset.index)
    plt.show()
    
    # S3アップロード
    fig.savefig('/tmp/cost.png')
    # s3.put_object(
    #     ACL='public-read',
    #     Body='/tmp/cost.png',
    #     Bucket='<バケット名>',
    #     ContentType='image/png',
    #     Key='cost.png',
    # )
    file_name = '/tmp/cost.png'
    bucket = '<バケット名>'
    object_name = 'cost.png'
    response = s3.upload_file(file_name, bucket, object_name, 
        ExtraArgs={
            'ACL': 'public-read',
            'ContentType': 'image/png',
        }
    )

def lambda_handler(event, context):
    main()
