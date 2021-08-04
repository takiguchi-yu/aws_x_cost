import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
import boto3

ce = boto3.client('ce')
s3 = boto3.client('s3')

def main():

    # 直近7日間の開始日、終了日を取得する
    start, end = get_start_end_date()

    # AWS Cost Explorer 実行する
    response = ce_get_cost_and_usage(start, end)

    # AWS Cost Explorer の実行結果をpandaのdataframeの形式に加工する
    dataset = make_dataframe(response)
    
    # 積み上げ棒グラフ内にデータラベルを表示する
    save_bar(dataset)
    
    # S3アップロード
    s3_upload_file('/tmp/cost.png', 'takiguchi-work-restdb', 'cost.png')

def get_start_end_date():
    """直近7日間の開始日、終了日を取得する"""

    now = dt.datetime.now()
    start = (now - dt.timedelta(days=8)).strftime('%Y-%m-%d')
    end = now.strftime('%Y-%m-%d')
    return [start, end]

def ce_get_cost_and_usage(start_date, end_date):
    """AWS Cost Explorer 実行する"""
    
    return ce.get_cost_and_usage (
        TimePeriod = {
            'Start': start_date,
            'End': end_date,
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

def make_dataframe(data):
    """AWS Cost Explorer の実行結果をpandaのdataframeの形式に加工する"""
    
    # 日ごとにループ
    dataframe = {}
    for result in data['ResultsByTime']:
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

    return pd.DataFrame(dataframe)

def save_bar(dataset):
    """積み上げ棒グラフ内にデータラベルを表示する"""

    print(dataset)
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
    # 作成した積み上げ棒グラフを一時保存する
    fig.savefig('/tmp/cost.png')

def s3_upload_file(file_name, bucket, object_name):
    """S3にmatplotlibで作成したファイルをアップロードする"""

    response = s3.upload_file(file_name, bucket, object_name, 
        ExtraArgs={
            'ACL': 'public-read',
            'ContentType': 'image/png',
        }
    )

def lambda_handler(event, context):
    main()
