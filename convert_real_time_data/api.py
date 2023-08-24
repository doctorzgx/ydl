# -*- coding: utf-8 -*-
import sys
import io
import re
import json
import pandas as pd
import pymysql
import logging
from datetime import datetime, timedelta
from flask import Flask, Response
from sqlalchemy import create_engine

sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf8", line_buffering=True)

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


def generate_insert_sql_real_time_data(last_update_time):
    data = pd.read_sql('''SELECT * FROM db_ydl_dev.cm_ydl_history where updateTime >= '{}' '''
                       .format(last_update_time), engine)
    start_time, end_time = data['updateTime'].min(), data['updateTime'].max()
    logging.info('start_time: {}  end_time: {}'.format(start_time, end_time))
    count = 0
    updateTime = []
    df_dict = dict()
    pattern = re.compile(r'[^A-Za-z0-9_]')
    just = datetime.now().timestamp()
    data.drop_duplicates(subset=['name', 'value', 'updateTime'], keep='first', inplace=True)
    data['name'] = data.apply(lambda x: re.sub(pattern, '_', x['name'] + 'type{}'.format(x['type'])), axis=1)
    for _ in range((end_time - start_time).seconds):
        start_time += timedelta(seconds=1)
        time_df = data[data['updateTime'] == start_time]
        if time_df.empty:
            continue
        updateTime.append(start_time.strftime("%Y-%m-%d %H:%M:%S"))
        time_df_colums = time_df['name'].copy()
        for index in time_df.index:
            d = time_df.loc[index]
            d_name = d['name']
            d_value = d['value']
            if d_name not in df_dict.keys():
                df_dict[d_name] = [None for _ in range(count)]
            df_dict[d_name].append(d_value)
        set_abn_name = set([x for x in time_df_colums.values if list(time_df_colums.values).count(x) > 1])
        for abn in set_abn_name:
            df_dict.pop(abn)
            time_df_colums = time_df_colums[time_df_colums != abn]
        for feature in df_dict.keys():
            if feature not in time_df_colums.values:
                df_dict[feature].append(None)
        if len(set([len(_) for _ in df_dict.values()])) > 1:
            print(set([len(_) for _ in df_dict.values()]))
        count += 1

    logging.info('所用时间 {} count {}'.format(datetime.now().timestamp() - just, count))

    df = pd.DataFrame(df_dict)
    df.insert(0, 'updateTime', updateTime)

    return df


def insert_sql_real_time_data():
    conn = pymysql.connect(
        user=config['mysql_user'],
        password=config['mysql_password'],
        host=config['mysql_host'],
        database=config['mysql_database'],
        port=config['mysql_port'],
        charset=config['mysql_charset']
    )
    cursor = conn.cursor()

    select_query = "SELECT updateTime FROM cm_ydl_history_real_time ORDER BY updateTime DESC LIMIT 1"
    cursor.execute(select_query)
    last_update_time = cursor.fetchone()[0]
    logging.info('last_update_time: {}'.format(last_update_time))

    cursor.execute("DELETE FROM cm_ydl_history_real_time WHERE updateTime = '{}';".format(last_update_time))
    conn.commit()

    data = generate_insert_sql_real_time_data(last_update_time)

    cursor.execute("SELECT * FROM cm_ydl_history_real_time LIMIT 1")
    sql_database_columns = [i[0] for i in cursor.description]

    data_columns = data.columns
    different_cloumns = list(set(data_columns).difference(sql_database_columns))

    if different_cloumns:
        sql_query = "ALTER TABLE cm_ydl_history_real_time "
        for column_name in different_cloumns:
            if column_name[-1] == '0':
                sql_query += f"ADD COLUMN `{column_name}` FLOAT, "
            elif column_name[-1] == '1':
                sql_query += f"ADD COLUMN `{column_name}` INT, "
            else:
                raise 'type is other'
        sql_query = sql_query[:-2]
        cursor.execute(sql_query)
        conn.commit()
    # engine = create_engine('mysql+pymysql://bigdata:BBbb11335577!@192.168.3.104:3306/db_ydl_dev')
    data.to_sql(name='cm_ydl_history_real_time', con=engine, if_exists='append', index=False)
    logging.info('convert finish')
    # 关闭游标和连接
    cursor.close()
    conn.close()


@app.route('/api/Convert_real_time_data', methods=['post'])
@app.errorhandler(Exception)
def Convert_real_time_data():
    insert_sql_real_time_data()

    return Response(status=200)


if __name__ == '__main__':
    logging.basicConfig(filename='./convert_data.log', level=logging.INFO, encoding='utf-8')
    config = json.load(open('./config_file.json', encoding='utf-8'))
    url = 'mysql+pymysql://{}:{}@{}: {}/{}'.format(config['mysql_user'], config['mysql_password'],
                                                   config['mysql_host'], config['mysql_port'], config['mysql_database'])
    engine = create_engine(url)
    # app.run(host=config['API_http_host'], port=config['API_http_port'], debug=True)
    app.run(host=config['API_http_host'], port=config['API_http_port'])
