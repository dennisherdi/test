import datetime
import pandas as pd
from sqlalchemy.sql.expression import update
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String , update
## import statements ##
from tensorflow import keras
from keras.preprocessing.text import Tokenizer
from keras.preprocessing import sequence
from keras.preprocessing.sequence import pad_sequences
import numpy as np
MAX_NB_WORDS = 10000
tokenizer = Tokenizer(num_words=MAX_NB_WORDS,
                      lower=True,
                      char_level=False)

def autolabel():
    engine = create_engine(
        "mysql://admin:db_dev_Interconnect_Data_Project@interconnect-data-project-dev.cqorlseuayk8.ap-southeast-1.rds.amazonaws.com/interconnect_data",
        connect_args={'connect_timeout': 30}, echo=False
    )
    conn = engine.connect()

    df = pd.read_sql("select * from interconnect_data.ic_news where sentimen is null", conn)

    model1 = keras.models.load_model("C:/Users/denni/Documents/Kerja/docker-airflow-master/includess/model007.h5")

    raw_docs_test  = df['body']

    tokenizer.fit_on_texts(raw_docs_test)

    word_seq_test  = tokenizer.texts_to_sequences(raw_docs_test)

    word_seq_test  = sequence.pad_sequences(word_seq_test, maxlen = 4716)

    y_pred = model1.predict(word_seq_test).round()

    hasil = y_pred.argmax(axis=1)

    aa = hasil.tolist()

    data = {'sentimen': aa}

    df2 = pd.DataFrame(data)

    # Encode target labels
    repl = {0:'Neutral',1:'Positive',2:'Negative'}
    df2['sentimen'].replace(repl, inplace=True)

    df3 = df.assign(sentimen = df2['sentimen'])

    df4 = df3[['id','sentimen']]

    import mysql.connector

    def update_sentimen(sentimen,id):
        try:
            connection = mysql.connector.connect(host='interconnect-data-project-dev.cqorlseuayk8.ap-southeast-1.rds.amazonaws.com',
                                                database='interconnect_data',
                                                user='admin',
                                                password='db_dev_Interconnect_Data_Project')

            cursor = connection.cursor()
            sql_update_query = """Update ic_news set sentimen = %s where id = %s"""
            input_data = (sentimen,id)
            cursor.execute(sql_update_query, input_data)
            connection.commit()
            print("Record Updated successfully ")

        except mysql.connector.Error as error:
            print("Failed to update record to database: {}".format(error))
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")
        
    for i in range(len(df4)):        
            update_sentimen(df4.loc[i]['sentimen'],int(df4.loc[i]['id']) )
