import sys
import psycopg2
import csv
import os
import uuid


class ImportArticlesContentCsvToPostgresDB:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.db_name = "deloitte"
        self.db_user = 'deloitte'
        self.db_password = '3edc@WSX'
        self.db_host = '203.135.132.57'
        self.db_port = '5480'
        self.article_table_name = 'standard_news_article_table'
        self.content_table_name = 'standard_news_content_table'
        self.path = r'./data'

    def init_db_connection_settings(self, db_name, db_user, db_password, db_host, db_port):
        '''
        :param db_name: init the db name: currently we use deloitte as the db name
        :param db_user: init the db user: currently we use deloitte as the db user
        :param db_password: init the password: currently we use 3edc@WSX as the db pw
        :param db_host: init the db host: currently we use 203.135.132.57 as the db host
        :param db_port: init the db_port: currently we use 5480 as the db port
        '''
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

    def init_db_connection(self):
        '''
        establish the db connection, and init the self.conn and self.cursor
        '''
        self.conn = psycopg2.connect(database=self.db_name,
                                     user=self.db_user, password=self.db_password,
                                     host=self.db_host, port=self.db_port
                                     )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
    def close_db_connection(self):
        self.conn.close()

    def create_article_table(self):
        '''
        This script is used to create the article table
        '''
        create_table_sql1 = '''
            CREATE TABLE IF NOT EXISTS  {} (
                id text not null,
                keyword text null,
                news_category text not null,
                release_date date not null,
                title text not null,
                url text not null,
                src text not null,
                created_date timestamp not null default now()::timestamp(0)without time zone, 
                constraint {} primary key (id)
            );
            create index IF NOT EXISTS release_date_idx on {} using btree (release_date desc);
            -- Permissions 
            -- alter table deloitte.public.cleanednewsdata owner to postgres;
            -- grant all on table deloitte.public.cleanednewsdata;
        '''.format(self.article_table_name, self.article_table_name + 'news_id', self.article_table_name)
        self.cursor.execute(create_table_sql1)
        self.conn.commit()

    def create_content_table(self):
        '''
        This script is used to create the content table
        '''
        create_table_sql2 = '''
        -- DROP TABLE {}

            CREATE TABLE IF NOT EXISTS {} (
                news_id text not null,
                content text[] not null,
                constraint news_id_fk foreign key (news_id) references {}(id)
            );
        '''.format(self.content_table_name, self.content_table_name, self.article_table_name)
        self.cursor.execute(create_table_sql2)
        self.conn.commit()

    def create_new_table_for_article_and_content(self, article_table_name, content_table_name):
        '''
        Used to create new table for article and content
        '''
        self.article_table_name = article_table_name
        self.content_table_name = content_table_name
        self.create_article_table()
        self.create_content_table()

    def create_result_table(self):
        '''
        Used to create result table for result table
        :return:
        '''
        create_table_sql3 = '''
            -- DROP TABLE deloitte.public.cleanednewsdata_result
            CREATE TABLE IF NOT EXISTS deloitte.public.cleanednewsdata_result (
                news_id text not null,
                entity text[] not null,
                frequency integer[] not null,
                organization text[][][] not null,
                sentence_score decimal[][] not null,
                score decimal[] not null,
                average_score decimal not null,
                sentiment text not null,
                entity_sentiment jsonb null,
                entity_sentiment_key text[] null,
                entity_sentiment_value text[] null,
                similar_news integer[] null,
                constraint news_id_fk foreign key (news_id) references deloitte.public.cleanednewsdata_articles(id)
            );
        '''
        self.conn.commit()
        self.cursor.execute(create_table_sql3)

    def inject_single_article_and_content_into_db(self, keyword, news_category, release_date, title, url, src, content):
        '''
        :param keyword:
        :param news_category:
        :param release_date:
        :param title:
        :param url:
        :param src:
        :param content:
        Inject single article and content data into db
        '''
        uuId = str(uuid.uuid4())
        postgres_insert_query1 = ''' INSERT INTO {} (id, keyword, news_category, release_date, title, url, src) VALUES (%s,%s,%s,%s,%s,%s,%s)'''.format(self.article_table_name)
        record_to_insert1 = (uuId, keyword, news_category, release_date, title, url, src)
        try:
            self.cursor.execute(postgres_insert_query1, record_to_insert1)
            postgres_insert_query2 = ''' INSERT INTO {} (news_id,content)VALUES (%s, %s);'''.format(self.content_table_name)
            record_to_insert2 = (uuId, content)
            self.cursor.execute(postgres_insert_query2, record_to_insert2)
            self.conn.commit()
        except psycopg2.InterfaceError as e:
            print('{}: {}; {}; {}; {}'.format(type(e).__name__, str(e), __file__, e.__traceback__.tb_lineno, str(e)))
            self.cursor = self.conn.cursor()

    def inject_csv_article_content_data_into_db(self):
        '''
        (currently not used)
        Used to inject the csv article and content data into db
        '''
        files = os.listdir(self.path)
        for f_name in files:
            if not os.path.isdir(f_name):
                with open(self.path + '/' + f_name, "r", encoding='utf-8') as f:
                    reader = csv.reader(f)
                    cnt = 0
                    for item in reader:
                        if cnt > 0:
                            keyword = item[0]
                            news_category = item[1]
                            release_date = item[2]
                            title = item[3]
                            url = item[4]
                            src = item[5]
                            content = eval(item[6])
                            self.inject_single_article_and_content_into_db(keyword, news_category, release_date, title,
                                                                           url, src, content)
                        cnt += 1
        self.conn.close()

    def start_insert_news_into_articles_and_content_table(self, keyword, news_category, release_date, title, url, src,
                                                          content):
        self.inject_single_article_and_content_into_db(keyword, news_category, release_date, title, url, src, content)

    def start_csv_import_process(self):
        self.init_db_connection()
        self.create_new_table_for_article_and_content('test_article_table', 'test_content_table')
        # self.create_result_table()
        self.inject_csv_article_content_data_into_db()


def main():
    data_transformer = ImportArticlesContentCsvToPostgresDB()
    data_transformer.start_csv_import_process()



if __name__ == "__main__":
    sys.exit(main())

# REMOTE_HOST='203.135.132.57'
# REMOTE_SSH_PORT=22
# REMOTE_USERNAME='root'
# REMOTE_PASSWORD='X*(mv98e'
# PORT=5432

# server = SSHTunnelForwarder((REMOTE_HOST, REMOTE_SSH_PORT),
#          ssh_username=REMOTE_USERNAME,
#          ssh_password=REMOTE_PASSWORD,
#          remote_bind_address=('172.29.0.2', PORT),
#          local_bind_address=('localhost', 5432))
# server.start()

# conn = psycopg2.connect(database="deloitte",
#                         user='deloitte', password='3edc@WSX',
#                         host='172.29.0.2', port='5432'
# )
