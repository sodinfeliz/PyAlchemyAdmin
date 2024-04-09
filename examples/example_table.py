import uuid

from sqlalchemy import Column, Text
from sqlalchemy.dialects.postgresql import UUID

from pyalchemyadmin import PostgreDBManager

DATABASE = 'postgres'
USER = 'postgres'
PASSWORD = 'password'
HOST = '127.0.0.1'
PORT = 5432


db = PostgreDBManager(
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    engine='psycopg2'
)


class TestTable(db.base):
    __tablename__ = 'test_table'
    
    project_uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_name = Column(Text, nullable=False)
    annotation = Column(Text)


db.create_all_tables()
