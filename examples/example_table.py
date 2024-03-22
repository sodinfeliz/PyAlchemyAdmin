import uuid
from sqlalchemy import Column, Text
from sqlalchemy.dialects.postgresql import UUID, JSON, BIGINT
from pyalchemyadmin import PostgreDBManager

DATABASE = 'postgres'
USER = 'admin'
PASSWORD = 'admin'
HOST = '127.0.0.1'
PORT = 7776


PostgreDBManager.create_database_session(
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    engine='psycopg2'
)


class Project(PostgreDBManager.BASE):
    __tablename__ = 'project'
    
    project_uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_name = Column(Text, nullable=False)
    project_status = Column(JSON)
    project_path = Column(Text)
    annotation = Column(Text)
    cover_image_name = Column(Text)
    cover_image_type = Column(Text)
    create_time = Column(BIGINT)


PostgreDBManager.create_all()

