import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from sqlalchemy.exc import DataError
from examples.example_table import Project, db


if __name__ == "__main__":
    try:
        # Get all projects with project_name='adsadsy'
        with db.session() as session:
            sql_results = session.query(Project).filter_by(project_name='adsadsy').all()
            for sql_result in sql_results:
                print(sql_result.project_status)
    except DataError as e:
        print(e)
