import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from examples.example_table import TestTable, db

if __name__ == "__main__":
    # Create the records
    db.create(TestTable, project_name="Project 1", annotation="This is project 1")
    db.create(TestTable, project_name="Project 2", annotation="This is project 2")
    db.create(TestTable, project_name="Project 2", annotation="This is project 2")

    # Update the records
    db.update(TestTable, filters={"project_name": "Project 2"}, annotation="This is project 2 updated")

    # Retrieve the records
    records = db.retrieve(TestTable, project_name="Project 2")
    for record in records:
        print(record.annotation)

    # Delete the records
    db.delete(TestTable, project_name="Project 1")

    # Check if the record exists
    exists =  db.exists(TestTable, project_name="Project 2")
    print(exists)


