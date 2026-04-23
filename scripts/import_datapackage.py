"""Import the frictionless datapackage into PostgreSQL, replacing existing tables."""

import os
import sys

from frictionless import Package
from sqlalchemy import MetaData, create_engine, text

if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} <path/to/datapackage.json>")
    sys.exit(1)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/test_frictionless",
)

SKIP_RESOURCES = ["classe_clinique", "specialite_classe_clinique"]

# Tables with old names that are replaced by datapackage tables
OLD_TABLE_NAMES = ["cis_atc", "presentations"]

package = Package(sys.argv[1])
for name in SKIP_RESOURCES:
    package.remove_resource(name)
print(f"Loaded package with {len(package.resources)} resources: {[r.name for r in package.resources]}")

engine = create_engine(DATABASE_URL)
existing = MetaData()
existing.reflect(bind=engine)

with engine.begin() as conn:
    for table_name in OLD_TABLE_NAMES + [r.name for r in package.resources]:
        if table_name in existing.tables:
            print(f"Dropping existing table: {table_name}")
            conn.execute(text(f"DROP TABLE {table_name} CASCADE"))

print(f"Publishing to {DATABASE_URL} ...")
package.publish(DATABASE_URL)
print("Done.")
