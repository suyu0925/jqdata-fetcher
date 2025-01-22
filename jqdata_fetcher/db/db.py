import os
from urllib.parse import parse_qs, urlparse

import sqlalchemy.dialects.postgresql
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()


def parse_db_url(db_url: str) -> dict[str, str]:
    # get schema from query params
    parsed_url = urlparse(db_url)
    query_params = parse_qs(parsed_url.query)
    schema = query_params.get('schema', ['public'])[0]

    # clean query params in db_url
    query_string = '&'.join(f'{k}={v[0]}' for k, v in query_params.items() if k != 'schema')
    db_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
    if query_string:
        db_url = f"{db_url}?{query_string}"

    return {
        'db_url': db_url,
        'schema': schema,
    }


parsed_db_url = parse_db_url(os.getenv('PGURL'))

engine = create_engine(
    parsed_db_url['db_url'],
    connect_args={'options': f'-csearch_path={parsed_db_url["schema"]}'}
)

Base = declarative_base()

Session = sessionmaker(bind=engine)

insert = sqlalchemy.dialects.postgresql.insert

select = sqlalchemy.select

and_ = sqlalchemy.and_

text = sqlalchemy.text

desc = sqlalchemy.desc

asc = sqlalchemy.asc
