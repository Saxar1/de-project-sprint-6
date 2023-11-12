import vertica_python
from airflow.models import Variable
from typing import List, Dict, Optional
import contextlib
import pandas as pd


def load_dataset_file_to_vertica(dataset_path: str, schema: str, table: str, columns: List[str], parse_dates: List[str], type_override: Optional[Dict[str, str]] = None):
    df = pd.read_csv(dataset_path, parse_dates=parse_dates, dtype=type_override)
    df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host='vertica.tgcloudenv.ru',
        port=5433,
        user='st23052702',
        password=Variable.get("vertica_pass")
    )
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1
 
    vertica_conn.close()
