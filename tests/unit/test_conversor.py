import io
import duckdb
from ingestion.base.conversor import Conversor  


def test_csv_para_parquet():
    # GIVEN
    csv_dados = b"id,nome\n1,Aninha\n2,Bruno"

    # WHEN
    resultado_bytes = Conversor.csv_para_parquet(csv_dados)

    # THEN
    # Validações básicas
    assert isinstance(resultado_bytes, bytes)
    assert len(resultado_bytes) > 0
    assert resultado_bytes.startswith(b"PAR1")


def test_json_para_parquet():
    # GIVEN
    json_dados = b'[{"id": 1, "nome": "Aninha"}, {"id": 2, "nome": "Bruno"}]'

    # WHEN
    resultado_bytes = Conversor.json_para_parquet(json_dados)

    # THEN
    assert isinstance(resultado_bytes, bytes)
    assert len(resultado_bytes) > 0
    assert resultado_bytes.startswith(b"PAR1")
