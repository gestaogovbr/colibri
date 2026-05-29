from ingestion.ncm import pipeline as ncm
from ingestion.pncp import pipeline as pncp


def main():
    print("=== NCM ===")
    ncm.main()
    print("=== PNCP ===")
    pncp.main()


if __name__ == "__main__":
    main()
