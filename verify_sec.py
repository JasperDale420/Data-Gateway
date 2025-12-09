import asyncio
from dataingestion.features.sec_data.ingestor import sec_ingestor
from dataingestion.features.sec_data.schemas import SecQuery

async def main():
    print("Testing SEC Ingestion for AAPL...")
    query = SecQuery(ticker="AAPL")
    try:
        result = await sec_ingestor.ingest(query)
        print(f"Successfully fetched data for {result.name} (CIK: {result.cik})")
        print(f"Total filings found: {len(result.filings)}")
        if result.filings:
            print(f"Most recent filing: {result.filings[0].form} on {result.filings[0].filingDate}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
