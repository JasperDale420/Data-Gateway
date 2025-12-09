import pandas as pd
from typing import Dict, Any
from datetime import datetime


class SecParser:
    """
    Parses SEC Company Facts (XBRL) to extract Point-in-Time (PIT) financials.
    """

    # Key US-GAAP concepts to extract
    # We can expand this list as needed for the strategy
    CONCEPTS = [
        "Assets",
        "Liabilities",
        "StockholdersEquity",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueServicesNet",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "GrossProfit",
        "NetIncomeLoss",
        "ProfitLoss",  # Alternative for NetIncome
        "NetIncomeLossAvailableToCommonStockholdersBasic",  # Alternative for NetIncome
        "OperatingIncomeLoss",
        "CostOfRevenue",  # For Gross Margin
        "CostOfGoodsAndServicesSold",  # Alternative for CostOfRevenue
        "NetCashProvidedByUsedInOperatingActivities",  # For FCF
        "PaymentsToAcquirePropertyPlantAndEquipment",  # For CapEx
        "EarningsPerShareBasic",
        "EarningsPerShareDiluted",
        "CashAndCashEquivalentsAtCarryingValue",
        "LongTermDebt",
        "CommonStockSharesOutstanding",
    ]

    def extract_pit_financials(self, facts_json: Dict[str, Any]) -> pd.DataFrame:
        """
        Extracts a PIT DataFrame from the raw Company Facts JSON.
        Index: filing_date
        Columns: [concept_name, value, period_end, form, frame]
        """
        if "facts" not in facts_json or "us-gaap" not in facts_json["facts"]:
            return pd.DataFrame()

        us_gaap = facts_json["facts"]["us-gaap"]
        records = []

        for concept in self.CONCEPTS:
            if concept in us_gaap:
                units = us_gaap[concept].get("units", {})
                # Units can be "USD", "shares", "USD/shares"
                for unit_name, data_points in units.items():
                    for point in data_points:
                        # We need 'filed' date for PIT
                        if "filed" not in point:
                            continue

                        # We focus on 10-K and 10-Q for now
                        form = point.get("form", "")
                        if form not in ["10-K", "10-Q"]:
                            continue

                        # Calculate duration for period-based concepts
                        start = point.get("start")
                        end = point.get("end")
                        duration = "instant"

                        if start and end:
                            try:
                                s_date = datetime.strptime(start, "%Y-%m-%d")
                                e_date = datetime.strptime(end, "%Y-%m-%d")
                                days = (e_date - s_date).days
                                if 80 <= days <= 100:
                                    duration = "Q"  # Quarterly (~90 days)
                                elif 350 <= days <= 370:
                                    duration = "FY"  # Fiscal Year (~365 days)
                                else:
                                    duration = f"{days}D"  # Other (e.g., YTD)
                            except Exception:
                                pass

                        records.append(
                            {
                                "ticker": facts_json.get(
                                    "cik"
                                ),  # Using CIK as placeholder if ticker not in root
                                "concept": concept,
                                "value": point.get("val"),
                                "unit": unit_name,
                                "period_start": start,
                                "period_end": end,
                                "duration": duration,
                                "filing_date": point.get("filed"),
                                "form": form,
                                "frame": point.get("frame"),  # e.g., CY2020Q1
                                "accn": point.get("accn"),
                            }
                        )

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)

        # Convert dates
        df["filing_date"] = pd.to_datetime(df["filing_date"])
        df["period_end"] = pd.to_datetime(df["period_end"])

        # Sort by filing date to simulate timeline
        df.sort_values("filing_date", inplace=True)

        return df


sec_parser = SecParser()
