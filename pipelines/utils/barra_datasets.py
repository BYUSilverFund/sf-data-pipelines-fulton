from dotenv import load_dotenv
from datetime import date
import os
from pathlib import Path


class BarraDataset:
    def __init__(
        self,
        history_folder: str | None,
        daily_folder: str,
        history_zip_file: str | None,
        daily_zip_file: str,
        file_name: str,
    ) -> None:
        load_dotenv(override=True)

        self._base_path = os.getenv("DATA_ROOT")

        self._history_folder = history_folder
        self._daily_folder = daily_folder
        self._history_zip_file = history_zip_file
        self._daily_zip_file = daily_zip_file
        self._file_name = file_name

    def history_zip_folder(self) -> Path:
        return Path(self._base_path) / self._history_folder

    def history_zip_file(self, year: int) -> str:
        return f"{self._history_zip_file}_{year}"

    def history_zip_folder_path(self, year: int) -> Path:
        return Path(self._base_path) / self._history_folder / f"{self._history_zip_file}_{year}.zip"

    def file_name(self, date_: date | None = None) -> str:
        if date_:
            return f"{self._file_name}.{date_.strftime('%Y%m%d')}"
        else:
            return self._file_name

    def daily_zip_folder_path(self, date_: date) -> Path:
        zip_filename = f"{self._daily_zip_file}_{date_.strftime('%y%m%d')}.zip"
        return Path(self._base_path) / self._daily_folder / zip_filename
    

# these files contain historical data for all assets tracked by barra

barra_returns = BarraDataset(
    history_folder="us/usslow/daily",
    history_zip_file="SMD_USSLOW_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    # files in this zip folder ^^ and their columns:
        # USSLOWL_100_Asset_Data -> !Barrid|Yield%|TotalRisk%|SpecRisk%|HistBeta|PredBeta|DataDate
        # USSLOWL_100_Asset_LSR -> !Barrid|RootID|Elasticity|RootSpecificRisk|DataDate
        # USSLOWL_100_Covariance -> !Factor1|Factor2|VarCovar|DataDate
        # USSLOWL_100DlyFacRet -> !Factor|DlyReturn|DataDate
        # USSLOW_100_Asset_DlySpecRet -> !Barrid|SpecificReturn|DataDate
        # USSLOW_ESTU_POR -> !Barrid|Shares
        # USSLOWL_100_Asset_Exposure -> !Barrid|Factor|Exposure|DataDate
        # USSLOW_Rates -> !Currency|USDxrate|RFRate%|DataDate
        # USSLOW_Daily_Asset_Price -> !Barrid|Price|Capt|PriceSource|Currency|DlyReturn%|DataDate
    file_name="USSLOW_Daily_Asset_Price",
    # USSLOW_Daily_Asset_Price -> !Barrid|Price|Capt|PriceSource|Currency|DlyReturn%|DataDate
)

barra_specific_returns = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOW_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOW_100_Asset_DlySpecRet",
    # USSLOW_100_Asset_DlySpecRet -> !Barrid|SpecificReturn|DataDate
)

barra_risk = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOWL_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOWL_100_Asset_Data",
    # USSLOWL_100_Asset_Data -> !Barrid|Yield%|TotalRisk%|SpecRisk%|HistBeta|PredBeta|DataDate
)

barra_covariances = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOWL_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOWL_100_Covariance",
    # USSLOWL_100_Covariance -> !Factor1|Factor2|VarCovar|DataDate
)

barra_exposures = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOWL_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOWL_100_Asset_Exposure",
)
# USSLOWL_100_Asset_Exposure -> !Barrid|Factor|Exposure|DataDate


barra_factors = BarraDataset(
    history_folder=None,
    history_zip_file=None,
    daily_folder="bime",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOWL_100_DlyFacRet",
)
# USSLOWL_100DlyFacRet -> !Factor|DlyReturn|DataDate


barra_volume = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOW_100_D",
    daily_folder="bime",
    daily_zip_file="SMD_USSLOW_Market_Data",
    file_name="USSLOW_Market_Data",
)

barra_assets = BarraDataset(
    history_folder=None,
    history_zip_file=None,
    daily_folder="bime",
    daily_zip_file="SMD_USSLOW_XSEDOL_ID",
    file_name="USA_Asset_Identity",
    # USA_Asset_Identity -> !Barrid|Name|Instrument|IssuerID|ISOCountryCode|ISOCurrencyCode|RootID|StartDate|EndDate
)

barra_ids = BarraDataset(
    history_folder=None,
    history_zip_file=None,
    daily_folder="bime",
    daily_zip_file="SMD_USSLOW_XSEDOL_ID",
    file_name="USA_XSEDOL_Asset_ID",
    # USA_XSEDOL_Asset_ID -> !Barrid|AssetIDType|AssetID|StartDate|EndDate
)