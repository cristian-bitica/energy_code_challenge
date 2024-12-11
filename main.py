from src.bronze_wind_turbine_data_pipeline import (
    main as bronze_wind_turbine_data_pipeline,
)
from src.gold_wind_turbine_daily_power_output_metrics import (
    main as gold_wind_turbine_daily_power_output_metrics,
)
from src.silver_wind_turbine_data_pipeline import (
    main as silver_wind_turbine_data_pipeline,
)
from src.utils import get_spark_session

get_spark_session()

bronze_wind_turbine_data_pipeline()
silver_wind_turbine_data_pipeline()
gold_wind_turbine_daily_power_output_metrics()
