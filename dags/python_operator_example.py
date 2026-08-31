import json
from datetime import datetime
from urllib.parse import urljoin

import pydantic
import requests

API_host = 'https://de-start-sprint-etl-airflow-api.de.education-services.ru'


class UserActivityModel(pydantic.BaseModel):
    id: int
    uniq_id: str
    date_time: datetime
    action_id: int
    customer_id: int
    quantity: int