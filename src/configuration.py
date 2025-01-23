import logging
from typing import List, Optional
from enum import Enum
from pydantic import BaseModel, Field, ValidationError, computed_field
from keboola.component.exceptions import UserException

class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"

class OverridePKeyItem(BaseModel):
    pkey: List[str]
    dataset: str = Field()

class Source(BaseModel):
    setup_id: str
    days_interval: int
    hours_interval: int
    date_to: Optional[str]
    datasets: List[str] = Field(default=["Click", "Impression", "Trackingpoint", "Event"])
    file_charset: str = Field(default="UTF-8")
    meta_files: Optional[List[str]] = Field(default=None)

class Destination(BaseModel):
    file_name: str = Field(default=None)
    table_name: str = Field(default=None)
    load_type: LoadType = Field(default=LoadType.incremental_load)
    override_pkey: Optional[List[OverridePKeyItem]]

    @computed_field
    def incremental(self) -> bool:
        return self.load_type in (LoadType.incremental_load)


class Configuration(BaseModel):
    source: Source
    destination: Destination
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
